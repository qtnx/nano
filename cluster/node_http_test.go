package cluster_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/benchmark/testdata"
	"github.com/lonng/nano/cluster"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/scheduler"
	. "github.com/pingcap/check"
	"google.golang.org/protobuf/proto"
)

var _ = Suite(&nodeSuite{})

func TestNodeHTTP(t *testing.T) {
	TestingT(t)
}

func (s *nodeSuite) TestNodeHTTP(c *C) {
	go scheduler.Sched()
	defer scheduler.Close()

	// Start the master node
	masterComps := &component.Components{}
	masterComps.Register(&MasterComponent{})
	masterNode := &cluster.Node{
		Options: cluster.Options{
			IsMaster:   true,
			Components: masterComps,
			// OpenPrometheus: true, // Uncomment if you want to test Prometheus metrics
		},
		ServiceAddr: "127.0.0.1:4450",
	}
	err := masterNode.Startup()
	c.Assert(err, IsNil)
	defer masterNode.Shutdown()

	// Start the gate node
	member1Comps := &component.Components{}
	member1Comps.Register(&GateComponent{})
	memberNode1 := &cluster.Node{
		Options: cluster.Options{
			AdvertiseAddr: "127.0.0.1:4450",
			ClientAddr:    "127.0.0.1:14452",
			Components:    member1Comps,
			IsWebsocket:   false, // We are testing HTTP
		},
		ServiceAddr: "127.0.0.1:14451",
	}
	err = memberNode1.Startup()
	c.Assert(err, IsNil)
	defer memberNode1.Shutdown()

	// Start the game node
	member2Comps := &component.Components{}
	member2Comps.Register(&GameComponent{})
	memberNode2 := &cluster.Node{
		Options: cluster.Options{
			AdvertiseAddr: "127.0.0.1:4450",
			Components:    member2Comps,
		},
		ServiceAddr: "127.0.0.1:24451",
	}
	err = memberNode2.Startup()
	c.Assert(err, IsNil)
	defer memberNode2.Shutdown()

	// Wait a moment for the cluster to initialize
	time.Sleep(500 * time.Millisecond)

	// Now, we'll perform HTTP requests to the gate node
	client := &http.Client{}
	baseURL := "http://127.0.0.1:14452"

	// First, establish SSE connection
	var wg sync.WaitGroup
	wg.Add(1)
	messageChan := make(chan string, 10)
	var sseSessionID string
	go func() {
		defer wg.Done()
		var err error
		sseSessionID, err = connectSSE(baseURL+"/sse", messageChan)
		if err != nil {
			c.Errorf("Failed to connect to SSE: %v", err)
			return
		}
	}()
	// Wait for SSE to be ready
	time.Sleep(100 * time.Millisecond)

	pingData := &testdata.Ping{Content: "ping"}
	pingDataBytes, err := proto.Marshal(pingData)
	c.Assert(err, IsNil)

	// --- Test Notify ---
	notifyBody := map[string]interface{}{
		"route": "GateComponent.Test",
		"data":  string(pingDataBytes),
		"type":  1, // Notify
	}
	bodyBytes, err := json.Marshal(notifyBody)
	c.Assert(err, IsNil)

	req, err := http.NewRequest("POST", baseURL+"/api", bytes.NewReader(bodyBytes))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SSE-SessionID", sseSessionID) // Add this line
	resp, err := client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	fmt.Println("OK")

	// Wait for the SSE push message
	select {
	case msg := <-messageChan:
		c.Assert(strings.Contains(msg, "gate server pong"), IsTrue)
	case <-time.After(2 * time.Second):
		c.Error("Timeout waiting for SSE message")
	}

	// --- Test Request/Response ---
	requestBody := map[string]interface{}{
		"route": "GateComponent.Test2",
		"data":  pingDataBytes,
		"type":  0, // Request
	}
	bodyBytes, err = json.Marshal(requestBody)
	c.Assert(err, IsNil)

	req, err = http.NewRequest("POST", baseURL+"/api", bytes.NewReader(bodyBytes))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SSE-SessionID", sseSessionID) // Add this line
	resp, err = client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	responseData, err := io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	// The response should be JSON containing the result
	var response map[string]interface{}
	err = json.Unmarshal(responseData, &response)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(responseData), "gate server pong2"), IsTrue)

	// --- Test Remote Request ---
	// Test requesting GameComponent.Test2 through the gate node
	requestBody = map[string]interface{}{
		"route": "GameComponent.Test2",
		"data": map[string]interface{}{
			"Content": "ping",
		},
		"type": 0, // Request
	}
	bodyBytes, err = json.Marshal(requestBody)
	c.Assert(err, IsNil)

	req, err = http.NewRequest("POST", baseURL+"/api", bytes.NewReader(bodyBytes))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	responseData, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	// The response should be JSON containing the result
	err = json.Unmarshal(responseData, &response)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(responseData), "game server pong2"), IsTrue)

	// --- Test Remote Notify ---
	// Test notifying GameComponent.Test through the gate node
	notifyBody = map[string]interface{}{
		"route": "GameComponent.Test",
		"data": map[string]interface{}{
			"Content": "ping",
		},
		"type": 1, // Notify
	}
	bodyBytes, err = json.Marshal(notifyBody)
	c.Assert(err, IsNil)

	req, err = http.NewRequest("POST", baseURL+"/api", bytes.NewReader(bodyBytes))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	// Wait for the SSE push message
	select {
	case msg := <-messageChan:
		c.Assert(strings.Contains(msg, "game server pong"), IsTrue)
	case <-time.After(2 * time.Second):
		c.Error("Timeout waiting for SSE message")
	}

	// --- Test MasterComponent Notify ---
	// Test notifying MasterComponent.Test through the gate node
	notifyBody = map[string]interface{}{
		"route": "MasterComponent.Test",
		"data": map[string]interface{}{
			"Content": "ping",
		},
		"type": 1, // Notify
	}
	bodyBytes, err = json.Marshal(notifyBody)
	c.Assert(err, IsNil)

	req, err = http.NewRequest("POST", baseURL+"/api", bytes.NewReader(bodyBytes))
	c.Assert(err, IsNil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	// Wait for the SSE push message
	select {
	case msg := <-messageChan:
		c.Assert(strings.Contains(msg, "master server pong"), IsTrue)
	case <-time.After(2 * time.Second):
		c.Error("Timeout waiting for SSE message")
	}

	// Close the SSE connection
	close(messageChan)
	wg.Wait() // Wait for the SSE goroutine to finish
}

// connectSSE connects to the SSE endpoint and reads messages.
// It returns the session ID obtained from the response headers.
func connectSSE(url string, messageChan chan string) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	// Disable keep-alives
	req.Close = true

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	// Retrieve the session ID from the Set-Cookie header
	var sessionID string
	cookies := resp.Header["Set-Cookie"]
	for _, cookie := range cookies {
		if strings.HasPrefix(cookie, "sse_sessionID=") {
			parts := strings.Split(cookie, ";")
			sessionID = strings.TrimPrefix(parts[0], "sse_sessionID=")
			break
		}
	}

	go func() {
		defer resp.Body.Close()
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				fmt.Printf("Error reading SSE: %v\n", err)
				return
			}
			// SSE messages start with "data: "
			if strings.HasPrefix(line, "data: ") {
				// Trim prefix and newline
				data := strings.TrimPrefix(line, "data: ")
				data = strings.TrimSpace(data)
				// Send to channel if it's still open
				if messageChan != nil {
					messageChan <- data
				} else {
					return
				}
			}
		}
	}()
	return sessionID, nil
}
