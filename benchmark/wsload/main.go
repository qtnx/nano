// wsload drives Nano's WebSocket packet protocol. Apply network impairment outside
// this process (for example tc netem) and retain the emitted JSON parameters to
// reproduce the identical target workload.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	URL             string
	Connections     int
	Ramp            time.Duration
	Duration        time.Duration
	RequestRoute    string
	RequestJSON     json.RawMessage
	RequestEvery    time.Duration
	ConnectTimeout  time.Duration
	Output          string
	FailOnErrorRate *float64
}

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "wsload:", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("wsload", flag.ContinueOnError)
	flags.SetOutput(stderr)
	urlValue := flags.String("url", "", "required Nano WebSocket URL (ws:// or wss://)")
	connections := flags.Int("connections", 1, "number of simultaneous clients")
	ramp := flags.Duration("ramp", 0, "total connection admission window")
	duration := flags.Duration("duration", 30*time.Second, "total load duration")
	requestRoute := flags.String("request-route", "", "optional Nano request route")
	requestJSON := flags.String("request-json", "", "raw JSON request payload for --request-route")
	requestEvery := flags.Duration("request-every", time.Second, "request interval when --request-route is set")
	connectTimeout := flags.Duration("connect-timeout", 10*time.Second, "per-client WebSocket and Nano handshake timeout")
	output := flags.String("output", "json", "output format (json only)")
	failOnErrorRate := flags.Float64("fail-on-error-rate", -1, "optional failure-rate threshold in [0,1]")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	cfg := Config{
		URL:            *urlValue,
		Connections:    *connections,
		Ramp:           *ramp,
		Duration:       *duration,
		RequestRoute:   *requestRoute,
		RequestEvery:   *requestEvery,
		ConnectTimeout: *connectTimeout,
		Output:         *output,
	}
	if *requestJSON != "" {
		cfg.RequestJSON = json.RawMessage(*requestJSON)
	}
	if *failOnErrorRate >= 0 {
		threshold := *failOnErrorRate
		cfg.FailOnErrorRate = &threshold
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(stderr, "wsload: target=%s connections=%d ramp=%s duration=%s\n", cfg.URL, cfg.Connections, cfg.Ramp, cfg.Duration)
	result := Run(ctx, cfg)
	encoder := json.NewEncoder(stdout)
	if err := encoder.Encode(result); err != nil {
		return err
	}
	if cfg.FailOnErrorRate != nil && result.Attempted > 0 && float64(result.Failed)/float64(result.Attempted) > *cfg.FailOnErrorRate {
		return fmt.Errorf("failure rate %.6f exceeded threshold %.6f", float64(result.Failed)/float64(result.Attempted), *cfg.FailOnErrorRate)
	}
	return nil
}

func validateConfig(cfg Config) error {
	if cfg.URL == "" {
		return errors.New("--url is required")
	}
	parsed, err := url.Parse(cfg.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
		return errors.New("--url must be an absolute ws:// or wss:// URL")
	}
	if cfg.Output != "json" {
		return errors.New("--output must be json")
	}
	if cfg.Connections <= 0 {
		return errors.New("--connections must be greater than zero")
	}
	if cfg.Ramp < 0 {
		return errors.New("--ramp must not be negative")
	}
	if cfg.Duration <= 0 {
		return errors.New("--duration must be greater than zero")
	}
	if cfg.Ramp > cfg.Duration {
		return errors.New("--ramp must not exceed --duration")
	}
	if cfg.ConnectTimeout <= 0 {
		return errors.New("--connect-timeout must be greater than zero")
	}
	if cfg.RequestRoute == "" && len(cfg.RequestJSON) > 0 {
		return errors.New("--request-json requires --request-route")
	}
	if cfg.RequestRoute != "" {
		if len(cfg.RequestJSON) == 0 || !json.Valid(cfg.RequestJSON) {
			return errors.New("--request-route requires valid --request-json")
		}
		if cfg.RequestEvery <= 0 {
			return errors.New("--request-every must be greater than zero when --request-route is set")
		}
	}
	if cfg.FailOnErrorRate != nil && (*cfg.FailOnErrorRate < 0 || *cfg.FailOnErrorRate > 1) {
		return errors.New("--fail-on-error-rate must be between 0 and 1")
	}
	return nil
}

func Run(ctx context.Context, cfg Config) Result {
	started := time.Now()
	stats := newStats(cfg)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var clients sync.WaitGroup
	admit := func() {
		clients.Add(1)
		go func() {
			defer clients.Done()
			runClient(runCtx, cfg, stats)
		}()
	}

	for index := 0; index < cfg.Connections; index++ {
		if index > 0 && cfg.Ramp > 0 {
			when := started.Add(time.Duration(index) * cfg.Ramp / time.Duration(cfg.Connections-1))
			wait := time.NewTimer(time.Until(when))
			select {
			case <-runCtx.Done():
				wait.Stop()
				clients.Wait()
				return stats.result(started, time.Now())
			case <-wait.C:
			}
		}
		admit()
	}

	endTimer := time.NewTimer(time.Until(started.Add(cfg.Duration)))
	select {
	case <-runCtx.Done():
	case <-endTimer.C:
	}
	if !endTimer.Stop() {
		select {
		case <-endTimer.C:
		default:
		}
	}
	cancel()
	clients.Wait()
	return stats.result(started, time.Now())
}

func classifyDialError(err error) string {
	if err == nil {
		return errorDial
	}
	return errorDial
}
