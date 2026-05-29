package runtime

import (
	"sync"
	"testing"

	"github.com/lonng/nano/cluster"
)

// M24: CurrentNode is published from startup/shutdown while PingNodes (and other
// callers) read it concurrently. Publication must be synchronized so readers do
// not race the writers.
func TestCurrentNode_ConcurrentAccess(t *testing.T) {
	defer SetCurrentNode(nil)

	const writers = 4
	const readers = 4
	const iterations = 2000
	var writersWG sync.WaitGroup
	var readersWG sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < writers; i++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			n := &cluster.Node{}
			for j := 0; j < iterations; j++ {
				SetCurrentNode(n)
				SetCurrentNode(nil)
			}
		}()
	}

	for i := 0; i < readers; i++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = CurrentNode()
				}
			}
		}()
	}

	writersWG.Wait()
	close(stop)
	readersWG.Wait()
}
