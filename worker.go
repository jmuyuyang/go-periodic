package periodic

import (
	"fmt"
	"github.com/Lupino/periodic/protocol"
	"net"
	"strings"
)

// Worker defined a client.
type Worker struct {
	bc *BaseClient
}

// NewWorker create a client.
func NewWorker() *Worker {
	return new(Worker)
}

// Connect a periodic server.
func (w *Worker) Connect(addr string) error {
	parts := strings.SplitN(addr, "://", 2)
	conn, err := net.Dial(parts[0], parts[1])
	if err != nil {
		return err
	}
	w.bc = NewBaseClient(conn, protocol.TYPECLIENT)
	go w.bc.ReceiveLoop()
	return nil
}

// Ping a periodic server.
func (w *Worker) Ping() bool {
	agent := w.bc.NewAgent()
	defer w.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.PING, nil)
	ret, _, _ := agent.Receive()
	if ret == protocol.PONG {
		return true
	}
	return false
}

// GrabJob from periodic server.
func (w *Worker) GrabJob() (j *Job, e error) {
	agent := w.bc.NewAgent()
	defer w.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.GRABJOB, nil)
	ret, data, _ := agent.Receive()
	if ret != protocol.JOBASSIGN {
		e = fmt.Errorf("GrabJob failed!")
		return
	}
	j, e = NewJob(w.bc, data)
	return
}

// AddFunc to periodic server.
func (w *Worker) AddFunc(Func string) error {
	agent := w.bc.NewAgent()
	defer w.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.CANDO, []byte(Func))
	return nil
}

// RemoveFunc to periodic server.
func (w *Worker) RemoveFunc(Func string) error {
	agent := w.bc.NewAgent()
	defer w.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.CANTDO, []byte(Func))
	return nil
}

// Close the client.
func (w *Worker) Close() {
	w.bc.Close()
}
