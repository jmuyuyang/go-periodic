package periodic

import (
	"fmt"
	"github.com/Lupino/periodic/driver"
	"github.com/Lupino/periodic/protocol"
	"net"
	"sort"
	"strings"
)

// Client defined a client.
type Client struct {
	bc *BaseClient
}

// NewClient create a client.
func NewClient() *Client {
	return new(Client)
}

// Connect a periodic server.
func (c *Client) Connect(addr string) error {
	parts := strings.SplitN(addr, "://", 2)
	conn, err := net.Dial(parts[0], parts[1])
	if err != nil {
		return err
	}
	c.bc = NewBaseClient(conn, protocol.TYPECLIENT)
	go c.bc.ReceiveLoop()
	return nil
}

// Ping a periodic server.
func (c *Client) Ping() bool {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.PING, nil)
	ret, _, _ := agent.Receive()
	if ret == protocol.PONG {
		return true
	}
	return false
}

// SubmitJob to periodic server.
func (c *Client) SubmitJob(job driver.Job) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.SUBMITJOB, job.Bytes())
	ret, data, _ := agent.Receive()
	if ret == protocol.SUCCESS {
		return nil
	}
	return fmt.Errorf("SubmitJob error: %s", data)
}

// Status return a status from periodic server.
func (c *Client) Status() ([][]string, error) {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.STATUS, nil)

	_, data, _ := agent.Receive()
	stats := strings.Split(string(data), "\n")
	sort.Strings(stats)

	lines := make([][]string, 0, 4)
	for _, stat := range stats {
		if stat == "" {
			continue
		}
		line := strings.Split(stat, ",")
		lines = append(lines, line)
	}
	return lines, nil
}

// DropFunc drop unuself function from periodic server.
func (c *Client) DropFunc(Func string) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.DROPFUNC, []byte(Func))
	ret, data, _ := agent.Receive()
	if ret == protocol.SUCCESS {
		return nil
	}
	return fmt.Errorf("Drop func %s error: %s", Func, data)
}

// RemoveJob to periodic server.
func (c *Client) RemoveJob(job driver.Job) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.REMOVEJOB, job.Bytes())
	ret, data, _ := agent.Receive()
	if ret == protocol.SUCCESS {
		return nil
	}
	return fmt.Errorf("RemoveJob error: %s", data)
}

// Close the client.
func (c *Client) Close() {
	c.bc.Close()
}
