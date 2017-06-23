package periodic

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/jmuyuyang/periodic/driver"
	"github.com/jmuyuyang/periodic/protocol"
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
//  opts = map[string]string{
//    "schedat": schedat,
//    "args": args,
//    "timeout": timeout,
//  }
func (c *Client) SubmitJob(funcName, name string, opts map[string]string) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	job := driver.Job{
		Func: funcName,
		Name: name,
	}
	if args, ok := opts["args"]; ok {
		job.Args = args
	}
	if schedat, ok := opts["schedat"]; ok {
		i64, _ := strconv.ParseInt(schedat, 10, 64)
		job.SchedAt = i64
	}
	if timeout, ok := opts["timeout"]; ok {
		i64, _ := strconv.ParseInt(timeout, 10, 64)
		job.Timeout = i64
	}
	if period, ok := opts["period"]; ok {
		job.Period = period
	}
	if failRetry, ok := opts["fail_retry"]; ok {
		i32, _ := strconv.Atoi(failRetry)
		job.FailRetry = i32
	}
	if retention, ok := opts["retention"]; ok {
		i64, _ := strconv.ParseInt(retention, 10, 64)
		job.Retention = i64
	}
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
func (c *Client) RemoveJob(funcName, name string) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	job := driver.Job{
		Func: funcName,
		Name: name,
	}
	agent.Send(protocol.REMOVEJOB, job.Bytes())
	ret, data, _ := agent.Receive()
	if ret == protocol.SUCCESS {
		return nil
	}
	return fmt.Errorf("RemoveJob error: %s", data)
}

// Dump data from periodic server.
func (c *Client) Dump(w io.Writer) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.DUMP, nil)
	for {
		_, payload, _ := agent.Receive()
		if bytes.Equal(payload, []byte("EOF")) {
			break
		}
		header, _ := protocol.MakeHeader(payload)
		w.Write(header)
		w.Write(payload)
	}
	return nil
}

// Load data for periodic server.
func (c *Client) Load(r io.Reader) error {
	agent := c.bc.NewAgent()
	defer c.bc.RemoveAgent(agent.ID)
	var err error
	for {
		payload, err := readPatch(r)
		if err != nil {
			break
		}
		agent.Send(protocol.LOAD, payload)
	}
	if err == io.EOF {
		err = nil
	}
	return err
}

func readPatch(fp io.Reader) (payload []byte, err error) {
	var header = make([]byte, 4)
	nRead := uint32(0)
	for nRead < 4 {
		n, err := fp.Read(header[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}

	length := protocol.ParseHeader(header)
	payload = make([]byte, length)
	nRead = uint32(0)
	for nRead < length {
		n, err := fp.Read(payload[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}
	return
}

// Close the client.
func (c *Client) Close() {
	c.bc.Close()
}
