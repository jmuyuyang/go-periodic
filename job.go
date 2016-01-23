package periodic

import (
	"bytes"
	"github.com/Lupino/periodic/driver"
	"github.com/Lupino/periodic/protocol"
	"strconv"
)

// Job defined a job type.
type Job struct {
	bc     *BaseClient
	Raw    driver.Job
	Handle []byte
}

// NewJob create a job
func NewJob(bc *BaseClient, data []byte) (job Job, err error) {
	var raw driver.Job
	parts := bytes.SplitN(data, protocol.NullChar, 2)
	raw, err = driver.NewJob(parts[1])
	if err != nil {
		return
	}
	job = Job{
		bc:     bc,
		Raw:    raw,
		Handle: parts[0],
	}
	return
}

// Done tell periodic server the job done.
func (j *Job) Done() error {
	agent := j.bc.NewAgent()
	defer j.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.WORKDONE, j.Handle)
	return nil
}

// Fail tell periodic server the job fail.
func (j *Job) Fail() error {
	agent := j.bc.NewAgent()
	defer j.bc.RemoveAgent(agent.ID)
	agent.Send(protocol.WORKFAIL, j.Handle)
	return nil
}

// SchedLater tell periodic server to sched job later on delay.
func (j *Job) SchedLater(delay int) error {
	agent := j.bc.NewAgent()
	defer j.bc.RemoveAgent(agent.ID)
	buf := bytes.NewBuffer(nil)
	buf.Write(j.Handle)
	buf.Write(protocol.NullChar)
	buf.WriteString(strconv.Itoa(delay))
	agent.Send(protocol.SCHEDLATER, buf.Bytes())
	return nil
}
