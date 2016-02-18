The periodic task system client for go.

Install
-------

    go get -v github.com/Lupino/go-preiodic

Usage
-----

worker
```go
import "github.com/Lupino/go-periodic"
    
var periodicServer = "unix:///tmp/periodic.sock"
var worker = periodic.Worker()
worker.Connect(periodicServer)

func handle(job periodic.Job) {
    job.Done()
    // job.Fail()
    // job.SchedLater(3)
}

worker.AddFunc("funcName", handle)

worker.Work()

```
client

```go
import "github.com/Lupino/go-periodic"
    
var periodicServer = "unix:///tmp/periodic.sock"
var client = periodic.Client()
client.Connect(periodicServer)
client.SubmitJob(...)
```

example see [here](https://github.com/Lupino/periodic/tree/master/cmd/periodic/subcmd)
