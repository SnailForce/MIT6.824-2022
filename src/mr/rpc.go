package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// Add your RPC definitions here.

// 请求任务

type HeartbeatRequest struct {
}

type HeartbeatResponse struct {
	FilePath string
	JobType  JobType
	NReduce  int
	NMap     int
	Id       int
}

func (responce HeartbeatResponse) String() string {
	switch responce.JobType {
	case MapJob:
		return fmt.Sprintf("{JobType: %v, FilePath: %v, NReduce: %v}", responce.JobType, responce.FilePath, responce.NReduce)
	case ReduceJob:
		return fmt.Sprintf("{JobType: %v, Id: %v, NMap: %v, NReduce: %v}", responce.JobType, responce.Id, responce.NMap, responce.NReduce)
	case WaitJob, CompleteJob:
		return fmt.Sprintf("{JobType: %v}", responce.JobType)
	}
	panic(fmt.Sprintf("unexpected JobType %d", responce.JobType))
}

// 报告任务

type ReportRequest struct {
	Id    int
	Phase SchedulePhase
}

type ReportResponse struct {
}

func (request ReportRequest) String() string {
	return fmt.Sprintf("{Id: %v, SchedulePhase: %v}", request.Id, request.Phase)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
