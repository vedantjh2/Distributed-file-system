package common

const (
	DeleteFileOp = 1
	UpdateFileOp = 2
	NewFileOp = 3
	ReadFileOp = 4
)

type Node struct {
	Address          string
	Port             int
	IterationNumber  int
}

type Failure struct {
	Address string
}

type Replication struct {
	Source string
	Destination string
	FileGroup
}

type AddressSet map[string]struct{}

type FileGroup struct {
	Name string
	Version int
	Replicas AddressSet
}

type PutRequest struct {
	Source string
	Name string
}

type FileUpdate struct {
	Name string
	Version int
	OpType int
}

type LsRequest struct {
	Filename string
}

type LsResponse struct {
	Addresses []string
}

type StoreRequest struct {
	Address string
}

type StoreResponse struct {
	Files []string
}

type GetVersionsRequest struct {
	NumVersions int
	Filename string
}

type GetVersionsResponse struct {
	Versions []string
}

type DeleteRequest struct {
	Filename string
}

type DeleteResponse bool

type MemListRequest struct {}

type MemListResponse map[string]Node

type P1Ack struct {}

type P2Ack struct {}

type RollbackAck struct{}

type FDPing struct{}

type FDAck struct{}

type PutAck struct{}

type JoinAck struct{}

type LeaveAck struct{}

type FileUpdateAck struct{}

type FailureDetectedAck struct{}

type ReplicationReceivedAck struct{}

type ReplicationSentAck struct{}
