package replica

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/common"
)

const (
	DefaultPort = 60221
)

type Replica struct {
	Self common.Node
	Port int
}

func (s *Replica) FDAck(req *common.FDPing, resp *common.FDAck) error {
	return nil
}

func (s *Replica) Failure(req *common.Failure, resp *common.FailureDetectedAck) error {
	log.Printf("Failure detected at [%s]", req.Address)
	return nil
}

func (s *Replica) ReceiveReplication(req *common.Replication, resp *common.ReplicationReceivedAck) error {
	log.Printf("Received file [%s] from [%s]", req.Name, req.Source)
	return nil
}

func (s* Replica) SendReplication(req *common.Replication, resp *common.ReplicationSentAck) error {
	log.Printf("Sending file [%s] to [%s]", req.Name, req.Destination)
	return nil
}

func (s *Replica) ReceiveFileUpdate(req *common.FileUpdate, resp *common.FileUpdateAck) error {
	switch req.OpType {
	case common.DeleteFileOp:
		log.Printf("deleted all versions of file [%s]", req.Name)
	case common.NewFileOp:
		log.Printf("received new file [%s], version [%d]", req.Name, req.Version)
	case common.UpdateFileOp:
		log.Printf("update file [%s] to version [%d]", req.Name, req.Version)
	case common.ReadFileOp:
		log.Printf("acking read for file [%s], version [%d]", req.Name, req.Version)
	}
	return nil
}

func (s* Replica) Run() {
	log.Printf("starting replica server on [%s]", s.Self.Address)
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	defer http.Serve(l, nil)
}