package coordinator

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/serialx/hashring"
	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/common"
)

const (
	CoordinatorBufferSize = 10
	DefaultPort = 60222
	ReplicaPort = 60221
	FileTransmissionPort = 60223
	RequestTimeout = 1 * time.Second
)

type Coordinator struct {
	Self common.Node
	NumReplicas int
	Nodes map[string]common.Node
	Files map[string]common.FileGroup
	Ring *hashring.HashRing
	pingPeriod time.Duration
	RequestTimeout time.Duration
}


func NewCoordinator(self common.Node, numReplicas int, nodes map[string]common.Node, pingPeriod time.Duration, requestTimeout time.Duration) *Coordinator {
	nodeAddresses := []string{}
	for addr := range nodes {
		nodeAddresses = append(nodeAddresses, addr)
	}
 
	return &Coordinator{
		Self: self,
		NumReplicas: numReplicas,
		Nodes: nodes,
		pingPeriod: pingPeriod,
		RequestTimeout: requestTimeout,
		Ring: hashring.New(nodeAddresses),
		Files: map[string]common.FileGroup{},
	}
}

func (c *Coordinator) ping() []common.Node {
	wg := sync.WaitGroup{}
	failures := make(chan common.Node, CoordinatorBufferSize)
	for _, node := range c.Nodes {
		if node.Address == c.Self.Address {
			continue;
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, node common.Node) {
			client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", node.Address, node.Port))
			if err != nil {
				failures <- node
				log.Println("dialing:", err)
			} else {
				ack := new(common.FDAck)
				ping := new(common.FDPing)
				call := client.Go("Replica.FDAck", &ping, &ack, nil)
				select {
				case <- call.Done:
					break
				case <- time.After(c.RequestTimeout):
					log.Printf("ping not acked within timeout at %s", node.Address)
					failures <- node
				}
			}
			wg.Done()
		}(&wg, node)
	}
	wg.Wait()
	close(failures)
	output := []common.Node{}
	for f := range failures {
		output = append(output, f)
	}
	return output
}

// retruns a set of replicas for a file in a ring
func (c* Coordinator) getReplicasForFile(file string, ring *hashring.HashRing) (string, map[string]struct{}) {
	replicas, ok := ring.GetNodes(file, c.NumReplicas)
	if !ok {
		log.Panicf("could not get replicas for [%s], ring [%d]", file, ring.Size())
	}
	output := map[string]struct{}{}
	for _, r := range replicas {
		output[r] = struct{}{}
	}
	return replicas[0], output
}

func (c *Coordinator) sendReplication(rep common.Replication) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", rep.Source, ReplicaPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.ReplicationSentAck)
		call := client.Go("Replica.SendReplication", &rep, &ack, nil)
		select {
		case <- call.Done:
			return
		case <- time.After(RequestTimeout):
			log.Panicln("could not send replication")
		}
	}
}

func (c *Coordinator) receiveReplication(rep common.Replication) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", rep.Destination, ReplicaPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.ReplicationSentAck)
		call := client.Go("Replica.ReceiveReplication", &rep, &ack, nil)
		select {
		case <- call.Done:
			return
		case <- time.After(RequestTimeout):
			log.Panicln("could not receive replication")
		}
	}
}

func (c *Coordinator) diff(newRing *hashring.HashRing) map[string]common.FileGroup {
	log.Println("calculating diff between pre-replicated and post-replicated state")
	output := map[string]common.FileGroup{}
	// compare newRing with the current file distribution
	// return the new file distribution
	for f, fg := range c.Files {
		// get replicas on new hashring
		_, newReplicas := c.getReplicasForFile(f, newRing)

		// get replicas on old hashring
		src, oldReplicas := c.getReplicasForFile(f, c.Ring)
		
		for r := range newReplicas {
			_, inOld := oldReplicas[r]
			if !inOld {
				log.Printf("[%s] replication: [%s] -> [%s]", f, src, r)
				fg.Replicas = newReplicas
				c.sendReplication(common.Replication{
					Destination: r,
					FileGroup: fg,
					Source: src,
				})
				c.receiveReplication(common.Replication{
					Destination: r,
					FileGroup: fg,
					Source: src,
				})
			}
			output[f] = fg
		}
	}

	return output
}

func (c *Coordinator) handleFailure(failed common.Node) {
	log.Printf("detected failure at [%s]", failed.Address)
	// remove node from node map
	delete(c.Nodes, failed.Address)
	// remove node from hashring
	removed := c.Ring.RemoveNode(failed.Address)
	
	// calculate and log the differences between the old ring and new ring
	// send ReplicationReceived and ReplicationSent requests
	c.Files = c.diff(removed)

	c.Ring = removed
}

func (c *Coordinator) sendFileUpdate(addr string, update common.FileUpdate) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", addr, ReplicaPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.PutAck)
		call := client.Go("Replica.ReceiveFileUpdate", &update, &ack, nil)
		select {
		case <- call.Done:
			return
		case <- time.After(RequestTimeout):
			log.Panicln("could not send replication")
		}
	}
}

func (c *Coordinator) Ls(req *common.LsRequest, resp *common.LsResponse) error {
	fg, exists := c.Files[req.Filename]
	*resp = common.LsResponse{
		Addresses: []string{},
	}
	if !exists {
		return nil
	}
	for machine := range fg.Replicas {
		resp.Addresses = append(resp.Addresses, machine)
	}
	return nil
}

func (c *Coordinator) Store(req *common.StoreRequest, resp *common.StoreResponse) error {
	*resp = common.StoreResponse{
		Files: []string{},
	}
	for f, fg := range c.Files {
		_, ok := fg.Replicas[req.Address]
		if ok {
			resp.Files = append(resp.Files, f)
		}
	}
	return nil
}

func (c *Coordinator) MemList(req *common.MemListRequest, resp *common.MemListResponse) error {
	*resp = c.Nodes
	return nil
}

func (c *Coordinator) Join(req *common.Node, resp *common.JoinAck) error {
	c.Nodes[req.Address] = *req
	c.Ring = c.Ring.AddNode(req.Address)
	log.Printf("joined node [%s] to sdfs", req.Address)
	return nil
}

func (c *Coordinator) GetVersions(req *common.GetVersionsRequest, resp *common.GetVersionsResponse) error {
	log.Printf("getting last [%d] versions of [%s]", req.NumVersions, req.Filename)
	fg, ok := c.Files[req.Filename]
	if !ok {
		log.Printf("file [%s] does not exist in SDFS", req.Filename)
		return nil
	}
	*resp = common.GetVersionsResponse{
		Versions: []string{},
	}
	for version := 1 ; version <= fg.Version ; version += 1 {
		name := fmt.Sprintf("%d,%s", version, fg.Name)
		log.Printf("aggregating [%s]", name)
		resp.Versions = append(resp.Versions, name)
	}
	return nil
}

func (c *Coordinator) Delete(req *common.DeleteRequest, resp *common.DeleteResponse) error {
	log.Printf("deleting [%s]", req.Filename)
	_, ok := c.Files[req.Filename]
	if !ok {
		log.Printf("[%s] does not exist in SDFS", req.Filename)
		*resp = false;
		return nil
	}
	delete(c.Files, req.Filename)
	*resp = true 
	return nil
}

func (c *Coordinator) Leave(req *common.Node, resp *common.LeaveAck) error {
	// remove node from node map
	delete(c.Nodes, req.Address)
	// remove node from hashring
	removed := c.Ring.RemoveNode(req.Address)
	
	// calculate and log the differences between the old ring and new ring
	// send ReplicationReceived and ReplicationSent requests
	c.Files = c.diff(removed)

	c.Ring = removed
	log.Printf("removed node [%s] from sdfs", req.Address)
	return nil
}

/* func (c *Coordinator) receiveFile(peer string, name string) {
	connection, err := net.Dial("tcp", fmt.Sprintf("%s:%d", peer, FileTransmissionPort))
	if err != nil {
		log.Panic("unable to get file from peer")
	}
	defer connection.Close()
	bufferFileSize := make([]byte, 10)
	connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)
	
	newFile, err := os.Create("/tmp/sdfs/" + name)
	
	if err != nil {
		log.Panic("unable to create file")
	}
	defer newFile.Close()
	var receivedBytes int64
	
	for {
		log.Println("receiving")
		if (fileSize - receivedBytes) < 1024 {
			io.CopyN(newFile, connection, (fileSize - receivedBytes))
			connection.Read(make([]byte, (receivedBytes+1024)-fileSize))
			break
		}
4)
		receivedBytes += 1024
	}
	log.Printf("successfully received [%s] for [%s]", name, peer)
} */

func (c *Coordinator) Put(req *common.PutRequest, resp *common.PutAck) error {
	log.Printf("received put request for file [%s]", req.Name)
	// go c.receiveFile(req.Source, req.Name)
	opType := common.UpdateFileOp

	// increment sequence number for the file
	_, ok := c.Files[req.Name]
	if !ok {
		log.Printf("files [%s] not found in sdfs", req.Name)
		log.Printf("ring has [%d] nodes", c.Ring.Size())
		nodes, ok := c.Ring.GetNodes(req.Name, c.NumReplicas)
		if !ok {
			log.Panicln("could not get neighbors")
		}
		addrSet := common.AddressSet{}
		for _, n := range nodes {
			addrSet[n] = struct{}{}
		}
		c.Files[req.Name] = common.FileGroup{
			Name: req.Name,
			Version: 0,
			Replicas: addrSet,
		}
		opType = common.NewFileOp
	}

	fileGroup := c.Files[req.Name]
	fileGroup.Version += 1
	c.Files[req.Name] = fileGroup

	for replica := range c.Files[req.Name].Replicas {
		if replica != c.Self.Address {
			c.sendFileUpdate(replica, common.FileUpdate{
				Name: req.Name,
				Version: c.Files[req.Name].Version,
				OpType: opType,
			})
		}
	}

	return nil
}


func (c *Coordinator) Run() {
	
	wg := sync.WaitGroup{}
	
	go func() {
		log.Printf("starting failure detector server on [%s]", c.Self.Address)
		ticker := time.NewTicker(c.pingPeriod)
		quit := make(chan struct{})
		for {
		select {
			case <- ticker.C:
				failed := c.ping()
				if len(failed) > 0 {
					c.handleFailure(failed[0])
				}
			case <- quit:
				ticker.Stop()
				return
			}
		}
	}()
	
	wg.Add(1)
	go func() {
		log.Printf("starting coordinator server on [%s]", c.Self.Address)
		rpc.Register(c)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", DefaultPort))
		if e != nil {
			log.Fatal("listen error:", e)
		}
		defer http.Serve(l, nil)
	}()
}