package client

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/common"
	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/coordinator"
)

const (
	ServerPath = "/home/akroy2/files/"
	CoordinatorAddress = "fa22-cs425-3301.cs.illinois.edu"
	BufferSize = 100000000
)

type Client struct {
	Self common.Node
}

/*func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}*/

func (c *Client) Put(local string, target string, version int) error {
	log.Printf("putting local file [%s] on SDFS as [%s]", local, target)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.PutAck)
		pr := common.PutRequest{
			Name: target,
			Source: c.Self.Address,
		}
		call := client.Go("Coordinator.Put", &pr, &ack, nil)
		select {
		case <- call.Done:
			// send file to client
		case <- time.After(coordinator.RequestTimeout):
			return err
		}
	}
	return nil
}

func (c *Client) Get(target string, local string, version int) error {
	log.Printf("downloading sdfs file [%s] to local file [%s]", target, local)
	cmd := exec.Command("/usr/bin/cp", ServerPath + target, local)
	err := cmd.Run()
	if err != nil {
		log.Println("copy error", err)
	}
	return nil
}

func (c *Client) Join() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.JoinAck)
		call := client.Go("Coordinator.Join", &c.Self, &ack, nil)
		select {
		case <- call.Done:
			log.Printf("successfully joined client to sdfs")
		case <- time.After(coordinator.RequestTimeout):
			log.Printf("unable to join client to sdfs")
			return err
		}
	}
	return nil
}

func (c *Client) Leave() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		ack := new(common.JoinAck)
		call := client.Go("Coordinator.Leave", &c.Self, &ack, nil)
		select {
		case <- call.Done:
			log.Printf("successfully exited client to sdfs")
		case <- time.After(coordinator.RequestTimeout):
			log.Printf("unable to exit client tfromo sdfs")
			return err
		}
	}
	return nil
}

func (c *Client) ListSelf() error {
	log.Printf("Self: %s", c.Self.Address)
	return nil
}

func (c *Client) ListMem() error {
	output := "Membership List:\n---------------\n"
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	req := new(common.MemListRequest)
	resp := new(common.MemListResponse)
	call := client.Go("Coordinator.MemList", req, resp, nil)
	select {
	case <- call.Done:
		for address := range *resp {
			output += address + "\n"
		}
	case <- time.After(coordinator.RequestTimeout):
		log.Printf("unable to exit client tfromo sdfs")
		return err
	}
	log.Println(output)
	return nil
}

func (c *Client) Delete(target string) error {
	log.Printf("deleting [%s]", target)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	if err != nil {
		log.Println("dialing:", err)
	} else {
		req := common.DeleteRequest{
			Filename: target,
		}
		resp := new(common.DeleteResponse)
		call := client.Go("Coordinator.Delete", &req, &resp, nil)
		select {
		case <- call.Done:
			if *resp {
				log.Printf("successfully deleted file [%s] from SDFS", target)
			} else {
				log.Printf("file [%s] does not exist in SDFS", target)
			}
		case <- time.After(coordinator.RequestTimeout):
			return err
		}
	}
	return nil
}

func (c *Client) ListReplicas(target string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	req := new(common.LsRequest)
	req.Filename = target
	resp := new(common.LsResponse)
	call := client.Go("Coordinator.Ls", req, resp, nil)
	output := "Replicas for " + target + ":\n-----------------------\n"
	select {
	case <- call.Done:
		for _, address := range resp.Addresses {
			output += address + "\n"
		}
	case <- time.After(coordinator.RequestTimeout):
		log.Printf("unable to exit client tfromo sdfs")
		return err
	}
	log.Println(output)
	return nil
}

func (c *Client) ListFiles(address string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	req := new(common.StoreRequest)
	req.Address = c.Self.Address
	resp := new(common.StoreResponse)
	call := client.Go("Coordinator.Store", req, resp, nil)
	output := "Files on local server " + c.Self.Address + ":\n-----------------------\n"
	select {
	case <- call.Done:
		for _, file := range resp.Files {
			output += file + "\n"
		}
	case <- time.After(coordinator.RequestTimeout):
		log.Printf("unable to exit client tfromo sdfs")
		return err
	}
	log.Println(output)
	return nil
}

func (c *Client) GetVersions(target string, numVersions int, local string) error {
	fmt.Printf("querying last [%d] versions of [%s] to [%s]", numVersions, target, local)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", CoordinatorAddress, coordinator.DefaultPort))
	req := new(common.GetVersionsRequest)
	req.NumVersions = numVersions
	req.Filename = target
	resp := new(common.GetVersionsResponse)
	call := client.Go("Coordinator.GetVersions", req, resp, nil)
	output := fmt.Sprintf("Aggregating versions for [%s] ", target) + c.Self.Address + ":\n-----------------------\n"
	select {
	case <- call.Done:
		for _, version := range resp.Versions {
			output += version + "\n"
		}
	case <- time.After(coordinator.RequestTimeout):
		log.Printf("unable to exit client tfromo sdfs")
		return err
	}
	log.Println(output)
	return nil
}

func (c *Client) Run() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := scanner.Text()
		tokens := strings.Split(cmd, " ")
		if len(tokens) < 1 {
			continue
		}
		if len(tokens) == 1 {
			switch tokens[0] {
			case "store":
				c.ListFiles(c.Self.Address)
			case "join":
				c.Join()
			case "leave":
				c.Leave()
			case "list_mem":
				c.ListMem()
			case "list_self":
				c.ListSelf()
			default:
				log.Printf("invalid command: %s", cmd)
			}
		} else if len(tokens) == 2 {
			switch (tokens[0]) {
			case "delete":
				c.Delete(tokens[1])
			case "ls":
				c.ListReplicas(tokens[1])
			default:
				log.Printf("invalid command: %s", cmd)
			}
		} else if len(tokens) == 3 {
			switch (tokens[0]) {
			case "get":
				c.Get(tokens[1], tokens[2], -1)
			case "put":
				err := c.Put(tokens[1], tokens[2], -1)
				if err != nil {
					log.Println(err.Error())
				}
			default:
				log.Printf("invalid command: %s", cmd)
			}
		} else if len(tokens) == 4{
			switch (tokens[0]) {
			case "get-versions":
				val, err := strconv.Atoi(tokens[2])
				if err != nil {
					log.Printf("invalid command: %s", cmd)
					break
				}
				c.GetVersions(tokens[1], val, tokens[3])
			}
		} else {
			log.Printf("invalid command: %s", cmd)
		}
	}
}