package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/client"
	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/common"
	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/coordinator"
	"gitlab.engr.illinois.edu/akroy2/mp3/sdfs/replica"

)

const (
	DefaultPort                     = 60221
	DefaultFailureChannelBufferSize = 10
)

var (
	MachineIdx    string
	PingPeriod    time.Duration
	PingTimeout   time.Duration
	IsCoordinator bool
)

func init() {
	flag.BoolVar(&IsCoordinator, "coordinator", false, "true if running a coordinator instance, false otherwise")
	flag.StringVar(&MachineIdx, "machine_idx", "01", "the server machine index")
	flag.DurationVar(&PingPeriod, "ping_period", 3 * time.Second, "the ping period")
	flag.DurationVar(&PingTimeout, "ping_timeout", 1500 * time.Millisecond, "the request timeout")
	flag.Parse()
}

func main() {
	nodes := map[string]common.Node{
		"fa22-cs425-3301.cs.illinois.edu": {
			Address: "fa22-cs425-3301.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3302.cs.illinois.edu": {
			Address: "fa22-cs425-3302.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3303.cs.illinois.edu": {
			Address: "fa22-cs425-3303.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3304.cs.illinois.edu": {
			Address: "fa22-cs425-3304.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3305.cs.illinois.edu": {
			Address: "fa22-cs425-3305.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3306.cs.illinois.edu": {
			Address: "fa22-cs425-3306.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3307.cs.illinois.edu": {
			Address: "fa22-cs425-3307.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3308.cs.illinois.edu": {
			Address: "fa22-cs425-3308.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3309.cs.illinois.edu": {
			Address: "fa22-cs425-3309.cs.illinois.edu",
			Port: DefaultPort,
		},
		"fa22-cs425-3310.cs.illinois.edu": {
			Address: "fa22-cs425-3310.cs.illinois.edu",
			Port: DefaultPort,
		},
	}
	self, ok := nodes["fa22-cs425-33" + MachineIdx + ".cs.illinois.edu"]
	if !ok {
		log.Panicf("machine with idx [%s] does not exist", MachineIdx)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if IsCoordinator {
			log.Printf("starting coordinator on [%s]", self.Address)
			c := coordinator.NewCoordinator(self, 4, map[string]common.Node{}, PingPeriod, PingTimeout)
			c.Run()
		} else {
			log.Printf("starting sdfs client on [%s]", self.Address)
			r := replica.Replica{
				Self: self,
				Port: replica.DefaultPort,
			}
			r.Run()
		}
		wg.Done()
	}()

	cli := client.Client{
		Self: self,
	}
	cli.Run()
}
