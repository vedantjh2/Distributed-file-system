# Simple Distributed File System (SDFS)
CS 425, Fall 2022

Authors: Andrea Roy (akroy2), Vedant Jhaveri (vedantj2)

## Introduction
SDFS is a distributed file system that stores files in a distributed manner over many servers with 4 total copies of a file to ensure it is always available to view by a client. We use a hashring to implement consistent hashing and figure out which servers must store a give file and handle the distribution of replicas in case of server failures.  



## Usage
to start a coordinator use the coordinator flag to indicate this server is going to act as a coordinator. Available arguments and command line options can be seen below (or by running the `--help` flag):
```
-coordinator
    	true if running a coordinator instance, false otherwise
  -machine_idx string
    	the server machine index (default "01")
  -ping_period duration
    	the ping period (default 3s)
  -ping_timeout duration
    	the request timeout (default 1.5s)
```
## Building SDFS
The SDFS can be built using the following command:
```
go build .
```

## Example to Run SDFS
To start a coordinator, use the following command
```
go run . -coordinator -machine_id="03" 
```
To start clients and servers, use the following command
```
go run . -machine_id="07" 
```

you can the type commands such as join, leave, put [local file] [sdfs file], get [sdfs file] [local file], etc. The coordinator takes care of all the processes. 
