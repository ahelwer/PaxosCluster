package main

import (
    "fmt"
    "net"
    "net/rpc"
)

type Server struct {
    id int
}

func (this *Server) Receive(i int, reply *int) error {
    fmt.Println("Server", this.id, "received", i)
    *reply = this.id
    return nil
}

func (this *Server) run(address string) {
    rpc.Register(this)
    ln, _ := net.Listen("tcp", address)
    for {
        cxn, _ := ln.Accept()
        go rpc.ServeConn(cxn)
    }
}

func main() {
    addresses := map[int]string {
        1: "127.0.0.1:10000",
        2: "127.0.0.1:10001",
        3: "127.0.0.1:10002",
    }

    for id, address := range addresses {
        server := Server{id}
        go server.run(address)
    }
    
    servers := make(map[int]*rpc.Client)
    for id, address := range addresses {
        server, _ := rpc.Dial("tcp", address)
        servers[id] = server
    }

    for id, server := range servers {
        var reply int
        server.Call("Server.Receive", id, &reply)
        fmt.Println("Returned", reply)
    }
}
