package main

import (
    "fmt"
    "net"
    "net/rpc"
    "github/paxoscluster/role"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
)

func main() {
    client := make(chan string)
    peers := map[uint64]string {
        1: "127.0.0.1:10000",
        2: "127.0.0.1:10001",
        3: "127.0.0.1:10002",
        4: "127.0.0.1:10003",
        5: "127.0.0.1:10004",
    }

    role := role.Role{0, client, peers}
    for roleId, address := range peers {
        role.RoleId = roleId
        acceptor := acceptor.AcceptorRole{role, 5, 0, "foobar"}
        handler := rpc.NewServer()
        err := handler.Register(&acceptor)
        if err != nil {
            fmt.Println("Failed to register Acceptor", roleId, err)
            continue
        }
        ln, err := net.Listen("tcp", address)
        if err != nil {
            fmt.Println("Listening error:", err)
            return
        }
        go acceptor.Run(handler, ln)
    }

    role.RoleId = 5
    proposer := proposer.ProposerRole{role, 0, ""}
    go proposer.Run()

    client <- "Hello, world!"

    var input string
    fmt.Scanln(&input)
}

