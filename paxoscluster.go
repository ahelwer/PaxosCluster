package main

import (
    "fmt"
    "github/paxoscluster/role"
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

    for roleId, address := range peers {
        role.Initialize(roleId, client, address)
    }

    for roleId := range peers {
        role.Run(roleId, peers)
    }

    //client <- "Hello, world!"

    var input string
    fmt.Scanln(&input)
}

