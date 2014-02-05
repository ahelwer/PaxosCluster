package main

import (
    "fmt"
    "net/rpc"
    "github/paxoscluster/role"
)

func main() {
    peers := map[uint64]string {
        1: "127.0.0.1:10000",
        2: "127.0.0.1:10001",
        3: "127.0.0.1:10002",
        4: "127.0.0.1:10003",
        5: "127.0.0.1:10004",
    }

    nodes := make(map[uint64]*role.Node)
    for roleId, address := range peers {
        node, err := role.Construct(roleId, address, peers)
        if err != nil {
            fmt.Println("Role", roleId, "init error:", err)
            continue
        }
        nodes[roleId] = node
    }

    for roleId, node := range nodes {
        err := node.Run(roleId)
        if err != nil {
            fmt.Println("Role", roleId, "run error:", err)
            continue
        }
    }

    cxn, err := rpc.Dial("tcp", peers[5])
    for {
        var input string
        fmt.Scanln(&input)
        var output string
        err = cxn.Call("ProposerRole.Replicate", &input, &output)
        if err != nil {
            fmt.Println(err)
        }
    }

}

