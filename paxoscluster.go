package main

import (
    "fmt"
    "net/rpc"
    "github/paxoscluster/role"
)

func main() {
    roles := []uint64{1, 2, 3, 4, 5}
    nodes := make(map[uint64]*role.Node)
    addresses:= make(map[uint64]string)
    for _, roleId := range roles {
        node, address, err := role.ConstructNode(roleId)
        if err != nil { panic(err) }
        nodes[roleId] = node
        addresses[roleId] = address
    }

    for _, node := range nodes {
        err := node.Run()
        if err != nil { panic(err) }
    }

    cxn, err := rpc.Dial("tcp", addresses[5])
    for {
        var input string
        fmt.Scanln(&input)
        var output string
        err = cxn.Call("ProposerRole.Replicate", &input, &output)
        if err != nil { fmt.Println(err) }
    }

}

