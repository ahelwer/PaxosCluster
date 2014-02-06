package main

import (
    "fmt"
    "net/rpc"
    "github/paxoscluster/role"
)

func main() {
    roles := []uint64{1,2,3,4,5}
    var nodes []*role.Node = nil
    for _, roleId := range roles {
        node, err := role.ConstructNode(roleId)
        if err != nil {
            fmt.Println(err)
            return
        }
        nodes = append(nodes, node)
    }

    for _, node := range nodes {
        err := node.Run()
        if err != nil {
            fmt.Println(err)
            return
        }
    }

    cxn, err := rpc.Dial("tcp", "127.0.0.1:10004")
    for {
        var input string
        fmt.Scanln(&input)
        var output string
        err = cxn.Call("ProposerRole.Replicate", &input, &output)
        if err != nil { fmt.Println(err) }
    }

}
