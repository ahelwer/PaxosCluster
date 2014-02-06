package main

import (
    "os"
    "fmt"
    "net/rpc"
    "github/paxoscluster/role"
)

func main() {
    if len(os.Args) > 1 {
        roles := []uint64{1,2,3,4,5}
        var nodes []*role.Node = nil
        var addresses []string = nil
        for _, roleId := range roles {
            node, address, err := role.ConstructNode(roleId)
            if err != nil {
                fmt.Println(err)
                return
            }
            nodes = append(nodes, node)
            addresses = append(addresses, address)
        }

        for _, node := range nodes {
            err := node.Run()
            if err != nil {
                fmt.Println(err)
                return
            }
        }

        cxn, err := rpc.Dial("tcp", addresses[len(addresses)-1])
        if err != nil {
            fmt.Println(err)
            return
        }
        for {
            var input string
            fmt.Scanln(&input)
            var output string
            err = cxn.Call("ProposerRole.Replicate", &input, &output)
            if err != nil { fmt.Println(err) }
        }
    } else {
        node, address, err := role.ConstructNode(0)
        if err != nil {
            fmt.Println(err)
            return
        }

        fmt.Print("Press Enter once all nodes are running: ")
        var run string
        fmt.Scanln(&run)

        err = node.Run()
        if err != nil {
            fmt.Println(err)
            return
        }
        cxn, err := rpc.Dial("tcp", address)
        if err != nil {
            fmt.Println(err)
            return
        }
        for {
            fmt.Println("Enter string to be replicated: ")
            var input string
            fmt.Scanln(&input)
            var output string
            err = cxn.Call("ProposerRole.Replicate", &input, &output)
            if err != nil { fmt.Println(err) }
        }
    }
}
