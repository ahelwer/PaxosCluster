package main

import (
    "os"
    "fmt"
    "net/rpc"
    "github/paxoscluster/role"
    "github/paxoscluster/recovery"
)

func main() {
    nodeAddress := ""

    disk, err := recovery.ConstructManager()
    if err != nil {
        fmt.Println(err)
        return
    }

    if len(os.Args) > 1 {
        roles := []uint64{1,2,3,4,5}
        var addresses []string = nil
        for _, roleId := range roles {
            address, err := role.LaunchNode(roleId, disk)
            if err != nil {
                fmt.Println(err)
                return
            }
            addresses = append(addresses, address)
        }

        nodeAddress = addresses[len(addresses)-1]
    } else {
        address, err := role.LaunchNode(0, disk)
        if err != nil {
            fmt.Println(err)
            return
        }

        nodeAddress = address
    }

    cxn, err := rpc.Dial("tcp", nodeAddress)
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
}
