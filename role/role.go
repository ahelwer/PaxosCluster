package role

import (
    "fmt"
    "time"
    "net"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
)

// Initialize proposer and acceptor roles
func Initialize(roleId uint64, client chan string, address string) {
    acceptorRole := acceptor.AcceptorRole{roleId, 0, 0, ""}
    proposerRole := proposer.ProposerRole{roleId, client, 0, ""}

    // Registers with RPC server
    handler := rpc.NewServer()
    err := handler.Register(&acceptorRole)
    if err != nil {
        fmt.Println("Failed to register Acceptor", roleId, err)
        return
    }
    err = handler.Register(&proposerRole)
    if err != nil {
        fmt.Println("Failed to register Proposer", roleId, err)
        return
    }

    // Listens on specified address
    ln, err := net.Listen("tcp", address)
    if err != nil {
        fmt.Println("Listening error:", err)
        return
    }

    // Dispatches connection processing loop
    go func() {
        for {
            cxn, err := ln.Accept()
            if err != nil { continue }
            go handler.ServeConn(cxn)
        }
    }()
}

func Run(roleId uint64, addresses map[uint64]string) {
    // Connects to peers
    peers, err := connect(addresses)
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }

    // Dispatches heartbeat signal
    go heartbeat(roleId, peers)
}

// Connects to peers
func connect(addresses map[uint64]string) (map[uint64]*rpc.Client, error) {
    peers := make(map[uint64]*rpc.Client)
    for key, val := range addresses {
        cxn, err := rpc.Dial("tcp", val)
        if err != nil { return peers, err }
        peers[key] = cxn
    }
    return peers, nil
}

// Sends hearbeat signal to peers
func heartbeat(roleId uint64, peers map[uint64]*rpc.Client) {
    for {
        for _, peer := range peers {
            request := roleId 
            var response bool
            peer.Go("ProposerRole.Heartbeat", &request, &response, nil)
        }
        time.Sleep(1000 * time.Millisecond)
    }
}
