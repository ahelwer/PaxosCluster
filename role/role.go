package role

import (
    "time"
    "net"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/replicatedlog"
)

type Node struct {
    acceptorEntity *acceptor.AcceptorRole
    proposerEntity *proposer.ProposerRole
}

// Initialize proposer and acceptor roles
func Initialize(roleId uint64, address string) (Node, error) {
    log := replicatedlog.Construct()
    acceptorRole := acceptor.Construct(roleId, log)
    proposerRole := proposer.Construct(roleId, log)
    node := Node{acceptorRole, proposerRole}

    // Registers with RPC server
    handler := rpc.NewServer()
    err := handler.Register(acceptorRole)
    if err != nil { return node, err }
    err = handler.Register(proposerRole)
    if err != nil { return node, err }

    // Listens on specified address
    ln, err := net.Listen("tcp", address)
    if err != nil { return node, err }

    // Dispatches connection processing loop
    go func() {
        for {
            cxn, err := ln.Accept()
            if err != nil { continue }
            go handler.ServeConn(cxn)
        }
    }()

    return node, nil
}

func Run(roleId uint64, node Node, addresses map[uint64]string) (error) {
    // Connects to peers
    peers, err := connect(addresses)
    if err != nil { return err }

    // Dispatches heartbeat signal
    go heartbeat(roleId, peers)

    // Begins leader election
    go proposer.Run(node.proposerEntity, peers)

    return nil
}

// Connect to peers
func connect(addresses map[uint64]string) (map[uint64]*rpc.Client, error) {
    peers := make(map[uint64]*rpc.Client)
    for key, val := range addresses {
        cxn, err := rpc.Dial("tcp", val)
        if err != nil { return peers, err }
        peers[key] = cxn
    }
    return peers, nil
}

// Send hearbeat signal to peers
func heartbeat(roleId uint64, peers map[uint64]*rpc.Client) {
    for {
        for _, peer := range peers {
            request := roleId 
            var response bool
            peer.Go("ProposerRole.Heartbeat", &request, &response, nil)
        }
        time.Sleep(time.Second)
    }
}
