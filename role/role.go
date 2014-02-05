package role

import (
    "fmt"
    "time"
    "net"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/clusterpeers"
)

type Node struct {
    acceptorEntity *acceptor.AcceptorRole
    proposerEntity *proposer.ProposerRole
    log *replicatedlog.Log
    peers *clusterpeers.Cluster
}

// Initialize proposer and acceptor roles
func Construct(roleId uint64, address string, addresses map[uint64]string) (*Node, error) {
    log := replicatedlog.Construct()
    peers := clusterpeers.Construct(addresses)
    acceptorRole := acceptor.Construct(roleId, log)
    proposerRole := proposer.Construct(roleId, log, peers)
    node := Node{acceptorRole, proposerRole, log, peers}

    // Registers with RPC server
    handler := rpc.NewServer()
    err := handler.Register(acceptorRole)
    if err != nil { return &node, err }
    err = handler.Register(proposerRole)
    if err != nil { return &node, err }

    // Listens on specified address
    ln, err := net.Listen("tcp", address)
    if err != nil { return &node, err }

    // Dispatches connection processing loop
    go func() {
        for {
            connection, err := ln.Accept()
            if err != nil { continue }
            go handler.ServeConn(connection)
        }
    }()

    return &node, nil
}

func (this *Node) Run(roleId uint64) error {
    // Connects to peers
    err := this.peers.Connect()
    if err != nil { return err }

    // Dispatches heartbeat signal
    go func() {
        for {
            err := this.peers.BroadcastHeartbeat(roleId)
            if err != nil {
                fmt.Println(err)
                return
            }
            time.Sleep(time.Second)
        }
    }()

    // Begins leader election
    go proposer.Run(this.proposerEntity)

    return nil
}
