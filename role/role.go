package role

import (
    "time"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/clusterpeers"
)

type Node struct {
    roleId uint64
    acceptorEntity *acceptor.AcceptorRole
    proposerEntity *proposer.ProposerRole
    log *replicatedlog.Log
    peers *clusterpeers.Cluster
}

// Initialize proposer and acceptor roles
func ConstructNode(assignedId uint64) (*Node, error) {
    peers, roleId, err := clusterpeers.ConstructCluster(assignedId)
    log, err := replicatedlog.ConstructLog(roleId)
    if err != nil { return nil, err }
    acceptorRole := acceptor.Construct(roleId, log)
    proposerRole := proposer.Construct(roleId, log, peers)
    node := Node {
        roleId: roleId,
        acceptorEntity: acceptorRole,
        proposerEntity: proposerRole,
        log: log,
        peers: peers,
    }

    handler := rpc.NewServer()
    err = handler.Register(acceptorRole)
    if err != nil { return &node, err }
    err = handler.Register(proposerRole)
    if err != nil { return &node, err }
    err = peers.Listen(handler)
    if err != nil { return &node, err }

    return &node, nil
}

func (this *Node) Run() error {
    // Connects to peers
    err := this.peers.Connect()
    if err != nil { return err }

    // Dispatches heartbeat signal
    go func() {
        for {
            this.peers.BroadcastHeartbeat(this.roleId)
            time.Sleep(time.Second)
        }
    }()

    // Begins leader election
    go proposer.Run(this.proposerEntity)

    return nil
}
