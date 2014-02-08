package role

import (
    "time"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/clusterpeers"
    "github/paxoscluster/recovery"
)

type Node struct {
    roleId uint64
    acceptorEntity *acceptor.AcceptorRole
    proposerEntity *proposer.ProposerRole
    log *replicatedlog.Log
    peers *clusterpeers.Cluster
    disk *recovery.Manager
}

// Initialize proposer and acceptor roles
func ConstructNode(assignedId uint64) (*Node, string, error) {
    disk, err := recovery.ConstructManager()
    if err != nil { return nil, "", err }
    peers, roleId, address, err := clusterpeers.ConstructCluster(assignedId, disk)
    if err != nil { return nil, address, err }
    log, err := replicatedlog.ConstructLog(roleId, disk)
    if err != nil { return nil, address, err }
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
    if err != nil { return &node, address, err }
    err = handler.Register(proposerRole)
    if err != nil { return &node, address, err }
    err = peers.Listen(handler)
    if err != nil { return &node, address, err }

    return &node, address, nil
}

func (this *Node) Run() error {
    // Connects to peers
    this.peers.Connect()

    // Dispatches heartbeat signal
    go func() {
        for {
            go this.peers.BroadcastHeartbeat(this.roleId)
            time.Sleep(time.Second)
        }
    }()

    // Begins leader election
    go proposer.Run(this.proposerEntity)

    return nil
}
