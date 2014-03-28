package role

import (
    "time"
    "net/rpc"
    "github/paxoscluster/proposer"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/clusterpeers"
    "github/paxoscluster/recovery"
    "github/paxoscluster/proposal/manager"
)

// Initialize proposer and acceptor roles
func LaunchNode(assignedId uint64, disk *recovery.Manager) (string, error) {
    cluster, roleId, address, err := clusterpeers.ConstructCluster(assignedId, disk)
    if err != nil { return address, err }
    log, err := replicatedlog.ConstructLog(roleId, disk)
    if err != nil { return address, err }
    proposals, err := manager.ConstructProposalManager(roleId, disk)
    if err != nil { return address, err }

    acceptorRole := acceptor.Construct(roleId, log)
    proposerRole := proposer.Construct(roleId, proposals, log, cluster)

    handler := rpc.NewServer()
    err = handler.Register(acceptorRole)
    if err != nil { return address, err }
    err = handler.Register(proposerRole)
    if err != nil { return address, err }
    err = cluster.Listen(handler)
    if err != nil { return address, err }

    // Connects to peers
    go cluster.Connect()

    // Dispatches heartbeat signal
    go func() {
        for {
            go cluster.BroadcastHeartbeat(roleId)
            time.Sleep(time.Second)
        }
    }()

    // Begins leader election
    go proposer.Run(proposerRole)

    return address, nil
}
