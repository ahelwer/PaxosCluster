package role

import (
    "os"
    "io"
    "net"
    "fmt"
    "time"
    "net/rpc"
    "strconv"
    "encoding/csv"
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
func ConstructNode(roleId uint64) (*Node, string, error) {
    peersFile, err := os.Open("./coldstorage/peers.txt")
    defer peersFile.Close()
    if err != nil { return nil, "", err }
    peersFileReader := csv.NewReader(peersFile)

    err = nil
    addresses := make(map[uint64]string)
    for {
        record, err := peersFileReader.Read() 
        if err == io.EOF {
            break
        } else if err != nil {
            return nil, "", err
        }
        roleId, err := strconv.ParseUint(record[0], 10, 64)
        if err != nil { return nil, "", err }
        addresses[roleId] = record[1]
    }
    fmt.Println(addresses)
    address := addresses[roleId]

    log, err := replicatedlog.ConstructLog(roleId)
    if err != nil { return nil, "", err }
    peers := clusterpeers.Construct(addresses)
    acceptorRole := acceptor.Construct(roleId, log)
    proposerRole := proposer.Construct(roleId, log, peers)
    node := Node {
        roleId: roleId,
        acceptorEntity: acceptorRole,
        proposerEntity: proposerRole,
        log: log,
        peers: peers,
    }

    // Registers with RPC server
    handler := rpc.NewServer()
    err = handler.Register(acceptorRole)
    if err != nil { return &node, address, err }
    err = handler.Register(proposerRole)
    if err != nil { return &node, address, err }

    // Listens on specified address
    ln, err := net.Listen("tcp", address)
    if err != nil { return &node, address, err }

    // Dispatches connection processing loop
    go func() {
        for {
            connection, err := ln.Accept()
            if err != nil { continue }
            go handler.ServeConn(connection)
        }
    }()

    return &node, address, nil
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
