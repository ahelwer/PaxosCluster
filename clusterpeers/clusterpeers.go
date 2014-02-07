package clusterpeers

import (
    "os"
    "io"
    "fmt"
    "sync"
    "time"
    "net"
    "net/rpc"
    "strconv"
    "encoding/csv"
    "github/paxoscluster/acceptor"
)

type Cluster struct {
    roleId uint64
    nodes map[uint64]Peer
    registerBadConnection chan uint64
    skipPromiseCount uint64
    exclude sync.Mutex
}

type Peer struct {
    roleId uint64
    address string
    port string
    comm *rpc.Client
    requirePromise bool
}

type Response struct {
    Data interface{}
}

func ConstructCluster(assignedId uint64) (*Cluster, uint64, string, error) {
    newCluster := Cluster {
        nodes: make(map[uint64]Peer),
        registerBadConnection: make(chan uint64, 16),
        roleId: 0,
        skipPromiseCount: 0,
    }

    peersFile, err := os.Open("./coldstorage/peers.txt")
    defer peersFile.Close()
    if err != nil { return &newCluster, 0, "", err }
    peersFileReader := csv.NewReader(peersFile)

    for {
        record, err := peersFileReader.Read() 
        if err == io.EOF {
            break
        } else if err != nil {
            return &newCluster, 0, "", err
        }

        roleId, err := strconv.ParseUint(record[0], 10, 64)
        if err != nil { return &newCluster, 0, "", err }

        newPeer := Peer {
            roleId: roleId,
            address: record[1],
            port: record[2],
            comm: nil,
            requirePromise: true,
        }

        newCluster.nodes[roleId] = newPeer
    }

    // Auto-detects IP address, matches it to a RoleId
    if assignedId == 0 {
        name, err := os.Hostname()
        if err != nil { return &newCluster, 0, "", err }
        addresses, err := net.LookupIP(name)
        if err != nil { return &newCluster, 0, "", err }
        address := ""
        for _, ip := range addresses {
            ipv4 := ip.To4()
            if ipv4 != nil {
                address = ip.String()
                break
            }
        }

        for id, peer := range newCluster.nodes {
            if peer.address == address {
                newCluster.roleId = id
                break
            }
        }

        if newCluster.roleId == 0 {
            return &newCluster, 0, "", fmt.Errorf("Could not find address %s in peers table.", address)
        }
    } else {
        newCluster.roleId = assignedId
    }

    self := newCluster.nodes[newCluster.roleId]
    address := net.JoinHostPort(self.address, self.port)

    go newCluster.connectionManager()

    return &newCluster, newCluster.roleId, address, nil
}

// Sets server to listen on this node's port
func (this *Cluster) Listen(handler *rpc.Server) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Listens on specified address
    self := this.nodes[this.roleId]
    ln, err := net.Listen("tcp", net.JoinHostPort(self.address, self.port))
    if err != nil { return err }

    fmt.Println("[ NETWORK", this.roleId, "] Listening on", net.JoinHostPort(self.address, self.port))

    // Dispatches connection processing loop
    go func() {
        for {
            connection, err := ln.Accept()
            if err != nil { continue }
            go handler.ServeConn(connection)
        }
    }()

    return nil
}

// Initializes connections to cluster peers
func (this *Cluster) Connect() {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for roleId, peer := range this.nodes {
        connection, err := rpc.Dial("tcp", peer.address + ":" + peer.port)
        if err != nil {
            this.registerBadConnection <- roleId
        } else {
            peer.comm = connection
            this.nodes[roleId] = peer
        }
    }
}

// Triages connection complaints, organizes repair attempts
func (this *Cluster) connectionManager() {
    establishing := make(map[uint64]bool)
    connectionEstablished := make(chan uint64)
    for {
        select {
        case roleId := <- this.registerBadConnection:
            if !establishing[roleId] {
                fmt.Println("[ NETWORK", this.roleId, "] Attempting to establish connection to", roleId)
                establishing[roleId] = true
                go this.establishConnection(roleId, connectionEstablished)
            }
        case roleId := <- connectionEstablished:
            establishing[roleId] = false
            fmt.Println("[ NETWORK", this.roleId, "] Connection to", roleId, "has been established")
        }
    }
}

// Attempts to re-connect to the specified role
func (this *Cluster) establishConnection(roleId uint64, connectionEstablished chan<- uint64) {
    this.exclude.Lock()
    peer := this.nodes[roleId]
    this.exclude.Unlock()

    for {
        connection, err := rpc.Dial("tcp", peer.address + ":" + peer.port)            
        if err != nil {
            time.Sleep(time.Second)
            continue
        }

        this.exclude.Lock()
        peer = this.nodes[roleId] 
        peer.comm = connection
        this.nodes[roleId] = peer
        connectionEstablished <- roleId
        this.exclude.Unlock()
        return
    }
}

// Returns number of peers in cluster
func (this *Cluster) GetPeerCount() uint64 {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return uint64(len(this.nodes))
}

// Returns number of peers from which no promise is required
func (this *Cluster) GetSkipPromiseCount() uint64 {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return this.skipPromiseCount
}

// Mark whether a promise is required from a node before sending accept requests
func (this *Cluster) SetPromiseRequirement(roleId uint64, required bool) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peer := this.nodes[roleId]

    // Value will be updated; therefore, update skipPromiseCount
    if peer.requirePromise != required {
        if required {
            this.skipPromiseCount--
        } else {
            this.skipPromiseCount++
        }
    }

    peer.requirePromise = required
    this.nodes[roleId] = peer
}

// Sends pulse to all nodes in the cluster
func (this *Cluster) BroadcastHeartbeat(roleId uint64) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := len(this.nodes)
    endpoint := make(chan *rpc.Call, peerCount)
    for _, peer := range this.nodes {
        if peer.comm != nil {
            var reply uint64
            peer.comm.Go("ProposerRole.Heartbeat", &roleId, &reply, endpoint)
        }
    }

    // Records nodes which return the heartbeat signal
    received := make(map[uint64]bool)
    failures := false
    replyCount := 0
    for replyCount < peerCount {
        select {
        case reply := <- endpoint:
            if reply.Error == nil {
                id := *reply.Reply.(*uint64)
                received[id] = true 
            } else {
                failures = true
            }
            replyCount++
        case <- time.After(time.Second/2):
            failures = true
            replyCount = peerCount
        }
    }
    
    // Registers bad connections if reply was not received
    if failures {
        for roleId := range this.nodes {
            if !received[roleId] {
                this.registerBadConnection <- roleId
            }
        }
    }
}

// Broadcasts a prepare phase request to the cluster
func (this *Cluster) BroadcastPrepareRequest(request acceptor.PrepareReq) (uint64, <-chan Response) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := uint64(0)
    nodeCount := uint64(len(this.nodes))
    endpoint := make(chan *rpc.Call, nodeCount)

    if this.skipPromiseCount < nodeCount/2+1 {
        for _, peer := range this.nodes {
            if peer.requirePromise && peer.comm != nil {
                var response acceptor.PrepareResp
                peer.comm.Go("AcceptorRole.Prepare", &request, &response, endpoint)
                peerCount++
            } 
        }
    } else {
        fmt.Println("[ NETWORK", this.roleId, "] Skipping prepare phase: know state of majority")
    }


    responses := make(chan Response, peerCount)
    go this.wrapReply(peerCount, endpoint, responses)
    return peerCount, responses 
}

// Broadcasts a proposal phase request to the cluster
func (this *Cluster) BroadcastProposalRequest(request acceptor.ProposalReq, filter map[uint64]bool) (uint64, <-chan Response) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := uint64(0)
    endpoint := make(chan *rpc.Call, len(this.nodes)) 
    for roleId, peer := range this.nodes {
        if !filter[roleId] && peer.comm != nil {
            var response acceptor.ProposalResp
            peer.comm.Go("AcceptorRole.Accept", &request, &response, endpoint)
            peerCount++
        }
    }

    responses := make(chan Response, peerCount)
    go this.wrapReply(peerCount, endpoint, responses)
    return peerCount, responses 
}

// Directly notifies a specific node of a chosen value
func (this *Cluster) NotifyOfSuccess(roleId uint64, info acceptor.SuccessNotify) <-chan Response {
    endpoint := make(chan *rpc.Call, 1)
    var firstUnchosenIndex int
    this.nodes[roleId].comm.Go("AcceptorRole.Success", &info, &firstUnchosenIndex, endpoint)

    response := make(chan Response)
    go this.wrapReply(1, endpoint, response)
    return response
}

// Wraps RPC return data to remove direct dependency of caller on net/rpc and improve testability
func (this *Cluster) wrapReply(peerCount uint64, endpoint <-chan *rpc.Call, forward chan<- Response) {
    replyCount := uint64(0)
    for replyCount < peerCount {
        select {
        case reply := <- endpoint:
            if reply.Error == nil {
                forward <- Response{reply.Reply}
            }
            replyCount++
        case <- time.After(2*time.Second):
            return
        }
    }
}
