package clusterpeers

import (
    "fmt"
    "sync"
    "net/rpc"
    "github/paxoscluster/acceptor"
)

type Cluster struct {
    nodes map[uint64]Peer
    hasConnected bool
    skipPromiseCount uint64
    exclude sync.Mutex
}

type Peer struct {
    roleId uint64
    address string
    comm *rpc.Client
    requirePromise bool
}

func Construct(addresses map[uint64]string) *Cluster {
    newCluster := Cluster {
        nodes: make(map[uint64]Peer),
        hasConnected: false,
        skipPromiseCount: 0,
    }

    for roleId, address := range addresses {
        newPeer := Peer {
            roleId: roleId,
            address: address,
            comm: nil,
            requirePromise: true,
        }
        newCluster.nodes[roleId] = newPeer 
    }

    return &newCluster
}

// Initializes connections to cluster peers
func (this *Cluster) Connect() error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    if this.hasConnected {
        return fmt.Errorf("Already connected to peers.")
    }

    for roleId, peer := range this.nodes {
        connection, err := rpc.Dial("tcp", peer.address)
        if err != nil { return err }
        peer.comm = connection
        this.nodes[roleId] = peer
    }

    this.hasConnected = true
    return nil
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
func (this *Cluster) BroadcastHeartbeat(roleId uint64) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    if !this.hasConnected {
        return fmt.Errorf("Connection to peers has not been established.")
    }

    for _, peer := range this.nodes {
        var reply bool
        peer.comm.Go("ProposerRole.Heartbeat", &roleId, &reply, nil)
    }

    return nil
}

// Broadcasts a prepare phase request to the cluster
func (this *Cluster) BroadcastPrepareRequest(request acceptor.PrepareReq) (uint64, <-chan *rpc.Call) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := uint64(0)
    nodeCount := uint64(len(this.nodes))
    endpoint := make(chan *rpc.Call, nodeCount)

    if this.skipPromiseCount < nodeCount/2+1 {
        for _, peer := range this.nodes {
            if peer.requirePromise {
                var response acceptor.PrepareResp
                peer.comm.Go("AcceptorRole.Prepare", &request, &response, endpoint)
                peerCount++
            }
        }
    } else {
        fmt.Println("Skipping prepare phase")
    }


    return peerCount, endpoint 
}

// Broadcasts a proposal phase request to the cluster
func (this *Cluster) BroadcastProposalRequest(request acceptor.ProposalReq) (uint64, <-chan *rpc.Call) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := uint64(0)
    endpoint := make(chan *rpc.Call, len(this.nodes)) 
    for _, peer := range this.nodes {
        var response acceptor.ProposalResp
        peer.comm.Go("AcceptorRole.Accept", &request, &response, endpoint)
        peerCount++
    }

    return peerCount, endpoint 
}
