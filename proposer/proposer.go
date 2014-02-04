package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "errors"
    "math"
    "sync"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/acceptor"
)

type Peer struct {
    roleId uint64
    comm *rpc.Client
}

type ProposerRole struct {
    roleId uint64
    peers []Peer
    proposalCount uint64
    log *replicatedlog.Log
    client chan ClientRequest
    heartbeat chan uint64
    terminator chan bool
    exclude sync.Mutex
}

// Constructor for ProposerRole
func Construct(roleId uint64, log *replicatedlog.Log) *ProposerRole {
    client := make(chan ClientRequest)
    heartbeat := make(chan uint64)
    terminator := make(chan bool)
    this := ProposerRole {
        roleId: roleId,
        peers: nil,
        proposalCount: 1,
        log: log,
        client: client,
        heartbeat: heartbeat,
        terminator: terminator,
    }
    return &this
}

// Starts proposer role state machine
func Run (this *ProposerRole, peers map[uint64]*rpc.Client) {
    for roleId, peer := range peers {
        this.peers = append(this.peers, Peer{roleId, peer}) 
    }

    isLeaderStateChannel := make(chan bool)
    isNotLeaderStateChannel := make(chan bool)
    go this.isNotLeaderState(isLeaderStateChannel, isNotLeaderStateChannel)
    go this.isLeaderState(isNotLeaderStateChannel, isLeaderStateChannel)
}

// Role is not leader; will reject client requests
func (this *ProposerRole) isNotLeaderState(trans chan<- bool, self <-chan bool) {
    electionNotify := make(chan bool)
    startElection := make(chan bool)
    go this.electLeader(startElection, electionNotify)

    for {
        select {
        case <- electionNotify:
            trans <- true
            <- self
            startElection <- true
        case request := <- this.client:
            request.reply <- errors.New("This role is not the cluster leader.")
        case <- this.terminator:
            return
        }
    }
}

// Elects self leader if not receiving heartbeat signal from role with higher ID
func (this *ProposerRole) electLeader(startElection <-chan bool, electionNotify chan<- bool) {
    for {
        select {
        case <- this.heartbeat:
            continue
        case <- time.After(2*time.Second):
            electionNotify <- true
            <- startElection
        }
    }
}

// Role is leader; will furnish client requests
func (this *ProposerRole) isLeaderState(trans chan<- bool, self <-chan bool) {
    <- self

    for {
        select {
        case <- this.heartbeat:
            trans <- true
            <- self
        case request := <- this.client:
            go func () { request.reply <- this.paxos(request.value) }()
        case <- this.terminator:
            return
        }
    }
}

// Generates a new probably-globally-unique proposal ID with ordering property
func (this *ProposerRole) generateNextProposalId() uint64 {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    proposalId := this.proposalCount * uint64(len(this.peers)) + this.roleId
    this.proposalCount++
    return proposalId
}

// Executes single round of Paxos protocol
func (this *ProposerRole) paxos(value string) error {
    chosen := false

    for !chosen {
        index := this.log.FirstEntryNotChosen()
        proposalId := this.generateNextProposalId()
        usingValue := value

        // Prepare phase
        peerCount, endpoint := this.sendPrepareRequests(proposalId, index)
        success, changed, changedValue, err := recvPromises(peerCount, endpoint)
        if err != nil { return err }

        if success {
            if changed {
                usingValue = changedValue
            }

            // Proposal phase
            request := acceptor.ProposalReq{proposalId, index, usingValue}
            peerCount, endpoint := this.sendProposalRequests(request)
            success, err = recvAccepts(proposalId, peerCount, endpoint)
            if err != nil { return err }

            if success {
                fmt.Println("Chose ProposalId:", proposalId, "Index:", index, "Value:", usingValue)
                this.log.SetEntryAt(index, usingValue, math.MaxUint64)
                chosen = !changed
            }
        }
    }

    return nil
}

// Sends out prepare requests to peers
func (this *ProposerRole) sendPrepareRequests(proposalId uint64, index int) (int, <-chan *rpc.Call) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := len(this.peers)
    endpoint := make(chan *rpc.Call, peerCount)
    request := acceptor.PrepareReq{proposalId, index}
    for _, peer := range this.peers {
        var response acceptor.PrepareResp
        peer.comm.Go("AcceptorRole.Prepare", &request, &response, endpoint)
    }
    return peerCount, endpoint 
}

// Receves replies to prepare requests
func recvPromises(peerCount int, endpoint <-chan *rpc.Call) (bool, bool, string, error) {
    success := false
    changed := false
    value := ""
    majority := peerCount/2+1
    replyCount := 0
    promiseCount := 0
    highestAccepted := uint64(0)
    for promiseCount < majority && replyCount < peerCount {
        var promise acceptor.PrepareResp
        select {
        case reply := <- endpoint:
            if reply.Error != nil { return success, changed, value, reply.Error }
            promise = *reply.Reply.(*acceptor.PrepareResp)
            replyCount++
        case <- time.After(time.Second):
            return success, changed, value, nil
        }

        if promise.PromiseAccepted {
            promiseCount++
            if promise.AcceptedProposalId > highestAccepted {
                highestAccepted = promise.AcceptedProposalId
                changed = true
                value = promise.AcceptedValue
            }
        }
    }

    fmt.Println("Processed", replyCount, "replies with", promiseCount, "promises.")
    success = promiseCount >= majority
    return success, changed, value, nil
}

// Sends out proposal requests to peers
func (this *ProposerRole) sendProposalRequests(request acceptor.ProposalReq) (int, <-chan *rpc.Call) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    peerCount := len(this.peers)
    endpoint := make(chan *rpc.Call, peerCount)
    for _, peer := range this.peers {
        var response acceptor.ProposalResp
        peer.comm.Go("AcceptorRole.Accept", &request, &response, endpoint)
    }
    return peerCount, endpoint 
}


// Receves replies to proposal
func recvAccepts(proposalId uint64, peerCount int, endpoint <-chan *rpc.Call) (bool, error) {
    majority := peerCount/2+1
    acceptCount := 0
    for acceptCount < majority {
        var response acceptor.ProposalResp
        select {
            case reply := <- endpoint :
                if reply.Error != nil { return false, reply.Error }
                response = *reply.Reply.(*acceptor.ProposalResp)
            case <- time.After(time.Second):
                return false, nil
        }

        if response.AcceptedId <= proposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    return true, nil
}

// Catches heartbeat signal as a remote procedure call
func (this *ProposerRole) Heartbeat(req *uint64, reply *bool) error {
    if this.roleId < *req {
        this.heartbeat <- *req
    }
    *reply = true
    return nil
}

type ClientRequest struct {
    value string
    reply chan error
}

// Receives requests from client
func (this *ProposerRole) Replicate(value *string, retValue *string) error {
    fmt.Println("Role", this.roleId, "received client request:", *value)
    replyChannel := make(chan error)
    request := ClientRequest{*value, replyChannel}
    this.client <- request
    err := <- replyChannel
    *retValue = *value
    return err
}

// Receives termination command
func (this *ProposerRole) Terminate(req *bool, reply *bool) error {
    this.terminator <- *req
    *reply = *req
    return nil
}
