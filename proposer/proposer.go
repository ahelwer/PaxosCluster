package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "errors"
    "math"
    "github/paxoscluster/replicatedlog"
    "github/paxoscluster/acceptor"
)

type ProposerRole struct {
    roleId uint64
    proposalId uint64
    log *replicatedlog.Log
    client chan ClientRequest
    heartbeat chan uint64
    terminator chan bool
}

// Constructor for ProposerRole
func Construct(roleId uint64, log *replicatedlog.Log) *ProposerRole {
    client := make(chan ClientRequest)
    heartbeat := make(chan uint64)
    terminator := make(chan bool)
    this := ProposerRole{roleId, 0, log, client, heartbeat, terminator}
    return &this
}

// Starts proposer role state machine
func Run (this *ProposerRole, peers map[uint64]*rpc.Client) {
    isLeaderStateChannel := make(chan bool)
    isNotLeaderStateChannel := make(chan bool)
    go this.isNotLeaderState(isLeaderStateChannel, isNotLeaderStateChannel)
    go this.isLeaderState(isNotLeaderStateChannel, isLeaderStateChannel, peers)
}

// Role is not leader; will reject client requests
func (this *ProposerRole) isNotLeaderState(trans chan<- bool, self <-chan bool) {
    electionNotify := make(chan bool)
    go this.electLeader(electionNotify)

    for {
        select {
        case <- electionNotify:
            trans <- true
            <- self
            this.electLeader(electionNotify)
        case request := <- this.client:
            request.reply <- errors.New("This role is not the cluster leader.")
        case <- this.terminator:
            return
        }
    }
}

// Role is leader; will furnish client requests
func (this *ProposerRole) isLeaderState(trans chan<- bool, self <-chan bool, peers map[uint64]*rpc.Client) {
    <- self

    for {
        select {
        case <- this.heartbeat:
            trans <- true
            <- self
        case request := <- this.client:
            go func () { request.reply <- this.paxos(request.value, peers) }()
        case <- this.terminator:
            return
        }
    }
}

// Elects self leader if not receiving heartbeat signal from role with higher ID
func (this *ProposerRole) electLeader(electionNotify chan bool) {
    for {
        select {
        case <- this.heartbeat:
            continue
        case <- time.After(2*time.Second):
            electionNotify <- true
            return
        }
    }
}

// Executes single round of Paxos protocol
func (this *ProposerRole) paxos(value string, peers map[uint64]*rpc.Client) error {
    index := 0
    notChosen := true

    for notChosen {
        this.proposalId += this.roleId

        // Executes prepare phase
        success, index, value, err := this.preparePhase(value, peers)
        if err != nil { return err }

        // Executes proposal phase
        if success {
            success, err = this.proposalPhase(index, value, peers)
            if err != nil { return err }
            notChosen = !success
        }
    }

    this.log.SetEntryAt(index, value, math.MaxUint64)

    return nil
}

// Prepare phase
func (this *ProposerRole) preparePhase(value string, peers map[uint64]*rpc.Client) (bool, int, string, error) {
    peerCount := len(peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)
    index, err := this.log.FirstEntryNotChosen()
    if err != nil { return false, index, value, err }

    // Sends out promise requests
    request := acceptor.PrepareReq{this.proposalId, index}
    for _, peer := range peers {
        var response acceptor.PrepareResp
        peer.Go("AcceptorRole.Prepare", &request, &response, endpoint)
    }
    
    // Waits for promises from majority of acceptors
    replyCount := 0
    promiseCount := 0
    var highestAccepted uint64 = 0
    for promiseCount < majority && replyCount < peerCount {
        var promise acceptor.PrepareResp
        select {
            case reply := <- endpoint: 
                if reply.Error != nil { return false, index, value, reply.Error }
                promise = *reply.Reply.(*acceptor.PrepareResp)
                replyCount++
            case <- time.After(time.Second):
                fmt.Println("Prepare phase time-out: proposal", this.proposalId)
                return false, index, value, nil
        }

        if promise.PromiseAccepted {
            promiseCount++
            if promise.AcceptedProposalId > highestAccepted {
                highestAccepted = promise.AcceptedProposalId
                value = promise.AcceptedValue
            }
        }
    }

    fmt.Println("Processed", replyCount, "replies with", promiseCount, "promises.")
    return promiseCount >= majority, index, value, nil
}

// Proposal phase
func (this *ProposerRole) proposalPhase(index int, value string, peers map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    request := acceptor.ProposalReq{this.proposalId, index, value}
    for _, peer := range peers {
        var response acceptor.ProposalResp
        peer.Go("AcceptorRole.Accept", &request, &response, endpoint)
    }

    // Waits for acceptance from majority of acceptors
    acceptCount := 0
    for acceptCount < majority {
        var response acceptor.ProposalResp
        select {
            case reply := <- endpoint :
                if reply.Error != nil { return false, reply.Error }
                response = *reply.Reply.(*acceptor.ProposalResp)
            case <- time.After(time.Second):
                fmt.Println("Accept phase time-out: proposal", this.proposalId)
                return false, nil
        }

        if response.AcceptedId <= this.proposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    fmt.Println("Majority accepted proposal", this.proposalId, "with value", value)
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
