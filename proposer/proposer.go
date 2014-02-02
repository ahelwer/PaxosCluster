package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "errors"
    "github/paxoscluster/acceptor"
)

type ProposerRole struct {
    roleId uint64
    proposalId uint64
    log []string
    client chan ClientRequest
    heartbeat chan uint64
    terminator chan bool
}

// Constructor for ProposerRole
func Construct(roleId uint64, log []string) *ProposerRole {
    client := make(chan ClientRequest)
    heartbeat := make(chan uint64)
    terminator := make(chan bool)
    this := ProposerRole{roleId, 0, log, client, heartbeat, terminator}
    return &this
}

// Starts proposer role state machine
func Run (this *ProposerRole, peers map[uint64]*rpc.Client) {
    go this.isNotLeaderState()
}

// Role is not leader; will reject client requests
func (this *ProposerRole) isNotLeaderState() {
    electionNotify := make(chan bool)

    go this.electLeader(electionNotify)

    for {
        select {
        case <- electionNotify
            go this.isLeaderState() 
            return
        case request <- this.client:
            request.reply <- errors.New("This role is not the cluster leader.")
        case <- this.terminator:
            return
        }
    }
}

// Role is leader; will furnish client requests
func (this *ProposerRole) isLeaderState() {
    for {
        select {
        case <- this.heartbeat:
            go this.isNotLeaderState()
            return
        case request <- this.client:
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
func (this *ProposerRole) paxos(value string, acceptors map[uint64]*rpc.Client) (error) {
    notChosen := true
    this.value = <- this.client

    for notChosen {
        this.proposalId += this.roleId

        // Executes prepare phase
        success, err := this.preparePhase(acceptors)
        if err != nil { return err }

        // Executes proposal phase
        if success {
            success, err = this.proposalPhase(acceptors)
            if err != nil { return err }
            notChosen = !success
        }
    }

    return nil
}

// Prepare phase
func (this *ProposerRole) preparePhase(peers map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out promise requests
    request := &acceptor.PrepareReq{this.proposalId}
    for _, peer := range peers {
        var response acceptor.PrepareResp
        peer.Go("AcceptorRole.Prepare", request, &response, endpoint)
    }
    
    // Waits for promises from majority of acceptors
    replyCount := 0
    promiseCount := 0
    var highestAccepted uint64 = 0
    for promiseCount < majority && replyCount < peerCount {
        var promise acceptor.PrepareResp
        select {
            case reply := <- endpoint: 
                if reply.Error != nil { return false, reply.Error }
                promise = *reply.Reply.(*acceptor.PrepareResp)
                replyCount++
            case <- time.After(1000000000):
                fmt.Println("Prepare phase time-out: proposal", this.proposalId)
                return false, nil
        }

        if promise.PromiseAccepted {
            promiseCount++
            if promise.AcceptedProposalId > highestAccepted {
                highestAccepted = promise.AcceptedProposalId
                this.value = promise.AcceptedValue
            }
        }
    }

    fmt.Println("Processed", replyCount, "replies with", promiseCount, "promises.")
    return promiseCount >= majority, nil
}

// Proposal phase
func (this *ProposerRole) proposalPhase(peers map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    request := &acceptor.ProposalReq{this.proposalId, this.value}
    for _, peer := range peers {
        var response acceptor.ProposalResp
        peer.Go("AcceptorRole.Accept", request, &response, endpoint)
    }

    // Waits for acceptance from majority of acceptors
    acceptCount := 0
    for acceptCount < majority {
        var response acceptor.ProposalResp
        select {
            case reply := <- endpoint :
                if reply.Error != nil { return false, reply.Error }
                response = *reply.Reply.(*acceptor.ProposalResp)
            case <- time.After(1000000000):
                fmt.Println("Accept phase time-out: proposal", this.proposalId)
                return false, nil
        }

        if response.AcceptedId <= this.proposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    fmt.Println("Majority accepted proposal", this.proposalId, "with value", this.value)
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
