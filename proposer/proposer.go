package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "github/paxoscluster/role"
)

/*
 * Proposer Role
 */
type ProposerRole struct {
    role.Role
    ProposalId uint64
    Value string
}

// Connects to Acceptors
func (this *ProposerRole) connect() (map[uint64]*rpc.Client, error) {
    acceptors := make(map[uint64]*rpc.Client)
    for key, val := range this.Role.Peers {
        cxn, err := rpc.Dial("tcp", val)
        if err != nil { return acceptors, err }
        acceptors[key] = cxn
    }
    return acceptors, nil
}

// Prepare phase
func (this *ProposerRole) preparePhase(acceptors map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(this.Role.Peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out promise requests
    req := &role.PromiseReq{this.ProposalId}
    for _, acceptor := range acceptors {
        var promiseReply role.Promise
        acceptor.Go("AcceptorRole.Prepare", req, &promiseReply, endpoint)
    }
    
    // Waits for promises from majority of acceptors
    replyCount := 0
    promiseCount := 0
    var highestAccepted uint64 = 0
    for promiseCount < majority && replyCount < peerCount {
        var promise role.Promise
        select {
            case reply := <- endpoint: 
                if reply.Error != nil { return false, reply.Error }
                promise = *reply.Reply.(*role.Promise)
                replyCount++
            case <- time.After(1000000000):
                fmt.Println("Prepare phase time-out: proposal", this.ProposalId)
                return false, nil
        }

        if promise.PromiseAccepted {
            promiseCount++
            if promise.AcceptedProposalId > highestAccepted {
                highestAccepted = promise.AcceptedProposalId
                this.Value = promise.AcceptedValue
            }
        }
    }

    fmt.Println("Processed", replyCount, "replies with", promiseCount, "promises.")
    return promiseCount >= majority, nil
}

// Proposal phase
func (this *ProposerRole) proposalPhase(acceptors map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(this.Role.Peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    proposal := &role.Proposal{this.ProposalId, this.Value}
    for _, acceptor := range acceptors {
        var proposalReply uint64
        acceptor.Go("AcceptorRole.Accept", proposal, &proposalReply, endpoint)
    }

    // Waits for acceptance from majority of acceptors
    acceptCount := 0
    for acceptCount < majority {
        var acceptedId uint64
        select {
            case reply := <- endpoint :
                if reply.Error != nil { return false, reply.Error }
                acceptedId = *reply.Reply.(*uint64)
            case <- time.After(1000000000):
                fmt.Println("Accept phase time-out: proposal", this.ProposalId)
                return false, nil
        }

        if acceptedId <= this.ProposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    fmt.Println("Majority accepted proposal", this.ProposalId, "with value", this.Value)
    return true, nil
}

func (this *ProposerRole) Run() {
    // Connects to acceptors
    acceptors, err := this.connect()
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }

    // Initiates Paxos protocol
    notChosen := true
    this.ProposalId = 0
    this.Value = <- this.Role.Client
    if err != nil {
        fmt.Println("Client request error", err)
        return
    }

    for notChosen {
        this.ProposalId += this.Role.RoleId

        // Executes prepare phase
        success, err := this.preparePhase(acceptors)
        if err != nil {
            fmt.Println("Prepare phase error:", err)
            return
        }

        // Executes proposal phase
        if success {
            success, err = this.proposalPhase(acceptors)
            if err != nil {
                fmt.Println("Proposal phase error:", err)
                return
            }
            notChosen = !success
        }
    }
}
