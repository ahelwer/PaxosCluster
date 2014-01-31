package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "github/paxoscluster/acceptor"
)

/*
 * Proposer Role
 */
type ProposerRole struct {
    RoleId uint64
    Client chan string
    ProposalId uint64
    Value string
}

func (this *ProposerRole) Heartbeat(req *uint64, reply *bool) error {
    fmt.Println("Role", this.RoleId, "received heartbeat from", *req)
    *reply = true
    return nil
}

func (this *ProposerRole) electLeader() (bool, error) {
    return true, nil
}

// Executes single rount of Paxos protocol
func (this *ProposerRole) paxos(acceptors map[uint64]*rpc.Client) (error) {
    notChosen := true
    this.ProposalId = 0
    this.Value = <- this.Client

    for notChosen {
        this.ProposalId += this.RoleId

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
    request := &acceptor.PrepareReq{this.ProposalId}
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
func (this *ProposerRole) proposalPhase(peers map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    request := &acceptor.ProposalReq{this.ProposalId, this.Value}
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
                fmt.Println("Accept phase time-out: proposal", this.ProposalId)
                return false, nil
        }

        if response.AcceptedId <= this.ProposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    fmt.Println("Majority accepted proposal", this.ProposalId, "with value", this.Value)
    return true, nil
}
