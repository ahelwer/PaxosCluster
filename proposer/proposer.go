package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "github/paxoscluster/acceptor"
)

type ProposerRole struct {
    roleId uint64
    proposalId uint64
    value string
    client chan string
    heartbeat chan uint64
}

// Constructor for ProposerRole
func Construct(roleId uint64) *ProposerRole {
    client := make(chan string, 32)
    heartbeat := make(chan uint64, 32)
    this := ProposerRole{roleId, 0, "", client, heartbeat}
    return &this
}

func Run (this *ProposerRole, roleId uint64, peers map[uint64]*rpc.Client) {
    this.electLeader()
    fmt.Println("Elected role", roleId, "as leader.")

    for {
        select {
            case <- this.heartbeat:
                fmt.Println("Role", roleId, "stepping down as leader.")
                this.electLeader()
            default:
                this.paxos(peers)
        }
    }
}

// Elects self leader if have not received heartbeat from node with higher ID for two seconds
func (this *ProposerRole) electLeader() {
    for {
        select {
            case <- this.heartbeat:
                continue 
            case <- time.After(2*time.Second):
                return
        }
    }
}

// Executes single rount of Paxos protocol
func (this *ProposerRole) paxos(acceptors map[uint64]*rpc.Client) (error) {
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

// Receives requests from client
func (this *ProposerRole) Replicate(req *string, reply *string) error {
    this.client <- *req
    fmt.Println("Role", this.roleId, "received client request:", *req)
    *reply = *req
    return nil
}

