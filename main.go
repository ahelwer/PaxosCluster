package main

import (
    "fmt"
    "net"
    "net/rpc"
    "time"
)

// Amazingly this function does not exist in the standard library
func max(a uint64, b uint64) uint64 {
    if (a > b) {
        return a
    } else {
        return b
    }
}

type Role struct {
    roleId uint64
    client chan string
    peers map[uint64]string
}

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    Role
    minProposalId uint64
    acceptedProposalId uint64
    acceptedValue string
}

type PromiseReq struct {
    ProposalId uint64
}

type Promise struct {
    PromiseAccepted bool
    AcceptedProposalId uint64
    AcceptedValue string
}

func (this *AcceptorRole) Prepare(req *PromiseReq, reply *Promise) error {
    fmt.Println("Acceptor", this.Role.roleId, "considering promise", req.ProposalId, "vs", this.minProposalId)
    reply.PromiseAccepted = req.ProposalId > this.minProposalId
    reply.AcceptedProposalId = this.acceptedProposalId
    reply.AcceptedValue = this.acceptedValue
    this.minProposalId = max(req.ProposalId, this.minProposalId)
    return nil
}

type Proposal struct {
    ProposalId uint64
    Value string
}

func (this *AcceptorRole) Accept(proposal *Proposal, reply *uint64) error {
    fmt.Println("Acceptor", this.Role.roleId, "considering proposal", proposal.ProposalId)
    if proposal.ProposalId >= this.minProposalId {
        this.acceptedProposalId = proposal.ProposalId
        this.acceptedValue = proposal.Value
    }
    *reply = this.minProposalId
    return nil
}

func (this *AcceptorRole) run(handler *rpc.Server, ln net.Listener) {
    for {
        cxn, err := ln.Accept()
        if err != nil { continue }
        go handler.ServeConn(cxn)
    }
}

/*
 * Proposer Role
 */
type ProposerRole struct {
    Role
    proposalId uint64
    value string
}

// Connects to Acceptors
func (this *ProposerRole) connect() (map[uint64]*rpc.Client, error) {
    acceptors := make(map[uint64]*rpc.Client)
    for key, val := range this.Role.peers {
        cxn, err := rpc.Dial("tcp", val)
        if err != nil { return acceptors, err }
        acceptors[key] = cxn
    }
    return acceptors, nil
}

// Prepare phase
func (this *ProposerRole) preparePhase(acceptors map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(this.Role.peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out promise requests
    req := &PromiseReq{this.proposalId}
    for _, acceptor := range acceptors {
        var promiseReply Promise
        acceptor.Go("AcceptorRole.Prepare", req, &promiseReply, endpoint)
    }
    
    // Waits for promises from majority of acceptors
    replyCount := 0
    promiseCount := 0
    var highestAccepted uint64 = 0
    for promiseCount < majority && replyCount < peerCount {
        var promise Promise
        select {
            case reply := <- endpoint: 
                if reply.Error != nil { return false, reply.Error }
                promise = *reply.Reply.(*Promise)
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
func (this *ProposerRole) proposalPhase(acceptors map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(this.Role.peers)
    majority := peerCount / 2 + 1
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    proposal := &Proposal{this.proposalId, this.value}
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
                fmt.Println("Accept phase time-out: proposal", this.proposalId)
                return false, nil
        }

        if acceptedId <= this.proposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

    fmt.Println("Majority accepted proposal", this.proposalId, "with value", this.value)
    return true, nil
}

func (this *ProposerRole) run() {
    // Connects to acceptors
    acceptors, err := this.connect()
    if err != nil {
        fmt.Println("Connection error:", err)
        return
    }

    // Initiates Paxos protocol
    notChosen := true
    this.proposalId = 0
    this.value = <- this.Role.client
    if err != nil {
        fmt.Println("Client request error", err)
        return
    }

    for notChosen {
        this.proposalId += this.Role.roleId

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

func main() {
    client := make(chan string)
    peers := map[uint64]string {
        1: "127.0.0.1:10000",
        2: "127.0.0.1:10001",
        3: "127.0.0.1:10002",
        4: "127.0.0.1:10003",
        5: "127.0.0.1:10004",
    }

    role := Role{0, client, peers}
    for roleId, address := range peers {
        role.roleId = roleId
        acceptor := AcceptorRole{role, 5, 0, "foobar"}
        handler := rpc.NewServer()
        err := handler.Register(&acceptor)
        if err != nil {
            fmt.Println("Failed to register Acceptor", roleId, err)
            continue
        }
        ln, err := net.Listen("tcp", address)
        if err != nil {
            fmt.Println("Listening error:", err)
            return
        }
        go acceptor.run(handler, ln)
    }

    role.roleId = 5
    proposer := ProposerRole{role, 0, ""}
    go proposer.run()

    client <- "Hello, world!"

    var input string
    fmt.Scanln(&input)
}

