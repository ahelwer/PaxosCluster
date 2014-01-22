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
    fmt.Println(this.Role.roleId, "considering promise", req.ProposalId, this.minProposalId)
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
    fmt.Println(this.Role.roleId, "considering proposal", proposal.ProposalId, ":", proposal.Value)
    if proposal.ProposalId >= this.minProposalId {
        fmt.Println("Accepted proposal", proposal.ProposalId)
        this.acceptedProposalId = proposal.ProposalId
        this.acceptedValue = proposal.Value
    }
    *reply = this.minProposalId
    return nil
}

func (this *AcceptorRole) run() {
    fmt.Println("Registering acceptor", this.Role.roleId, "at", this.Role.peers[this.Role.roleId])
    rpc.Register(this)
    ln, err := net.Listen("tcp", this.Role.peers[this.Role.roleId])
    if err != nil {
        fmt.Println("Listening error:", err)
        return
    }
    for {
        cxn, err := ln.Accept()
        fmt.Println("Accepting connection", this.Role.roleId, cxn)
        if err != nil { continue }
        go rpc.ServeConn(cxn)
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
        fmt.Println("Connected to acceptor", key, "at", val, cxn)
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
        fmt.Println("Sending prepare request to", acceptor)
        acceptor.Go("AcceptorRole.Prepare", req, &promiseReply, endpoint)
    }
    
    // Waits for promises from majority of acceptors
    replyCount := 0
    promiseCount := 0
    var highestAccepted uint64 = 0
    for promiseCount < majority && replyCount < peerCount {
        var promise *Promise
        select {
            case reply := <- endpoint: 
                if reply.Error != nil { return false, reply.Error }
                promise = reply.Reply.(*Promise)
                replyCount++
            case <- time.After(1000000000):
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

    fmt.Println("Promisecount", promiseCount, peerCount, majority, replyCount)
    return promiseCount >= majority, nil
}

// Proposal phase
func (this *ProposerRole) proposalPhase(acceptors map[uint64]*rpc.Client) (bool, error) {
    peerCount := len(this.Role.peers)
    endpoint := make(chan *rpc.Call, peerCount)

    // Sends out proposals
    proposal := &Proposal{this.proposalId, this.value}
    for _, acceptor := range acceptors {
        var proposalReply uint64
        acceptor.Go("AcceptorRole.Accept", proposal, &proposalReply, endpoint)
    }

    // Waits for acceptance from majority of acceptors
    acceptCount := 0
    for acceptCount < (peerCount / 2 + 1) {
        reply := <- endpoint 
        if reply.Error != nil {
            return false, reply.Error
        }
        acceptedId := reply.Reply.(*uint64)

        if *acceptedId <= this.proposalId {
            acceptCount++
        } else {
            return false, nil
        }
    }

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
        // Generates new proposal ID
        this.proposalId += this.Role.roleId

        success, err := this.preparePhase(acceptors)
        if err != nil {
            fmt.Println("Prepare phase error:", err)
            return
        }

        if success {
            notChosen, err = this.proposalPhase(acceptors)
            if err != nil {
                fmt.Println("Proposal phase error:", err)
                return
            }
        }
    }
}

func main() {
    client := make(chan string)
    peers := map[uint64]string {
        1: "127.0.0.1:10000",
        2: "127.0.0.1:10010",
    }

    role := Role{0, client, peers}
    for roleId := range peers {
        role.roleId = roleId
        acceptor := AcceptorRole{role, 2, 0, ""}
        go acceptor.run()
    }

    role.roleId = 2
    proposer := ProposerRole{role, 0, ""}
    go proposer.run()

    client <- "Hello, world!"

    var input string
    fmt.Scanln(&input)
}

