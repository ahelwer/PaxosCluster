package main

import (
    "fmt"
    "net"
    "net/rpc"
)

// Amazingly this function does not exist in the standard library
func max(a uint64, b uint64) uint64 {
    if (a > b) {
        return a
    } else {
        return b
    }
}

/*
 * Acceptor Role
 */
type Acceptor struct {
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

func (this *Acceptor) GetPromise(req *PromiseReq, reply *Promise) error {
    fmt.Println("Considering promise ", req.ProposalId);
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

func (this *Acceptor) Propose(proposal *Proposal, reply *uint64) error {
    if proposal.ProposalId >= this.minProposalId {
        this.acceptedProposalId = proposal.ProposalId
        this.acceptedValue = proposal.Value
    }
    *reply = this.minProposalId
    return nil
}

func acceptorRole() {
    rpc.Register(new(Acceptor))
    ln, err := net.Listen("tcp", ":9999")
    if err != nil {
        fmt.Println(err)
        return
    }
    for {
        c, err := ln.Accept()
        if err != nil {
            continue
        }
        go rpc.ServeConn(c)
    }
}

/*
 * Proposer Role
 */
func proposerRole(proposalId uint64, value string) {
    c, err := rpc.Dial("tcp", "127.0.0.1:9999")
    if err != nil {
        fmt.Println(err)
        return
    }
    req := &PromiseReq{proposalId}
    var reply Promise
    err = c.Call("Acceptor.GetPromise", req, &reply)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println("success");
    }
}

func main() {
    go acceptorRole()
    go proposerRole(1, "Hello, world!")

    var input string
    fmt.Scanln(&input)
}

