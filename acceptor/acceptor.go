package acceptor

import (
    "fmt"
    "net"
    "net/rpc"
    "github/paxoscluster/role"
)

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    role.Role
    MinProposalId uint64
    AcceptedProposalId uint64
    AcceptedValue string
}

// Amazingly this function does not exist in the standard library
func max(a uint64, b uint64) uint64 {
    if (a > b) {
        return a
    } else {
        return b
    }
}

func (this *AcceptorRole) Prepare(req *role.PromiseReq, reply *role.Promise) error {
    fmt.Println("Acceptor", this.Role.RoleId, "considering promise", req.ProposalId, "vs", this.MinProposalId)
    reply.PromiseAccepted = req.ProposalId > this.MinProposalId
    reply.AcceptedProposalId = this.AcceptedProposalId
    reply.AcceptedValue = this.AcceptedValue
    this.MinProposalId = max(req.ProposalId, this.MinProposalId)
    return nil
}

func (this *AcceptorRole) Accept(proposal *role.Proposal, reply *uint64) error {
    fmt.Println("Acceptor", this.Role.RoleId, "considering proposal", proposal.ProposalId)
    if proposal.ProposalId >= this.MinProposalId {
        this.AcceptedProposalId = proposal.ProposalId
        this.AcceptedValue = proposal.Value
    }
    *reply = this.MinProposalId
    return nil
}

func (this *AcceptorRole) Run(handler *rpc.Server, ln net.Listener) {
    for {
        cxn, err := ln.Accept()
        if err != nil { continue }
        go handler.ServeConn(cxn)
    }
}
