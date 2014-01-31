package acceptor

import (
    "fmt"
)

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    RoleId uint64
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

// Request sent out by proposer during prepare phase
type PrepareReq struct {
    ProposalId uint64
}

// Response sent by acceptors during prepare phase
type PrepareResp struct {
    PromiseAccepted bool
    AcceptedProposalId uint64
    AcceptedValue string
}

func (this *AcceptorRole) Prepare(req *PrepareReq, reply *PrepareResp) error {
    fmt.Println("Acceptor", this.RoleId, "considering promise", req.ProposalId, "vs", this.MinProposalId)
    reply.PromiseAccepted = req.ProposalId > this.MinProposalId
    reply.AcceptedProposalId = this.AcceptedProposalId
    reply.AcceptedValue = this.AcceptedValue
    this.MinProposalId = max(req.ProposalId, this.MinProposalId)
    return nil
}

// Request sent out by proposer during proposal phase
type ProposalReq struct {
    ProposalId uint64
    Value string
}

// Response sent by acceptors during proposal phase
type ProposalResp struct {
    AcceptedId uint64
}

func (this *AcceptorRole) Accept(proposal *ProposalReq, reply *ProposalResp) error {
    fmt.Println("Acceptor", this.RoleId, "considering proposal", proposal.ProposalId)
    if proposal.ProposalId >= this.MinProposalId {
        this.AcceptedProposalId = proposal.ProposalId
        this.AcceptedValue = proposal.Value
    }
    reply.AcceptedId = this.MinProposalId
    return nil
}
