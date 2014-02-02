package acceptor

import (
    "fmt"
)

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    roleId uint64
    minProposalId uint64
    log []string
}

// Constructor for AcceptorRole
func Construct(roleId uint64, log []string) *AcceptorRole {
    this := AcceptorRole{roleId, 0, 0, log}
    return &this
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
    Index uint64
}

// Response sent by acceptors during prepare phase
type PrepareResp struct {
    PromiseAccepted bool
    Index uint64
    AcceptedProposalId uint64
    AcceptedValue string
    NoMoreAccepted bool
}

func (this *AcceptorRole) Prepare(req *PrepareReq, reply *PrepareResp) error {
    fmt.Println("Acceptor", this.roleId, "considering promise", req.ProposalId, "vs", this.minProposalId)
    reply.PromiseAccepted = req.ProposalId > this.minProposalId
    reply.AcceptedProposalId = this.acceptedProposalId
    reply.AcceptedValue = this.acceptedValue
    this.minProposalId = max(req.ProposalId, this.minProposalId)
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
    fmt.Println("Acceptor", this.roleId, "considering proposal", proposal.ProposalId)
    if proposal.ProposalId >= this.minProposalId {
        this.acceptedProposalId = proposal.ProposalId
        this.acceptedValue = proposal.Value
    }
    reply.AcceptedId = this.minProposalId
    return nil
}
