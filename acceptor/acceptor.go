package acceptor

import (
    "fmt"
    "github/paxoscluster/replicatedlog"
)

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    roleId uint64
    minProposalId uint64
    log *replicatedlog.Log
}

// Constructor for AcceptorRole
func Construct(roleId uint64, log *replicatedlog.Log) *AcceptorRole {
    this := AcceptorRole{roleId, 0, log}
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
    Index int
}

// Response sent by acceptors during prepare phase
type PrepareResp struct {
    PromiseAccepted bool
    Index int
    AcceptedProposalId uint64
    AcceptedValue string
    NoMoreAccepted bool
    RoleId uint64
}

func (this *AcceptorRole) Prepare(req *PrepareReq, reply *PrepareResp) error {
    fmt.Println("Acceptor", this.roleId, "considering promise", req.ProposalId, "vs", this.minProposalId)
    logEntry := this.log.GetEntryAt(req.Index)
    reply.PromiseAccepted = req.ProposalId > this.minProposalId
    reply.AcceptedProposalId = logEntry.AcceptedProposalId
    reply.AcceptedValue = logEntry.Value
    reply.NoMoreAccepted = this.log.NoMoreAcceptedPast(req.Index)
    reply.RoleId = this.roleId
    this.minProposalId = max(req.ProposalId, logEntry.AcceptedProposalId)
    return nil
}

// Request sent out by proposer during proposal phase
type ProposalReq struct {
    ProposalId uint64
    Index int
    Value string
}

// Response sent by acceptors during proposal phase
type ProposalResp struct {
    AcceptedId uint64
    RoleId uint64
}

func (this *AcceptorRole) Accept(proposal *ProposalReq, reply *ProposalResp) error {
    fmt.Println("Acceptor", this.roleId, "considering proposal", proposal.ProposalId)
    if proposal.ProposalId >= this.minProposalId {
        this.log.SetEntryAt(proposal.Index, proposal.Value, proposal.ProposalId)
    }
    reply.AcceptedId = this.minProposalId
    reply.RoleId = this.roleId
    return nil
}
