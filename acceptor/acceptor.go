package acceptor

import (
    "fmt"
    "github/paxoscluster/proposal"
    "github/paxoscluster/replicatedlog"
)

/*
 * Acceptor Role
 */
type AcceptorRole struct {
    roleId uint64
    log *replicatedlog.Log
}

// Constructor for AcceptorRole
func Construct(roleId uint64, log *replicatedlog.Log) *AcceptorRole {
    this := AcceptorRole{roleId, log}
    return &this
}

// Request sent out by proposer during prepare phase
type PrepareReq struct {
    ProposalId proposal.Id
    Index int
}

// Response sent by acceptors during prepare phase
type PrepareResp struct {
    PromiseAccepted bool
    AcceptedProposalId proposal.Id
    AcceptedValue string
    NoMoreAccepted bool
    RoleId uint64
}

func (this *AcceptorRole) Prepare(req *PrepareReq, reply *PrepareResp) error {
    fmt.Println("Acceptor", this.roleId, "considering promise", req.ProposalId)
    logEntry := this.log.GetEntryAt(req.Index)
    reply.PromiseAccepted = req.ProposalId.IsGreaterThan(this.log.GetMinProposalId())
    reply.AcceptedProposalId = logEntry.AcceptedProposalId
    reply.AcceptedValue = logEntry.Value
    reply.NoMoreAccepted = this.log.NoMoreAcceptedPast(req.Index)
    reply.RoleId = this.roleId
    this.log.UpdateMinProposalId(req.ProposalId)
    return nil
}

// Request sent out by proposer during proposal phase
type ProposalReq struct {
    ProposalId proposal.Id
    Index int
    Value string
    FirstUnchosenIndex int
}

// Response sent by acceptors during proposal phase
type ProposalResp struct {
    AcceptedId proposal.Id
    RoleId uint64
}

func (this *AcceptorRole) Accept(proposal *ProposalReq, reply *ProposalResp) error {
    fmt.Println("Acceptor", this.roleId, "considering proposal", proposal.ProposalId)
    this.log.MarkAsAccepted(proposal.ProposalId, proposal.FirstUnchosenIndex)
    minProposalId := this.log.GetMinProposalId()
    if proposal.ProposalId.IsGreaterThan(minProposalId) || proposal.ProposalId == minProposalId {
        this.log.SetEntryAt(proposal.Index, proposal.Value, proposal.ProposalId)
    }
    reply.AcceptedId = minProposalId
    reply.RoleId = this.roleId
    return nil
}
