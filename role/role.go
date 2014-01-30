package role

type Role struct {
    RoleId uint64
    Client chan string
    Peers map[uint64]string
}

type PromiseReq struct {
    ProposalId uint64
}

type Promise struct {
    PromiseAccepted bool
    AcceptedProposalId uint64
    AcceptedValue string
}

type Proposal struct {
    ProposalId uint64
    Value string
}
