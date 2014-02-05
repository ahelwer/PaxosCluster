package proposer

import (
    "fmt"
    "net/rpc"
    "time"
    "github/paxoscluster/acceptor"
    "github/paxoscluster/proposal"
    "github/paxoscluster/clusterpeers"
    "github/paxoscluster/replicatedlog"
)

type ProposerRole struct {
    roleId uint64
    log *replicatedlog.Log
    peers *clusterpeers.Cluster
    proposals *proposal.Manager
    client chan ClientRequest
    heartbeat chan uint64
    terminator chan bool
}

// Constructor for ProposerRole
func Construct(roleId uint64, log *replicatedlog.Log, peers *clusterpeers.Cluster) *ProposerRole {
    newProposerRole := ProposerRole {
        roleId: roleId,
        log: log,
        peers: peers,
        proposals: proposal.CreateManager(roleId),    
        client: make(chan ClientRequest),
        heartbeat: make(chan uint64),
        terminator: make(chan bool),
    }
    return &newProposerRole
}

// Starts proposer role state machine
func Run (this *ProposerRole) {
    isLeaderStateChannel := make(chan bool)
    isNotLeaderStateChannel := make(chan bool)
    go this.isNotLeaderState(isLeaderStateChannel, isNotLeaderStateChannel)
    go this.isLeaderState(isNotLeaderStateChannel, isLeaderStateChannel)
}

// Role is not leader; will reject client requests
func (this *ProposerRole) isNotLeaderState(trans chan<- bool, self <-chan bool) {
    electionNotify := make(chan bool)
    startElection := make(chan bool)
    go this.electLeader(startElection, electionNotify)

    for {
        select {
        case <- electionNotify:
            trans <- true
            <- self
            startElection <- true
        case request := <- this.client:
            request.reply <- fmt.Errorf("This role is not the cluster leader.")
        case <- this.terminator:
            return
        }
    }
}

// Elects self leader if not receiving heartbeat signal from role with higher ID
func (this *ProposerRole) electLeader(startElection <-chan bool, electionNotify chan<- bool) {
    for {
        select {
        case <- this.heartbeat:
            continue
        case <- time.After(2*time.Second):
            electionNotify <- true
            <- startElection
        }
    }
}

// Role is leader; will furnish client requests
func (this *ProposerRole) isLeaderState(trans chan<- bool, self <-chan bool) {
    <- self

    for {
        select {
        case <- this.heartbeat:
            trans <- true
            <- self
        case request := <- this.client:
            go func () { request.reply <- this.paxos(request.value) }()
        case <- this.terminator:
            return
        }
    }
}

// Executes single round of Paxos protocol
func (this *ProposerRole) paxos(value string) error {
    chosen := false

    for !chosen {
        index := this.log.GetFirstUnchosenIndex()
        proposalId := this.proposals.GetCurrentProposalId()
        usingValue := value

        // Prepare phase
        request := acceptor.PrepareReq {
            ProposalId: proposalId, 
            Index: index,
        }
        peerCount, endpoint := this.peers.BroadcastPrepareRequest(request)
        success, changed, changedValue, err := this.recvPromises(peerCount, endpoint)
        if err != nil { return err }

        if success {
            if changed {
                usingValue = changedValue
            }

            // Proposal phase
            request := acceptor.ProposalReq {
                ProposalId: proposalId, 
                Index: index, 
                Value: usingValue, 
                FirstUnchosenIndex: this.log.GetFirstUnchosenIndex(),
            }
            peerCount, endpoint := this.peers.BroadcastProposalRequest(request)
            success, err = this.recvAccepts(proposalId, peerCount, endpoint)
            if err != nil { return err }

            if success {
                fmt.Println("Chose ProposalId:", proposalId, "Index:", index, "Value:", usingValue)
                this.log.SetEntryAt(index, usingValue, proposal.Chosen())
                chosen = !changed
            } else {
                this.proposals.GenerateNextProposalId()
            }
        } else {
            this.proposals.GenerateNextProposalId()
        }
    }

    return nil
}

// Receves replies to prepare requests
func (this *ProposerRole) recvPromises(peerCount uint64, endpoint <-chan *rpc.Call) (bool, bool, string, error) {
    success := false
    changed := false
    value := ""
    majority := this.peers.GetPeerCount()/2+1
    replyCount := uint64(0)
    promiseCount := this.peers.GetSkipPromiseCount()
    highestAccepted := proposal.Default()

    for promiseCount < majority && replyCount < peerCount {
        var promise acceptor.PrepareResp
        select {
        case reply := <- endpoint:
            if reply.Error != nil { return success, changed, value, reply.Error }
            promise = *reply.Reply.(*acceptor.PrepareResp)
            replyCount++
        case <- time.After(time.Second):
            return success, changed, value, nil
        }

        if promise.PromiseAccepted {
            promiseCount++

            if promise.AcceptedProposalId.IsGreaterThan(highestAccepted) {
                highestAccepted = promise.AcceptedProposalId
                changed = true
                value = promise.AcceptedValue
            }

            this.peers.SetPromiseRequirement(promise.RoleId, !promise.NoMoreAccepted)
        }
    }

    fmt.Println("Processed", replyCount, "replies with", promiseCount, "promises.")
    success = promiseCount >= majority
    return success, changed, value, nil
}

// Receves replies to proposal
func (this *ProposerRole) recvAccepts(proposalId proposal.Id, peerCount uint64, endpoint <-chan *rpc.Call) (bool, error) {
    majority := peerCount/2+1
    acceptCount := uint64(0)
    for acceptCount < majority {
        var response acceptor.ProposalResp
        select {
            case reply := <- endpoint :
                if reply.Error != nil { return false, reply.Error }
                response = *reply.Reply.(*acceptor.ProposalResp)
            case <- time.After(time.Second):
                return false, nil
        }

        if proposalId.IsGreaterThan(response.AcceptedId) || proposalId == response.AcceptedId {
            acceptCount++
        } else {
            this.peers.SetPromiseRequirement(response.RoleId, true)
            return false, nil
        }
    }

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

// Client request to replicate data
type ClientRequest struct {
    value string
    reply chan error
}

// Receives requests from client
func (this *ProposerRole) Replicate(value *string, retValue *string) error {
    fmt.Println("Role", this.roleId, "received client request:", *value)
    replyChannel := make(chan error)
    request := ClientRequest{*value, replyChannel}
    this.client <- request
    err := <- replyChannel
    *retValue = *value
    return err
}

// Receives termination command
func (this *ProposerRole) Terminate(req *bool, reply *bool) error {
    this.terminator <- *req
    *reply = *req
    return nil
}
