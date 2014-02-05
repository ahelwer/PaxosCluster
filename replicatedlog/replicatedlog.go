package replicatedlog

import (
    "fmt"
    "sync"
    "github/paxoscluster/proposal"
)

type Log struct {
    values []string
    acceptedProposals []proposal.Id
    minProposalId proposal.Id
    firstUnchosenIndex int
    exclude sync.Mutex
}

type LogEntry struct {
    Index int
    Value string
    AcceptedProposalId proposal.Id
}

func Construct() *Log {
    newLog := Log {
        values: make([]string, 1), 
        acceptedProposals: make([]proposal.Id, 1),
        minProposalId: proposal.Default(),
        firstUnchosenIndex: 0,
    }
    return &newLog
}

// Returns the minimum proposal for this log; all lesser proposals should be rejected
func (this *Log) GetMinProposalId() proposal.Id {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return this.minProposalId
}

// Updates minProposalId to the greater of itself and the provided proposalId
func (this *Log) UpdateMinProposalId(proposalId proposal.Id) proposal.Id {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    if proposalId.IsGreaterThan(this.minProposalId) {
        this.minProposalId = proposalId
    }

    return this.minProposalId
}

// Returns the index of the first entry in the log for which no value has been chosen
func (this *Log) GetFirstUnchosenIndex() int {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return this.firstUnchosenIndex
}

// Updates the location of the first unchosen index; exclude MUST be locked before calling
func (this *Log) updateFirstUnchosenIndex() {
    for idx := this.firstUnchosenIndex; idx < len(this.acceptedProposals); idx++ {
        if this.acceptedProposals[idx] != proposal.Chosen() {
            this.firstUnchosenIndex = idx
            break
        }
    }

    if this.firstUnchosenIndex == len(this.acceptedProposals)-1 {
        this.firstUnchosenIndex = len(this.acceptedProposals)
    }
}

// Certifies that no proposals have been accepted past the specified index
func (this *Log) NoMoreAcceptedPast(index int) bool {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    if index+1 >= len(this.acceptedProposals)-1 {
        return true
    }

    for _, proposalId := range this.acceptedProposals[index+1:] {
        if proposalId != proposal.Default() {
            return false
        }
    }

    return true
}

// Marks all log entries with the given proposalId as chosen, up to the given index
func (this *Log) MarkAsAccepted(proposalId proposal.Id, upto int) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for idx := this.firstUnchosenIndex; idx < len(this.acceptedProposals) && idx < upto; idx++ {
        if this.acceptedProposals[idx] == proposalId {
            this.acceptedProposals[idx] = proposal.Chosen()
        }
    }

    this.updateFirstUnchosenIndex()
}

// Returns details of the log entry at the specified index
func (this *Log) GetEntryAt(index int) LogEntry {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    entry := LogEntry {
        Index: index,
        Value: "",
        AcceptedProposalId: proposal.Default(),
    }

    if index < len(this.values) && index < len(this.acceptedProposals) {
        entry.Value = this.values[index]
        entry.AcceptedProposalId = this.acceptedProposals[index]
    } 

    return entry
}

// Sets the value of the log entry at the specified index
func (this *Log) SetEntryAt(index int, value string, proposalId proposal.Id) {
    this.exclude.Lock()
    defer this.exclude.Unlock()
    
    // Extends log as necessary
    if index >= len(this.values) || index >= len(this.acceptedProposals) {
        valuesDiff := index-len(this.values)+1
        proposalsDiff := index-len(this.acceptedProposals)+1
        this.values = append(this.values, make([]string, valuesDiff)...)
        this.acceptedProposals = append(this.acceptedProposals, make([]proposal.Id, proposalsDiff)...)
    } 

    this.values[index] = value 
    this.acceptedProposals[index] = proposalId
    fmt.Println("Values:", this.values, "Proposals:", this.acceptedProposals)

    // Updates firstUnchosenIndex if value is being chosen there
    if proposalId == proposal.Chosen() && this.firstUnchosenIndex == index {
        this.updateFirstUnchosenIndex()
    }
}
