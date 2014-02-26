package replicatedlog

import (
    "fmt"
    "sync"
    "github/paxoscluster/proposal"
    "github/paxoscluster/recovery"
)

type Log struct {
    roleId uint64
    values []string
    acceptedProposals []proposal.Id
    minProposalId proposal.Id
    firstUnchosenIndex int
    disk *recovery.Manager
    exclude sync.Mutex
}

type LogEntry struct {
    Index int
    Value string
    AcceptedProposalId proposal.Id
}

// Creates a new replicated log instance, using data from cold storage files
func ConstructLog(roleId uint64, disk *recovery.Manager) (*Log, error) {
    values, acceptedProposals, err := disk.RecoverLog(roleId)
    if err != nil { return nil, err }

    minProposalId, err := disk.RecoverMinProposalId(roleId)
    if err != nil { return nil, err }

    newLog := Log {
        roleId: roleId,
        values: values,
        acceptedProposals: acceptedProposals,
        minProposalId: minProposalId,
        firstUnchosenIndex: 0,
        disk: disk,
    }


    newLog.updateFirstUnchosenIndex()
    return &newLog, nil
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
        err := this.disk.UpdateMinProposalId(this.roleId, this.minProposalId)
        if err != nil {
            fmt.Println("[ LOG", this.roleId, "] Failed to write minProposalId update to disk")
        }
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
    limit := len(this.acceptedProposals)

    for idx := this.firstUnchosenIndex; idx < limit; idx++ {
        if !this.acceptedProposals[idx].IsChosen() {
            this.firstUnchosenIndex = idx
            return
        } else {
            this.emit(idx)
        }
    }

    this.firstUnchosenIndex = len(this.acceptedProposals)
}

// Certifies that no proposals have been accepted past the specified index
func (this *Log) NoMoreAcceptedPast(index int) bool {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    if index+1 >= len(this.acceptedProposals) {
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
func (this *Log) MarkAsChosen(proposalId proposal.Id, upto int) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for idx := this.firstUnchosenIndex; idx < len(this.acceptedProposals) && idx < upto; idx++ {
        if this.acceptedProposals[idx] == proposalId {
            this.acceptedProposals[idx] = proposal.Chosen()
            err := this.disk.UpdateLogRecord(this.roleId, idx, this.values[idx], proposal.Chosen())
            if err != nil {
                fmt.Println("[ LOG", this.roleId, "] Failed to write", proposalId, idx, "choice to disk")
            }
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

    if !this.acceptedProposals[index].IsChosen() &&
        (proposalId.IsGreaterThan(this.minProposalId) ||
        proposalId == this.minProposalId) {
        this.values[index] = value 
        this.acceptedProposals[index] = proposalId
        fmt.Println("[ LOG", this.roleId, "] Values:", this.values)
        fmt.Println("[ LOG", this.roleId, "] Proposals:", this.acceptedProposals)
        err := this.disk.UpdateLogRecord(this.roleId, index, value, proposalId)
        if err != nil {
            fmt.Println("[ LOG", this.roleId, "] Failed to write", proposalId, index, value, "to disk")
        }
    }

    // Updates firstUnchosenIndex if value is being chosen there
    if proposalId.IsChosen() && this.firstUnchosenIndex == index {
        this.updateFirstUnchosenIndex()
    }
}

// Emits chosen value to the registered callback function (currently just print to console)
func (this *Log) emit(index int) {
    fmt.Println("[ LOG", this.roleId, "] Emitting finalized value", this.values[index])
}
