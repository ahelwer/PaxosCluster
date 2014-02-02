package replicatedlog

import (
    "fmt"
    "sync"
    "math"
)

type Log struct {
    values []string
    acceptedProposals []uint64
    exclude sync.Mutex
}

type LogEntry struct {
    Index int
    Value string
    AcceptedProposalId uint64
}

func Construct() *Log {
    newLog := Log{values: make([]string, 1), acceptedProposals: make([]uint64, 1)}
    return &newLog
}

func (this *Log) FirstEntryNotChosen() (int, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for idx, val := range this.acceptedProposals {
        if val != math.MaxUint64 {
            return idx, nil
        }
    }

    this.values = append(this.values, "")
    this.acceptedProposals = append(this.acceptedProposals, 0)
    return len(this.acceptedProposals)-1, nil
}

func (this *Log) GetEntryAt(index int) (LogEntry, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    entry := LogEntry{index, "", 0}

    if index < len(this.values) && index < len(this.acceptedProposals) {
        entry.Value = this.values[index]
        entry.AcceptedProposalId = this.acceptedProposals[index]
        return entry, nil
    } else {
        return entry, fmt.Errorf("[replicatedlog.GetEntryAt] Log index out of range: %d", index)
    }
}

func (this *Log) SetEntryAt(index int, value string, proposalId uint64) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()
    
    if index < len(this.values) && index < len(this.acceptedProposals) {
        if this.acceptedProposals[index] != math.MaxUint64 {
            this.values[index] = value 
            this.acceptedProposals[index] = proposalId
            return nil
        } else {
            return fmt.Errorf("[replicatedlog.SetEntryAt] Log value already chosen at index %d", index)
        } 
    } else {
        return fmt.Errorf("[replicatedlog.SetEntryAt] Log index out of range: %d", index)
    }
}
