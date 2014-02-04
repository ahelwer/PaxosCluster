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

func (this *Log) FirstEntryNotChosen() int {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for idx, val := range this.acceptedProposals {
        if val != math.MaxUint64 {
            return idx
        }
    }

    return len(this.acceptedProposals)
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
        return entry, nil
    }
}

func (this *Log) SetEntryAt(index int, value string, proposalId uint64) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()
    
    // Extends log as necessary
    if index >= len(this.values) || index >= len(this.acceptedProposals) {
        valuesDiff := index-len(this.values)+1
        proposalsDiff := index-len(this.acceptedProposals)+1
        this.values = append(this.values, make([]string, valuesDiff)...)
        this.acceptedProposals = append(this.acceptedProposals, make([]uint64, proposalsDiff)...)
    } 

    // Checks if value has already been chosen
    if this.acceptedProposals[index] == math.MaxUint64 {
        return fmt.Errorf("[replicatedlog.SetEntryAt] Log value already chosen at index %d", index)
    }

    this.values[index] = value 
    this.acceptedProposals[index] = proposalId
    return nil
}
