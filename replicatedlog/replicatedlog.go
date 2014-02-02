package replicatedlog

import (
    "sync"
    "math"
    "errors"
)

type Log struct {
    values []string
    acceptedProposals []uint64
    exclude sync.Mutex
}

type LogEntry struct {
    Index uint64
    Value string
    AcceptedProposal uint64
}

func Create() *Log {
    values := make([]string, 1)
    acceptedProposals := make([]uint64, 1)
    exclude := sync.Mutex
    newLog := Log {
        values
        acceptedProposals
        locker
    }
    return &newLog
}

func (this *Log) FirstEntryNotChosen() (uint64, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    for idx, val := range this.acceptedProposals {
        if val != math.MaxUint64 {
            return idx, nil
        }
    }

    append(this.values, "")
    append(this.acceptedProposals, 0)
    return len(this.acceptedProposals)-1, nil
}

func (this *Log) GetEntryAt(index uint64) (LogEntry, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    entry := LogEntry{index, "", 0}

    if index < len(this.values) && index < len(this.acceptedProposals) {
        entry.Value = this.values[index]
        entry.AcceptedProposal = this.acceptedProposals[index]
        return entry, nil
    } else {
        return entry, errors.New("Log index out of range: " + index)
    }
}

func (this *Log) SetEntry(index uint64, value string, proposalId uint64) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()
    
    if index < len(this.values) && index < len(this.acceptedProposals) {
        if this.acceptedProposals[index] != math.MaxUint64 {
            this.values[index] = value 
            this.acceptedProposals[index] = proposalId
            return nil
        } else {
            return errors.New("Log value already chosen at index " + index)
        } 
    } else {
        return errors.New("Log index out of range: " + index)
    }
}
