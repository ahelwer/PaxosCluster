package replicatedlog

import (
    "os"
    "io"
    "fmt"
    "sync"
    "strconv"
    "encoding/csv"
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

func ConstructLog(roleId uint64) (*Log, error) {
    newLog := Log {
        values: nil,
        acceptedProposals: nil,
        minProposalId: proposal.Default(),
        firstUnchosenIndex: 0,
    }

    logFile, err := os.Open(fmt.Sprintf("./coldstorage/%d/log.txt", roleId))
    defer logFile.Close()
    if err != nil { return &newLog, err }
    logFileReader := csv.NewReader(logFile)

    err = nil
    for {
        record, err := logFileReader.Read() 
        if err == io.EOF {
            break
        } else if err != nil {
            return &newLog, err
        }

        proposalRole, err := strconv.ParseUint(record[1], 10, 64)
        if err != nil { return &newLog, err }
        sequence, err := strconv.ParseInt(record[2], 10, 64)
        if err != nil { return &newLog, err }

        newLog.values = append(newLog.values, record[0])
        newLog.acceptedProposals = append(newLog.acceptedProposals, proposal.Id{proposalRole, sequence})
    }

    proposalFile, err := os.Open(fmt.Sprintf("./coldstorage/%d/minproposalid.txt", roleId))
    defer proposalFile.Close()
    if err != nil { return &newLog, err }
    proposalFileReader := csv.NewReader(proposalFile)
    record, err := proposalFileReader.Read()
    if err != nil && err != io.EOF { return &newLog, err }
    proposalRole, err := strconv.ParseUint(record[0], 10, 64)
    if err != nil { return &newLog, err }
    sequence, err := strconv.ParseInt(record[1], 10, 64)
    if err != nil { return &newLog, err }
    newLog.minProposalId = proposal.Id{proposalRole, sequence}

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
        if this.acceptedProposals[idx] != proposal.Chosen() {
            this.firstUnchosenIndex = idx
            break
        }
    }

    if  this.firstUnchosenIndex == limit-1 && this.acceptedProposals[limit-1] == proposal.Chosen() {
        this.firstUnchosenIndex = len(this.acceptedProposals)
    }
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
