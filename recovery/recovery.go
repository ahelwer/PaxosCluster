package recovery

import (
    "os"
    "io"
    "fmt"
    "net"
    "sync"
    "strconv"
    "os/signal"
    "encoding/csv"
    "github/paxoscluster/proposal"
)

type Manager struct {
    sigint chan os.Signal
    exclude sync.Mutex
}

// Creates disk access manager for backup & recovery files
func ConstructManager() (*Manager, error) {
    _, err := os.Stat("coldstorage")
    if err != nil { return nil, err }

    newManager := Manager {
        sigint: make(chan os.Signal, 1),
    }
    signal.Notify(newManager.sigint, os.Interrupt)
    go newManager.finalize()

    return &newManager, nil
}

// Finishes all writes and permanently locks in preparation for shutdown
func (this *Manager) finalize() {
    <- this.sigint
    fmt.Println("[ DISK ] Clearing writes before shutdown")
    this.exclude.Lock()
    os.Exit(0)
}

// Reads the list of peers from a file on disk
func (this *Manager) RetrieveAddresses() (map[uint64]string, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    addresses := make(map[uint64]string)

    // Checks for existence of peers file
    _, err := os.Stat("coldstorage/peers.csv")
    if err != nil { return nil, err }

    // Opens & reads peers file
    peersFile, err := os.Open("coldstorage/peers.csv")
    if err != nil { return nil, err }
    peersFileReader := csv.NewReader(peersFile)
    records, err := peersFileReader.ReadAll()
    peersFile.Close()
    if err != nil { return nil, err }

    // Reads from & parses peers.csv file
    for _, record := range records {
        if len(record) != 3 { return addresses, fmt.Errorf("Invalid record length in peers file") }
        roleId, err := strconv.ParseUint(record[0], 10, 64)
        if err != nil { return nil, err }
        ipAddress := record[1]
        port := record[2]
        addresses[roleId] = net.JoinHostPort(ipAddress, port)
    }

    return addresses, nil
}

func (this *Manager) RecoverMinProposalId(roleId uint64) (proposal.Id, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Checks for existence of directory
    _, err := os.Stat(fmt.Sprintf("coldstorage/%d", roleId))
    if os.IsNotExist(err) {
        return proposal.Default(), nil
    } else if err != nil { return proposal.Default(), err }

    // Checks for existence of file
    proposalFileName := fmt.Sprintf("coldstorage/%d/minproposalid.csv", roleId)
    _, err = os.Stat(proposalFileName) 
    if os.IsNotExist(err) {
        return proposal.Default(), nil
    } else if err != nil { return proposal.Default(), err }

    // File exists, therefore read from it
    proposalFile, err := os.Open(proposalFileName)
    if err != nil { return proposal.Default(), err }
    proposalFileReader := csv.NewReader(proposalFile)
    record, err := proposalFileReader.Read()
    proposalFile.Close()
    if err != nil && err != io.EOF { return proposal.Default(), err }
    proposalRole, err := strconv.ParseUint(record[0], 10, 64)
    if err != nil { return proposal.Default(), err }
    sequence, err := strconv.ParseInt(record[1], 10, 64)
    if err != nil { return proposal.Default(), err }

    return proposal.Id{proposalRole, sequence}, nil
}

func (this *Manager) UpdateMinProposalId(roleId uint64, id proposal.Id) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Ensures existence of directory, creating it if necessary
    _, err := os.Stat(fmt.Sprintf("coldstorage/%d", roleId))
    if os.IsNotExist(err) {
        err = os.Mkdir(fmt.Sprintf("coldstorage/%d", roleId), os.ModeDir)
        if err != nil { return err }
    } else if err != nil { return err }

    // Creates file (automatic overwrite)
    proposalFileName := fmt.Sprintf("coldstorage/%d/minproposalid.csv", roleId)
    proposalFile, err := os.Create(proposalFileName)
    if err != nil { return err }
    defer proposalFile.Close()

    // Writes data to file
    record := []string{strconv.FormatUint(id.RoleId, 10), strconv.FormatInt(id.Sequence, 10)}
    proposalFileWriter := csv.NewWriter(proposalFile)
    err = proposalFileWriter.Write(record)
    if err != nil { return err }
    proposalFileWriter.Flush()
    return nil
}

func (this *Manager) RecoverLog(roleId uint64) ([]string, []proposal.Id, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Checks for existence of log file directory
    _, err := os.Stat(fmt.Sprintf("coldstorage/%d", roleId))
    if os.IsNotExist(err) {
        return nil, nil, nil
    } else if err != nil { return nil, nil, err }

    // Checks for existence of log file
    logFileName := fmt.Sprintf("coldstorage/%d/log.csv", roleId)
    _, err = os.Stat(logFileName) 
    if os.IsNotExist(err) {
        return nil, nil, nil
    } else if err != nil { return nil, nil, err }

    // Log file exists, therefore read from it
    logFile, err := os.Open(logFileName)
    if err != nil { return nil, nil, err }
    logFileReader := csv.NewReader(logFile)
    records, err := logFileReader.ReadAll()
    logFile.Close()
    if err != nil { return nil, nil, err }

    // Parse records
    var values []string = nil
    var proposals []proposal.Id = nil
    for _, record := range records {
        if len(record) != 3 { return nil, nil, fmt.Errorf("Invalid record length in log file %d", roleId) }
        values = append(values, record[0])
        proposalRole, err := strconv.ParseUint(record[1], 10, 64)
        if err != nil { return nil, nil, err }
        sequence, err := strconv.ParseInt(record[2], 10, 64)
        if err != nil { return nil, nil, err }
        proposals = append(proposals, proposal.Id{proposalRole, sequence})
    }

    return values, proposals, nil
}

// Updates a record in the log file
func (this *Manager) UpdateLogRecord(roleId uint64, index int, value string, id proposal.Id) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Ensures existence of directory, creating it if necessary
    _, err := os.Stat(fmt.Sprintf("coldstorage/%d", roleId))
    if os.IsNotExist(err) {
        err = os.Mkdir(fmt.Sprintf("coldstorage/%d", roleId), os.ModeDir)
        if err != nil { return err }
    } else if err != nil { return err }

    // If log file exists, read from it
    logFileName := fmt.Sprintf("coldstorage/%d/log.csv", roleId)
    var records [][]string = nil
    _, err = os.Stat(logFileName)
    if err == nil {
        logFile, err := os.Open(logFileName)
        if err != nil { return err }
        logFileReader := csv.NewReader(logFile)
        records, err = logFileReader.ReadAll()
        logFile.Close()
        if err != nil { return err }
    } else if err != nil && !os.IsNotExist(err) { return err }

    // Modifies record
    blank := []string{"", "0", "0"}
    record := []string{value, strconv.FormatUint(id.RoleId, 10), strconv.FormatInt(id.Sequence, 10)}
    for recordCount := len(records); recordCount <= index; recordCount++ {
        records = append(records, blank)
    }
    records[index] = record

    // Writes out log contents
    modifiedLogFile, err := os.Create(logFileName)
    if err != nil { return err }
    defer modifiedLogFile.Close()
    logFileWriter := csv.NewWriter(modifiedLogFile)
    return logFileWriter.WriteAll(records)
}
