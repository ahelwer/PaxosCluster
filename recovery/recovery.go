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

const RECOVERY_DIR string = "coldstorage"
const RECOVERY_DIR_PATTERN string = RECOVERY_DIR + "/%d"
const RECOVERY_FILE_PATTERN string = RECOVERY_DIR_PATTERN + "/%s"

const PEERS_FILENAME string = "peers.csv"
const CURRENT_PROPOSAL_FILENAME = "currentproposalid.csv"
const MIN_PROPOSAL_FILENAME string = "minproposalid.csv"
const LOG_FILENAME string = "log.csv"

type Manager struct {
    sigint chan os.Signal
    exclude sync.Mutex
}

// Creates disk access manager for backup & recovery files
func ConstructManager() (*Manager, error) {
    _, err := os.Stat(RECOVERY_DIR)
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
    path := RECOVERY_DIR + "/" + PEERS_FILENAME

    // Checks for existence of peers file
    _, err := os.Stat(path)
    if err != nil { return nil, err }

    // Opens & reads peers file
    peersFile, err := os.Open(path)
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

func (this *Manager) RecoverCurrentProposalId(roleId uint64) (proposal.Id, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return recoverProposalId(roleId, CURRENT_PROPOSAL_FILENAME)
}

func (this *Manager) UpdateCurrentProposalId(roleId uint64, id proposal.Id) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return updateProposalId(roleId, id, CURRENT_PROPOSAL_FILENAME)
}

// Recovers the MinProposalId from disk.
func (this *Manager) RecoverMinProposalId(roleId uint64) (proposal.Id, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return recoverProposalId(roleId, MIN_PROPOSAL_FILENAME)
}

func (this *Manager) UpdateMinProposalId(roleId uint64, id proposal.Id) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    return updateProposalId(roleId, id, MIN_PROPOSAL_FILENAME)
}

// Recovers replicated log from disk.
func (this *Manager) RecoverLog(roleId uint64) ([]string, []proposal.Id, error) {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Checks for existence of file
    exists, err := recoveryFileExists(roleId, LOG_FILENAME)
    if err != nil { return nil, nil, err }
    if !exists { return nil, nil, nil }

    // Log file exists, therefore read from it
    logFile, err := openRecoveryFile(roleId, LOG_FILENAME)
    if err != nil { return nil, nil, err }
    logFileReader := csv.NewReader(logFile)
    records, err := logFileReader.ReadAll()
    logFile.Close()
    if err != nil { return nil, nil, err }

    // Parse records
    var values []string = nil
    var proposals []proposal.Id = nil
    for _, record := range records {
        if len(record) < 2 { return nil, nil, fmt.Errorf("Invalid record length in log file %d", roleId) }
        values = append(values, record[0])
        id, err := proposal.DeserializeFromCSV(record[1:])
        if err != nil { return nil, nil, err }
        proposals = append(proposals, id)
    }

    return values, proposals, nil
}

// Updates a record in the replicated log on disk.
func (this *Manager) UpdateLogRecord(roleId uint64, index int, value string, id proposal.Id) error {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    // Ensures existence of directory, creating it if necessary
    err := idempotentCreateRecoveryDir(roleId)
    if err != nil { return err }

    // If log file exists, read from it
    var records [][]string = nil
    exists, err := recoveryFileExists(roleId, LOG_FILENAME)
    if err != nil { return err }
    if exists {
        logFile, err := openRecoveryFile(roleId, LOG_FILENAME)
        if err != nil { return err }
        logFileReader := csv.NewReader(logFile)
        records, err = logFileReader.ReadAll()
        logFile.Close()
        if err != nil { return err }
    }

    // Modifies record
    blank := append([]string{""}, proposal.SerializeToCSV(proposal.Default())...)
    record := append([]string{value}, proposal.SerializeToCSV(id)...)
    for recordCount := len(records); recordCount <= index; recordCount++ {
        records = append(records, blank)
    }
    records[index] = record

    // Writes out log contents
    modifiedLogFile, err := createRecoveryFile(roleId, LOG_FILENAME)
    if err != nil { return err }
    defer modifiedLogFile.Close()
    logFileWriter := csv.NewWriter(modifiedLogFile)
    return logFileWriter.WriteAll(records)
}

// Reads proposal ID from the given file.
func recoverProposalId(roleId uint64, filename string) (proposal.Id, error) {
    // Checks for existence of file
    exists, err := recoveryFileExists(roleId, filename)
    if err != nil { return proposal.Default(), err }
    if !exists { return proposal.Default(), nil }

    // File exists, therefore read from it
    proposalFile, err := openRecoveryFile(roleId, filename)
    if err != nil { return proposal.Default(), err }
    proposalFileReader := csv.NewReader(proposalFile)
    record, err := proposalFileReader.Read()
    proposalFile.Close()
    if err != nil && err != io.EOF { return proposal.Default(), err }
    
    // Deserializes proposal ID
    id, err := proposal.DeserializeFromCSV(record)
    if err != nil { return proposal.Default(), err }

    return id, nil
}

// Updates the proposal ID value in the given file.
func updateProposalId(roleId uint64, id proposal.Id, filename string) error {
    // Overwrites & opens file
    proposalFile, err := createRecoveryFile(roleId, filename)
    if err != nil { return err }

    // Writes data to file
    record := proposal.SerializeToCSV(id)
    proposalFileWriter := csv.NewWriter(proposalFile)
    err = proposalFileWriter.Write(record)
    if err != nil { return err }
    proposalFileWriter.Flush()
    return nil
}

// Creates recovery directory if it does not exist. This function is idempotent.
func idempotentCreateRecoveryDir(roleId uint64) error {
    _, err := os.Stat(fmt.Sprintf(RECOVERY_DIR_PATTERN, roleId))
    
    if os.IsNotExist(err) {
        err = os.Mkdir(fmt.Sprintf(RECOVERY_DIR_PATTERN, roleId), 0700)
    }

    return err
}

// Checks for existence of specified file in this role's recovery directory.
func recoveryFileExists(roleId uint64, filename string) (bool, error) {
    err := idempotentCreateRecoveryDir(roleId)
    if err != nil { return false, err }

    path := fmt.Sprintf(RECOVERY_FILE_PATTERN, roleId, filename)
     _, err = os.Stat(path) 
    if os.IsNotExist(err) {
        return false, nil
    } else if err != nil {
        return false, err
    } else {
        return true, nil
    }
}

// Creates or overwrites the specified file in this role's recovery directory.
func createRecoveryFile(roleId uint64, filename string) (*os.File, error) {
    err := idempotentCreateRecoveryDir(roleId)
    if err != nil { return nil, err }

    path := fmt.Sprintf(RECOVERY_FILE_PATTERN, roleId, filename)
    newFile, err := os.Create(path)
    return newFile, err
}

// Opens the specified file from this role's recovery directory.
func openRecoveryFile(roleId uint64, filename string) (*os.File, error) {
    exists, err := recoveryFileExists(roleId, filename)
    if err != nil { return nil, err }
    if !exists { return nil, os.ErrNotExist }

    path := fmt.Sprintf(RECOVERY_FILE_PATTERN, roleId, filename)
    proposalFile, err := os.Open(path)
    return proposalFile, err
}