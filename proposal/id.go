package proposal

import (
    "fmt"
    "strconv"
)

// Proposal ID struct to enable uniqueness
type Id struct {
    RoleId uint64
    Sequence uint64
    Chosen bool
}

// Creates a proposal ID from a role and sequence number
func ConstructProposalId(roleId uint64, sequence uint64) Id { 
    return Id {
        RoleId: roleId,
        Sequence: sequence,
        Chosen: false,
    }
}

// Returns the default proposal ID; will be outranked by every other proposal ID
func Default() Id {
    return Id {
        RoleId: 0,
        Sequence: 0,
        Chosen: false,
    }
}

// Returns a proposal ID flagged as chosen
func Chosen() Id {
    return Id {
        RoleId: 0,
        Sequence: 0,
        Chosen: true,
    }
}

// Checks for chosen flag
func (this *Id) IsChosen() bool {
    return this.Chosen
}

// Comparison operator
func (this *Id) IsGreaterThan(other Id) bool {
    if other.Chosen {
        return false
    } else if this.Chosen {
        return true
    } else {
        return this.Sequence > other.Sequence
    }
}

// Custom string formatter for ID
func (this Id) String() string {
    if this.Chosen {
        return "chosen"
    } else {
        return fmt.Sprintf("%d.%d", this.RoleId, this.Sequence)
    }
}

// Convert to array of strings for storage in CSV file
func SerializeToCSV(proposal Id) []string {
    record := []string {
        strconv.FormatUint(proposal.RoleId, 10),
        strconv.FormatUint(proposal.Sequence, 10),
        strconv.FormatBool(proposal.Chosen),
    }

    return record
}

// Recover proposal ID from an array of strings
func DeserializeFromCSV(record []string) (Id, error) {
    if len(record) != 3 {
        return Default(), fmt.Errorf("Invalid record size for proposal ID")
    }

    roleId, err := strconv.ParseUint(record[0], 10, 64)
    if err != nil { return Default(), err }
    sequence, err := strconv.ParseUint(record[1], 10, 64)
    if err != nil { return Default(), err }
    chosen, err := strconv.ParseBool(record[2])
    if err != nil { return Default(), err }

    proposal := Id {
        RoleId: roleId,
        Sequence: sequence,
        Chosen: chosen,
    }

    return proposal, nil
}
