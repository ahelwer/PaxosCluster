package proposal

import (
    "math"
    "sync"
)

type Manager struct {
    roleId uint64
    proposalCount uint64
    currentId Id
    exclude sync.Mutex
}

type Id struct {
    roleId uint64
    proposal uint64
}

func CreateManager(roleId uint64) *Manager {
    newManager := Manager {
        roleId: roleId,
        proposalCount: 0,
    }
    return &newManager
}

func (this *Manager) GetCurrentProposalId() Id {
    this.exclude.Lock()
    defer this.exclude.Unlock()
    
    return this.currentId
}

func (this *Manager) GenerateNextProposalId() Id {
    this.exclude.Lock()
    defer this.exclude.Unlock()

    this.proposalCount++
    this.currentId = Id {
        roleId: this.roleId,
        proposal: this.proposalCount,
    }
    return this.currentId
}

func Default() Id {
    return Id {
        roleId: 0,
        proposal: 0,
    }
}

func Chosen() Id {
    return Id {
        roleId: math.MaxUint64,
        proposal: math.MaxUint64,
    }
}

func (this *Id) IsGreaterThan(other Id) bool {
    if this.roleId == other.roleId {
        return this.proposal > other.proposal
    } else {
        return this.roleId > other.roleId
    }
}

