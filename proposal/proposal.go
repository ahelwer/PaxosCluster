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
    RoleId uint64
    Proposal uint64
}

func CreateManager(roleId uint64) *Manager {
    newManager := Manager {
        roleId: roleId,
        proposalCount: 0,
    }
    newManager.GenerateNextProposalId()
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
        RoleId: this.roleId,
        Proposal: this.proposalCount,
    }
    return this.currentId
}

func Default() Id {
    return Id {
        RoleId: 0,
        Proposal: 0,
    }
}

func Chosen() Id {
    return Id {
        RoleId: math.MaxUint64,
        Proposal: math.MaxUint64,
    }
}

func (this *Id) IsGreaterThan(other Id) bool {
    if this.RoleId == other.RoleId {
        return this.Proposal > other.Proposal
    } else {
        return this.RoleId > other.RoleId
    }
}

