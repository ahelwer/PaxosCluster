package proposal

import "sync"

type Manager struct {
    roleId uint64
    proposalCount int64
    currentId Id
    exclude sync.Mutex
}

func ConstructManager(roleId uint64) *Manager {
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
        Sequence: this.proposalCount,
    }
    return this.currentId
}
