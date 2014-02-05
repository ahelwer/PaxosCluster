package proposal

type Id struct {
    RoleId uint64
    Sequence int64
}

func Default() Id {
    return Id {
        RoleId: 0,
        Sequence: 0,
    }
}

func Chosen() Id {
    return Id {
        RoleId: 0,
        Sequence: -1,
    }
}

func (this *Id) IsGreaterThan(other Id) bool {
    if this.RoleId == other.RoleId {
        return  this.Sequence == -1 || (this.Sequence > other.Sequence && other.Sequence != -1)
    } else {
        return this.RoleId > other.RoleId
    }
}
