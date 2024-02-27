package hotswap

import (
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type Storage interface {
}

func NewStorage(yt.Client, ypath.Path) Storage {
	return nil
}
