package zakodb

import (
	"fmt"

	"github.com/yokomotod/zakodb/internal/common"
	"github.com/yokomotod/zakodb/internal/zakodb0"
	"github.com/yokomotod/zakodb/internal/zakodb1"
)

type ZakoDB = common.ZakoDB

func NewZakoDB(name string) (ZakoDB, error) {
	switch name {
	case "zakodb0":
		return zakodb0.NewZakoDB(), nil
	case "zakodb1":
		return zakodb1.NewZakoDB(), nil
	default:
		return nil, fmt.Errorf("unknown db: %s", name)
	}
}
