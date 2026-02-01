package bark

import (
	"github.com/blinklabs-io/dingo/database"
)

type Bark struct {
	config BarkConfig
}

type BarkConfig struct {
	DB *database.Database
}

func NewBark(config BarkConfig) *Bark {
	return &Bark{
		config: config,
	}
}
