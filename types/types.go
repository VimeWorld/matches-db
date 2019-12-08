package types

import (
	"fmt"
	"time"
)

const SnowflakeEpoch uint64 = 1546300800000

type UserMatch struct {
	Id    uint64 `json:"id"`
	State byte   `json:"state"`
}

func (s *UserMatch) GetDate() time.Time {
	return time.Unix(int64(GetSnowflakeTs(s.Id)/1000), 0)
}

func (s *UserMatch) String() string {
	return fmt.Sprint("UserMatch{id=", s.Id, ", state=", s.State, "}")
}

func GetSnowflakeTs(id uint64) uint64 {
	return (id >> 22) + SnowflakeEpoch
}
