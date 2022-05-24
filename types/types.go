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

type Match struct {
	Version int           `json:"version"`
	Winner  MatchWinner   `json:"winner"`
	Teams   []MatchTeam   `json:"teams"`
	Players []MatchPlayer `json:"players"`
}

type MatchWinner struct {
	Player  uint32   `json:"player"`
	Players []uint32 `json:"players"`
	Team    string   `json:"team"`
	Teams   []string `json:"teams"`
}

type MatchTeam struct {
	Id      string   `json:"id"`
	Members []uint32 `json:"members"`
}

type MatchPlayer struct {
	Id uint32 `json:"id"`
}
