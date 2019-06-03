package api

import (
	"strconv"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/json-iterator/go"
	"github.com/qiangxue/fasthttp-routing"
)

func (s *Server) handleGetMatch(c *routing.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return err
	}

	data, err := s.Matches.Get(uint64(id))
	if err != nil {
		return err
	}
	if data == nil {
		return writeBody(c, "match not found", 404)
	}

	c.SetBody(data)
	return nil
}

func (s *Server) handlePostMatch(c *routing.Context) error {
	intId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return err
	}
	id := uint64(intId)

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(id, c.PostBody(), true)
	})
	if err != nil {
		return err
	}

	var winners []uint32
	winnerAny := json.Get(c.PostBody(), "winner")
	if val := winnerAny.Get("team"); val.ValueType() == jsoniter.StringValue {
		team := val.ToString()
		val = json.Get(c.PostBody(), "teams")
		if val.ValueType() == jsoniter.ArrayValue {
			for i := 0; ; i++ {
				elem := val.Get(i)
				if elem.ValueType() == jsoniter.ObjectValue {
					if elem.Get("id").ToString() == team {
						members := elem.Get("members")
						winners = make([]uint32, members.Size())
						for k := 0; k < len(winners); k++ {
							winners[k] = members.Get(k).ToUint32()
						}
					}
				} else {
					break
				}
			}
		}
	} else if val := winnerAny.Get("player"); val.ValueType() == jsoniter.NumberValue {
		winners = []uint32{val.ToUint32()}
	} else if val := winnerAny.Get("players"); val.ValueType() == jsoniter.ArrayValue {
		winners = make([]uint32, val.Size())
		for k := 0; k < len(winners); k++ {
			winners[k] = val.Get(k).ToUint32()
		}
	}

	var users []uint32
	if val := json.Get(c.PostBody(), "players", '*', "id"); val.ValueType() == jsoniter.ArrayValue {
		users = make([]uint32, val.Size())
		for k := 0; k < len(users); k++ {
			users[k] = val.Get(k).ToUint32()
		}
	}

	err = s.Users.Transaction(func(txn *storage.UsersTransaction) error {
		for _, user := range users {
			err := txn.AddMatch(user, id, uint32SliceContains(winners, user))
			if err != nil {
				return err
			}
		}
		return nil
	}, true)
	if err != nil {
		return err
	}

	return writeBody(c, "OK", 200)
}

func uint32SliceContains(arr []uint32, e uint32) bool {
	for _, a := range arr {
		if e == a {
			return true
		}
	}
	return false
}
