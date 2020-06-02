package api

import (
	"strconv"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

func (s *Server) handleGetMatch(c *fasthttp.RequestCtx) {
	id, err := strconv.ParseInt(c.UserValue("id").(string), 10, 64)
	if err != nil {
		c.Error(err.Error(), 400)
		return
	}

	data, err := s.Matches.Get(uint64(id))
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}
	if data == nil {
		c.Error("match not found", 404)
		return
	}

	c.SetBody(data)
}

func (s *Server) handlePostMatch(c *fasthttp.RequestCtx) {
	intId, err := strconv.ParseInt(c.UserValue("id").(string), 10, 64)
	if err != nil {
		c.Error(err.Error(), 400)
		return
	}
	id := uint64(intId)

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(id, c.PostBody(), true)
	})
	if err != nil {
		c.Error(err.Error(), 500)
		return
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
			var state byte = 0
			if len(winners) == 0 {
				state = 2
			} else {
				for _, a := range winners {
					if user == a {
						state = 1
						break
					}
				}
			}
			err := txn.AddMatch(user, id, state)
			if err != nil {
				return err
			}
		}
		return nil
	}, true)
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}

	c.Error("OK", 200)
}
