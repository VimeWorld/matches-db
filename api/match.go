package api

import (
	"encoding/json"
	"strconv"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/VimeWorld/matches-db/types"
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

	var match types.Match
	if err = json.Unmarshal(c.PostBody(), &match); err != nil {
		c.Error(err.Error(), 400)
		return
	}

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(id, c.PostBody(), true)
	})
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}

	var winners []uint32
	if match.Winner.Player != 0 {
		winners = []uint32{match.Winner.Player}
	} else if len(match.Winner.Players) > 0 {
		winners = match.Winner.Players
	} else if match.Winner.Team != "" {
		for _, team := range match.Teams {
			if team.Id == match.Winner.Team {
				winners = team.Members
			}
		}
	}

	users := make([]uint32, len(match.Players))
	for i, player := range match.Players {
		users[i] = player.Id
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
