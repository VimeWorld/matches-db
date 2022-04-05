package api

import (
	"encoding/json"

	"github.com/VimeWorld/matches-db/types"
	"github.com/valyala/fasthttp"
)

func (s *Server) handleUserMatches(c *fasthttp.RequestCtx) {
	user := parseInt(c.QueryArgs().Peek("user"), 0)
	count := parseInt(c.QueryArgs().Peek("count"), 20)
	offset := parseInt(c.QueryArgs().Peek("offset"), 0)
	if count < 0 {
		c.Error("invalid count", 400)
		return
	}
	if offset < 0 {
		c.Error("invalid offset", 400)
		return
	}
	if user <= 0 {
		c.Error("invalid user id", 400)
		return
	}

	matches, err := s.Users.GetLastUserMatches(uint32(user), offset, count)
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}

	jsonMatches(c, matches, true)
}

func (s *Server) handleUserMatchesAfter(c *fasthttp.RequestCtx) {
	user := parseInt(c.QueryArgs().Peek("user"), 0)
	count := parseInt(c.QueryArgs().Peek("count"), 20)
	after := parseUint64(c.QueryArgs().Peek("after"), 0)
	if count < 0 {
		c.Error("invalid count", 400)
		return
	}
	if user <= 0 {
		c.Error("invalid user id", 400)
		return
	}

	matches, err := s.Users.GetUserMatchesAfter(uint32(user), after, count)
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}

	jsonMatches(c, matches, true)
}

func (s *Server) handleUserMatchesBefore(c *fasthttp.RequestCtx) {
	user := parseInt(c.QueryArgs().Peek("user"), 0)
	count := parseInt(c.QueryArgs().Peek("count"), 20)
	before := parseUint64(c.QueryArgs().Peek("before"), 0)
	if count < 0 {
		c.Error("invalid count", 400)
		return
	}
	if before == 0 {
		c.Error("invalid before", 400)
		return
	}
	if user <= 0 {
		c.Error("invalid user id", 400)
		return
	}

	matches, err := s.Users.GetUserMatchesBefore(uint32(user), before, count)
	if err != nil {
		c.Error(err.Error(), 500)
		return
	}

	jsonMatches(c, matches, true)
}

func jsonMatches(c *fasthttp.RequestCtx, matches []*types.UserMatch, reverse bool) {
	c.Response.Header.Set("Content-Type", "application/json")
	if len(matches) == 0 {
		_, _ = c.WriteString("[]")
		return
	}
	if reverse {
		for i, j := 0, len(matches)-1; i < j; i, j = i+1, j-1 {
			matches[i], matches[j] = matches[j], matches[i]
		}
	}

	bytes, _ := json.Marshal(matches)
	_, _ = c.Write(bytes)
}
