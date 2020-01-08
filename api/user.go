package api

import (
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

	c.Response.Header.Set("Content-Type", "application/json")
	if len(matches) == 0 {
		_, _ = c.WriteString("[]")
		return
	}

	stream := json.BorrowStream(c)
	stream.WriteArrayStart()
	for i := len(matches) - 1; i >= 0; i-- {
		stream.WriteVal(matches[i])
		if i != 0 {
			stream.WriteMore()
		}
	}
	stream.WriteArrayEnd()
	_ = stream.Flush()
	json.ReturnStream(stream)
}
