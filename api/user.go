package api

import (
	"github.com/qiangxue/fasthttp-routing"
)

func (s *Server) handleUserMatches(c *routing.Context) error {
	user := parseInt(c.QueryArgs().Peek("user"), 0)
	count := parseInt(c.QueryArgs().Peek("count"), 20)
	offset := parseInt(c.QueryArgs().Peek("offset"), 0)
	if count < 0 {
		return writeBody(c, "invalid count", 400)
	}
	if offset < 0 {
		return writeBody(c, "invalid offset", 400)
	}
	if user <= 0 {
		return writeBody(c, "invalid user id", 400)
	}

	matches, err := s.Users.GetLastUserMatches(uint32(user), count+offset)
	if err != nil {
		return err
	}

	end := len(matches) - offset
	start := end - count
	if start < 0 {
		start = 0
	}

	c.Response.Header.Set("Content-Type", "application/json")
	if end <= 0 {
		_, err = c.WriteString("[]")
		return err
	}

	stream := json.BorrowStream(c)
	stream.WriteArrayStart()
	for i := end - 1; i >= start; i-- {
		stream.WriteVal(matches[i])
		if i != start {
			stream.WriteMore()
		}
	}
	stream.WriteArrayEnd()
	_ = stream.Flush()
	json.ReturnStream(stream)
	return nil
}
