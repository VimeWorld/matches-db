package api

import (
	"fmt"

	"github.com/valyala/fasthttp"
)

func (s *Server) handleFlatten(c *fasthttp.RequestCtx) {
	err := s.Matches.DB.Flatten(3)
	if err != nil {
		c.Error(fmt.Sprint("flatten:", err), 500)
		return
	}
	c.Error("OK", 200)
}
