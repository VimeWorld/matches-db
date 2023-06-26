package api

import (
	"fmt"

	"github.com/valyala/fasthttp"
)

func (s *Server) handleBackup(c *fasthttp.RequestCtx) {
	err := s.Matches.Backup()
	if err != nil {
		c.Error(fmt.Sprint("matches backup:", err), 500)
		return
	}
	err = s.Users.Backup()
	if err != nil {
		c.Error(fmt.Sprint("users backup:", err), 500)
		return
	}
	c.Error("OK", 200)
}

func (s *Server) handleFlatten(c *fasthttp.RequestCtx) {
	err := s.Matches.Flatten()
	if err != nil {
		c.Error(fmt.Sprint("matches flatten:", err), 500)
		return
	}
	err = s.Users.Flatten()
	if err != nil {
		c.Error(fmt.Sprint("users flatten:", err), 500)
		return
	}
	c.Error("OK", 200)
}
