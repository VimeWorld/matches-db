package api

import (
	"fmt"
	"time"

	"github.com/valyala/fasthttp"
)

func (s *Server) handleCleanup(c *fasthttp.RequestCtx) {
	timestamp := parseInt(c.QueryArgs().Peek("deadline"), 0)
	if timestamp == 0 {
		c.Error("invalid deadline", 400)
		return
	}
	deadline := time.Unix(int64(timestamp), 0)

	deleted, err := s.Users.RemoveOldMatches(deadline)
	if err != nil {
		c.Error(fmt.Sprint("matches remove:", err), 500)
		return
	}
	deleted2, err := s.Matches.RemoveOldMatches(deadline)
	if err != nil {
		c.Error(fmt.Sprint("users remove:", err), 500)
		return
	}
	c.Error(fmt.Sprint("OK userMatches:", deleted, " matches:", deleted2), 200)
}

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
