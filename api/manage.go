package api

import (
	"fmt"
	"time"

	"github.com/qiangxue/fasthttp-routing"
)

func (s *Server) handleCleanup(c *routing.Context) error {
	timestamp := parseInt(c.QueryArgs().Peek("deadline"), 0)
	if timestamp == 0 {
		return writeBody(c, "invalid deadline", 400)
	}
	deadline := time.Unix(int64(timestamp), 0)

	deleted, err := s.Users.RemoveOldMatches(deadline)
	if err != nil {
		return err
	}
	deleted2, err := s.Matches.RemoveOldMatches(deadline)
	if err != nil {
		return err
	}
	return writeBody(c, fmt.Sprint("OK userMatches:", deleted, " matches:", deleted2), 200)
}

func (s *Server) handleImport(c *routing.Context) error {
	path := string(c.QueryArgs().Peek("path"))
	if path == "" {
		return writeBody(c, "invalid path", 400)
	}
	err := s.Matches.ImportFromDir(path)
	if err != nil {
		return nil
	}
	return writeBody(c, "OK", 200)
}

func (s *Server) handleBackup(c *routing.Context) error {
	err := s.Matches.Backup()
	if err != nil {
		return fmt.Errorf("matches backup: %v", err)
	}
	err = s.Users.Backup()
	if err != nil {
		return fmt.Errorf("users backup: %v", err)
	}
	return writeBody(c, "OK", 200)
}
