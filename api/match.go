package api

import (
	"strconv"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/qiangxue/fasthttp-routing"
)

func (s *Server) handleSaveMatchFile(c *routing.Context) error {
	id, err := strconv.ParseInt(string(c.QueryArgs().Peek("id")), 10, 64)
	if err != nil {
		return err
	}

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(uint64(id), c.PostBody(), true)
	})
	if err != nil {
		return err
	}

	c.Error("OK", 200)
	return nil
}

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
		c.Error("match not found", 404)
		return nil
	}

	c.SetBody(data)
	return nil
}

func (s *Server) handlePostMatch(c *routing.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return err
	}

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(uint64(id), c.PostBody(), true)
	})
	if err != nil {
		return err
	}

	c.Error("OK", 200)
	return nil
}
