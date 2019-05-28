package api

import (
	"errors"
	"github.com/VimeWorld/matches-db/storage"
	"github.com/qiangxue/fasthttp-routing"
)

func (s *Server) handleAddUserMatch(c *routing.Context) error {
	req := RequestSave{}
	err := json.Unmarshal(c.PostBody(), &req)
	if err != nil {
		return err
	}

	err = s.Users.Transaction(func(txn *storage.UsersTransaction) error {
		for _, user := range req.Users {
			err := txn.AddMatch(user.Id, req.MatchId, user.Win)
			if err != nil {
				return err
			}
		}
		return nil
	}, true)
	if err != nil {
		return err
	}

	c.Error("OK", 200)
	return nil
}

func (s *Server) handleAddUserMatches(c *routing.Context) error {
	req := []RequestSave(nil)
	err := json.Unmarshal(c.PostBody(), &req)
	if err != nil {
		return err
	}
	err = s.Users.Transaction(func(txn *storage.UsersTransaction) error {
		for _, match := range req {
			for _, user := range match.Users {
				err := txn.AddMatch(user.Id, match.MatchId, user.Win)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, true)
	if err != nil {
		return err
	}

	c.Error("OK", 200)
	return nil
}

func (s *Server) handleUserMatches(c *routing.Context) error {
	user := parseInt(c.QueryArgs().Peek("user"), 0)
	count := parseInt(c.QueryArgs().Peek("count"), 20)
	offset := parseInt(c.QueryArgs().Peek("offset"), 0)
	if count < 0 {
		return errors.New("invalid count")
	}
	if offset < 0 {
		return errors.New("invalid offset")
	}
	if user <= 0 {
		return errors.New("invalid user id")
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
