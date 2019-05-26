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

	if user == 0 {
		return errors.New("invalid user id")
	}

	matches, err := s.Users.GetLastUserMatches(uint32(user), count+offset)
	if err != nil {
		return err
	}

	if offset < 0 {
		offset = 0
	}
	if offset > len(matches) {
		offset = len(matches)
	}
	if count < 0 {
		count = 0
	}
	if count+offset >= len(matches) {
		count = len(matches) - offset
	}

	c.Response.Header.Set("Content-Type", "application/json")

	if count == 0 {
		_, err = c.WriteString("[]")
		return err
	}

	selection := matches[offset : offset+count]
	stream := json.BorrowStream(c)
	stream.WriteArrayStart()
	for i := len(selection) - 1; i >= 0; i-- {
		stream.WriteVal(selection[i])
		if i != 0 {
			stream.WriteMore()
		}
	}
	stream.WriteArrayEnd()
	_ = stream.Flush()
	json.ReturnStream(stream)
	return nil
}
