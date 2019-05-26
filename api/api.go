package api

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/json-iterator/go"
	"github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
)

var json = jsoniter.ConfigFastest

type Server struct {
	Users   *storage.UserStorage
	Matches *storage.MatchesStorage
}

func (s *Server) Bind(bind string) error {
	router := routing.New()
	router.Post("/saveMatch", s.handleSaveMatch)
	router.Post("/saveMatchBatch", s.handleSaveMatchBatch)
	router.Post("/saveMatchFile", s.handleSaveMatchFile)
	router.Get("/userMatches", s.handleUserMatches)
	router.Get("/match", s.handleMatch)
	router.Get("/cleanup", s.handleCleanup)

	server := &fasthttp.Server{
		Handler: s.loggingHandler(router.HandleRequest),
		Name:    "matches-db",
	}
	if strings.HasPrefix(bind, "/") {
		return server.ListenAndServeUNIX(bind, 0777)
	} else {
		return server.ListenAndServe(bind)
	}
}

func (s *Server) loggingHandler(handler fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if r := recover(); r != nil {
			log.Printf("panic when handling the request: %s%s", r, debug.Stack())
			ctx.Error("500 Internal Server Error", 500)
		}
		start := time.Now()
		handler(ctx)
		log.Printf("%s %s", string(ctx.Path()), time.Since(start).Round(100*time.Microsecond))
	}
}

func (s *Server) handleCleanup(c *routing.Context) error {
	timestamp := parseInt(c.QueryArgs().Peek("deadline"), 0)
	if timestamp == 0 {
		return errors.New("invalid deadline")
	}
	deadline := time.Unix(int64(timestamp), 0)

	deleted, err := s.Users.RemoveOldMatches(deadline)

	if err != nil {
		return err
	}
	c.Error(fmt.Sprint("OK", deleted), 200)
	return nil
}

func (s *Server) handleSaveMatch(c *routing.Context) error {
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

func (s *Server) handleSaveMatchBatch(c *routing.Context) error {
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

func (s *Server) handleSaveMatchFile(c *routing.Context) error {
	id, err := strconv.ParseInt(string(c.QueryArgs().Peek("id")), 10, 64)
	if err != nil {
		return err
	}

	err = s.Matches.Transaction(func(txn *storage.MatchesTransaction) error {
		return txn.Put(uint64(id), append([]byte(nil), c.PostBody()...))
	})
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

func (s *Server) handleMatch(c *routing.Context) error {
	id, err := strconv.ParseInt(string(c.QueryArgs().Peek("id")), 10, 64)
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

func parseInt(stringSlice []byte, fallback int) int {
	if len(stringSlice) == 0 {
		return fallback
	}
	num, err := strconv.Atoi(string(stringSlice))
	if err != nil {
		return fallback
	}
	return num
}
