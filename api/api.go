package api

import (
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
	server *fasthttp.Server

	Users   *storage.UserStorage
	Matches *storage.MatchesStorage
}

func (s *Server) Bind(bind string) error {
	router := routing.New()
	// старые пути для совместимости
	router.Post("/saveMatch", s.handleAddUserMatch)
	router.Post("/saveMatchFile", s.handleSaveMatchFile)

	router.Post("/user/addMatch", s.handleAddUserMatch)
	router.Post("/user/addMatches", s.handleAddUserMatches)
	router.Get("/user/getMatches", s.handleUserMatches)

	router.Get(`/match/<id:\d+>`, s.handleGetMatch)
	router.Post(`/match/<id:\d+>`, s.handlePostMatch)

	router.Get("/manage/importMatchFiles", s.handleImport)
	router.Get("/manage/backup", s.handleBackup)
	router.Get("/manage/cleanup", s.handleCleanup)

	s.server = &fasthttp.Server{
		Handler:     s.loggingHandler(router.HandleRequest),
		Name:        "matches-db",
		ReadTimeout: 60 * time.Second,
	}
	if strings.HasPrefix(bind, "/") {
		return s.server.ListenAndServeUNIX(bind, 0777)
	} else {
		return s.server.ListenAndServe(bind)
	}
}

func (s *Server) Close() error {
	return s.server.Shutdown()
}

func (s *Server) loggingHandler(handler fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if r := recover(); r != nil {
			log.Printf("panic when handling the request: %s%s", r, debug.Stack())
			ctx.Error("500 Internal Server Error", 500)
		}
		start := time.Now()
		handler(ctx)
		log.Printf("%s %s %s", string(ctx.Method()), string(ctx.Path()), time.Since(start).Round(100*time.Microsecond))
	}
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
