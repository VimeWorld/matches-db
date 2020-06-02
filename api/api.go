package api

import (
	"log"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/VimeWorld/matches-db/storage"
	"github.com/fasthttp/router"
	"github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

var json = jsoniter.ConfigFastest

type Server struct {
	server *fasthttp.Server

	Users   *storage.UserStorage
	Matches *storage.MatchesStorage
}

func (s *Server) Bind(bind string) error {
	r := router.New()
	r.GET("/user/getMatches", s.handleUserMatches)

	r.GET(`/match/{id}`, s.handleGetMatch)
	r.POST(`/match/{id}`, s.handlePostMatch)

	r.GET("/manage/backup", s.handleBackup)
	r.GET("/manage/cleanup", s.handleCleanup)
	r.GET("/manage/flatten", s.handleFlatten)

	s.server = &fasthttp.Server{
		Handler:           s.loggingHandler(r.Handler),
		Name:              "matches-db",
		ReadTimeout:       60 * time.Second,
		ReduceMemoryUsage: true,
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
