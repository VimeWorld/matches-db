package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VimeWorld/matches-db/api"
	"github.com/VimeWorld/matches-db/storage"
	"github.com/vharitonsky/iniflags"
)

func main() {
	bind := flag.String("bind", "127.0.0.1:8881", "address to bind baas (can be a unix domain socket: /var/run/matches-db.sock)")
	dir := flag.String("dir", "./db", "path to the database")
	ttl := flag.Duration("ttl", 6*30*24*time.Hour, "matches ttl")

	iniflags.Parse()

	db, err := storage.OpenDatabase(*dir)
	if err != nil {
		log.Printf("Could not open users database: %s", err)
		return
	}
	defer func() { _ = db.Close() }()

	users := &storage.UserStorage{
		DB:  db,
		TTL: *ttl,
	}
	users.Init()

	matches := &storage.MatchesStorage{
		DB:  db,
		TTL: *ttl + 10*24*time.Hour,
	}

	server := api.Server{
		Users:   users,
		Matches: matches,
	}

	go func() {
		log.Printf("Start http server on %s", *bind)
		if err := server.Bind(*bind); err != nil {
			log.Printf("Could not start server: %s", err)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stopChan:
		log.Printf("Shutting down the server")
		if err := server.Close(); err != nil {
			log.Printf("Stop server error: %s", err)
		}
		log.Printf("Close databases")
		if err := db.Close(); err != nil {
			log.Printf("Could not close database: %s", err)
		}
	}
}
