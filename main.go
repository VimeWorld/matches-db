package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/VimeWorld/matches-db/api"
	"github.com/VimeWorld/matches-db/storage"
	"github.com/vharitonsky/iniflags"
)

func main() {
	bind := flag.String("bind", "127.0.0.1:8881", "address to bind baas (can be a unix domain socket: /var/run/baas.sock)")
	usersPath := flag.String("users-db", "./db/users", "path to the users database")
	matchesPath := flag.String("matches-db", "./db/matches", "path to the matches database")
	truncate := flag.Bool("truncate", false, "enables badger to truncate corrupted values")

	iniflags.Parse()

	users := &storage.UserStorage{}
	if err := users.Open(*usersPath, *truncate); err != nil {
		log.Fatalf("Could not open users database: %s", err)
		return
	}

	matches := &storage.MatchesStorage{
		CompressThreshold: 512,
	}
	if err := matches.Open(*matchesPath, *truncate); err != nil {
		log.Fatalf("Could not open matches database: %s", err)
		return
	}

	server := api.Server{
		Users:   users,
		Matches: matches,
	}

	go func() {
		log.Printf("Start http server on %s", *bind)
		if err := server.Bind(*bind); err != nil {
			log.Fatalf("Could not start server: %s", err)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stopChan:
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			if err := users.Close(); err != nil {
				log.Printf("Could not close users database: %s", err)
			}
			wg.Done()
		}()
		go func() {
			if err := matches.Close(); err != nil {
				log.Printf("Could not close matches database: %s", err)
			}
			wg.Done()
		}()
		wg.Wait()
	}
}
