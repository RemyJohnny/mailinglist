package main

import (
	"database/sql"
	"log"
	"sync"

	"github.com/RemyJohnny/mailinglist/jsonapi"
	"github.com/RemyJohnny/mailinglist/mdb"
	"github.com/alexflint/go-arg"
)

var args struct {
	DbPath   string `arg:"env:MAILINGLIST_DB"`
	BindJson string `arg:"env:MAILINGLIST_BIND_JSON"`
}

func main() {
	arg.MustParse(&args)

	if args.DbPath == "" {
		//args.DbPath = "user=postgres dbname=postgres password=root sslmode=disable"
		args.DbPath = "postgres://postgres:root@localhost:5432/postgres?sslmode=disable"
	}
	if args.BindJson == "" {
		args.BindJson = ":8080"
	}

	log.Printf("using database '%v'\n", args.DbPath)

	db, err := sql.Open("postgres", args.DbPath)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	mdb.TryCreate(db)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log.Printf("Starting JSON API Server...\n")
		jsonapi.Serve(db, args.BindJson)
		wg.Done()
	}()
	wg.Wait()
}
