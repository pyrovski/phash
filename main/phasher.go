package main

import (
	"flag"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pyrovski/phash"
)

var procs int
var dbFile string
var keyFile string
var dbTimeout time.Duration
var query bool
var show bool
var store bool

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		log.Fatalf("must provide one or more path arguments")
	}
	flag.IntVar(&procs, "procs", 1, "# of goroutines for processing hashes")
	flag.StringVar(&dbFile, "db", "", "sqlite3 DB file")
	flag.StringVar(&keyFile, "keyfile", "", "read each directory's key from this filename in the directory")
	flag.DurationVar(&dbTimeout, "dbtimeout", time.Duration(30), "timeout for DB operations")
	flag.BoolVar(&query, "query", false, "query DB for input matches")
	flag.BoolVar(&store, "store", false, "add entries to DB")
	flag.BoolVar(&show, "show", true, "print hashes of input images")
	flag.Parse()
	args = flag.Args()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if bool2int(store)+bool2int(query)+bool2int(show) != 1 {
		log.Fatalf("must provide exactly one of -show, -query, -store")
	}

	if (query || store) && dbFile == "" {
		log.Fatalf("must set --db")
	}

	hasher := phash.PHasher{DBFile: dbFile, DBTimeout: dbTimeout, KeyFile: keyFile, HashProcs: procs}
	if query {
		hasher.LookupHashesInDirs(args)
	}
	if store {
		hasher.StoreHashesFromDirs(args)
	}
	if show {
		hasher.PrintHashesInDirs(args)
	}
}
