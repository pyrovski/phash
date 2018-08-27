package main

import (
	//	"context"
	"bytes"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"gocv.io/x/gocv"
	cv_contrib "gocv.io/x/gocv/contrib"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// CREATE TABLE key_hashes(fullpath text, mtime text, frame integer, h1 bigint, h2 bigint, h3 bigint, h4 bigint);
const InsertHashes = "INSERT INTO key_hashes(fullpath, frame, h1, h2, h3, h4) values(?,?,?,?,?,?)"

var procs int
var dbFile string

type Img struct {
	path  string
	img   gocv.Mat
	frame int
	hash  gocv.Mat
	// image filename with "-[0-9]+.jpg" removed
	key string
}

// Get all images from a path into a stream
// TODO: make this recursive
func GetImages(p string, c chan *Img, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print(p)
	// TODO: switch to directory walking in parallel ala https://www.oreilly.com/learning/run-strikingly-fast-parallel-file-searches-in-go-with-sync-errgroup
	files, err := ioutil.ReadDir(p)
	if err != nil {
		log.Print(err)
		return
	}
	if len(files) == 0 {
		log.Printf("no files in %q", p)
		return
	}
	re := regexp.MustCompile("(.*)-([0-9]+)[.]jpg")
	for _, f := range files {
		fullPath := path.Join(p, f.Name())
		matches := re.FindStringSubmatch(f.Name())
		// TODO: support video files directly with goav
		if matches == nil {
			log.Printf("skipping file: %q; regex: %v", fullPath, re)
			continue
		}
		frame, err := strconv.Atoi(matches[2])
		if err != nil {
			log.Printf("skipping file: %q; failed to parse frame: %v", fullPath, matches)
			continue
		}
		log.Printf("adding file: %q", fullPath)
		img := &Img{
			path:  fullPath,
			img:   gocv.IMRead(fullPath, gocv.IMReadGrayScale),
			frame: frame,
			key:   path.Join(p, matches[1]),
		}
		if img.img.Empty() {
			log.Print(fmt.Sprintf("empty image: %q", fullPath))
			continue
		}
		c <- img
	}
	log.Print("done reading")
}

func ProcessImages(c chan *Img, wg *sync.WaitGroup, dbC chan *Img) {
	defer wg.Done()
	hasher := cv_contrib.BlockMeanHash{}
	for img := range c {
		// log.Printf("processing %q", img.path)
		img.hash = gocv.NewMat()
		hasher.Compute(img.img, &img.hash)
		img.img = gocv.NewMat()
		// block mean hash: 1x32 bytes
		// log.Printf("%q hash: %v", img.path, img.hash.ToBytes())
		dbC <- img
	}
	log.Print("done processing")
}

func UnpackHash(h []byte) []uint32 {
	result := make([]uint32, 4)
	buf := bytes.NewBuffer(h)
	for i := 0; i < 4; i++ {
		binary.Read(buf, binary.BigEndian, &result[i])
	}
	return result
}

func StoreHashes(dbC chan *Img, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	writeGroup := &sync.WaitGroup{}
	commitFrames := func(imgs []*Img) error {
		defer writeGroup.Done()
		if len(imgs) == 0 {
			return nil
		}
		tx, err := db.Begin()
		defer tx.Rollback()
		if err != nil {
			log.Print(err)
			return err
		}
		stmt, err := tx.Prepare(InsertHashes)
		if err != nil {
			log.Print(err)
			return err
		}
		// TODO: this is super slow on LizardFS
		for _, img := range imgs {
			// TODO: put this inner loop code in a function
			un := UnpackHash(img.hash.ToBytes())
			// log.Printf("adding row: %s, %d, %v, %v, %v, %v", img.key, img.frame, un[0], un[1], un[2], un[3])
			_, err = stmt.Exec(img.key, img.frame, un[0], un[1], un[2], un[3])
			if err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
				log.Print(err)
				return err
			}
		}
		log.Printf("commit")
		err = tx.Commit()
		if err != nil {
			log.Print(err)
			return err
		}
		return nil
	}

	frames := []*Img{}
	cnt := 0
	for img := range dbC {
		frames = append(frames, img)
		cnt++
		if cnt%100 == 0 {
			writeGroup.Add(1)
			go commitFrames(frames)
		}
		// log.Print(img.path, img)
	}
	// log.Print(len(frames))
	go commitFrames(frames)
	writeGroup.Wait()

	log.Print("done storing")
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		log.Fatalf("must provide one or more path arguments")
	}
	flag.IntVar(&procs, "procs", 1, "# of goroutines for processing hashes")
	flag.StringVar(&dbFile, "db", "", "sqlite3 DB file")
	flag.Parse()
	args = flag.Args()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if dbFile == "" {
		log.Fatalf("must set --db")
	}
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			stacklen := runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
		}
	}()

	c := make(chan *Img)
	dbC := make(chan *Img)
	pg := &sync.WaitGroup{}
	rg := &sync.WaitGroup{}
	dg := &sync.WaitGroup{}
	for i := 0; i < procs; i++ {
		pg.Add(1)
		go ProcessImages(c, pg, dbC)
	}
	for _, p := range args {
		rg.Add(1)
		go GetImages(p, c, rg)
	}
	dg.Add(1)
	go StoreHashes(dbC, db, dg)
	rg.Wait()
	close(c)
	pg.Wait()
	close(dbC)
	dg.Wait()
}
