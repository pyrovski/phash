package main

import (
	"flag"
	"fmt"
	"gocv.io/x/gocv"
	cv_contrib "gocv.io/x/gocv/contrib"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

var procs int

type Img struct {
	path string
	img  gocv.Mat
	hash gocv.Mat
}

func logError(v interface{}) {
	fmt.Fprintln(os.Stderr, v)
}

// Get all images from a path into a stream
func GetImages(p string, c chan *Img, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print(p)
	files, err := ioutil.ReadDir(p)
	if err != nil {
		logError(err)
		return
	}
	if len(files) == 0 {
		log.Printf("no files in %q", p)
		return
	}
	for _, f := range files {
		fullPath := path.Join(p, f.Name())
		log.Printf("adding file: %q", fullPath)
		img := &Img{path: fullPath, img: gocv.IMRead(fullPath, gocv.IMReadGrayScale)}
		if img.img.Empty() {
			logError(fmt.Sprintf("empty image: %q", fullPath))
			continue
		}
		c <- img
	}
}

func ProcessImages(c chan *Img, wg *sync.WaitGroup) {
	hasher := cv_contrib.BlockMeanHash{}
	for img := range c {
		log.Printf("processing %q", img.path)
		img.hash = gocv.NewMat()
		hasher.Compute(img.img, &img.hash)
		// block mean hash: 1x32 bytes
		log.Printf("%q hash: %v", img.path, img.hash.ToBytes())
	}
	log.Print("done processing")
	wg.Done()
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		log.Fatalf("must provide one or more path arguments")
	}
	flag.IntVar(&procs, "procs", 1, "# of goroutines for processing hashes")
	flag.Parse()
	c := make(chan *Img)
	wg := &sync.WaitGroup{}
	rg := &sync.WaitGroup{}
	for i := 0; i < procs; i++ {
		rg.Add(1)
		go ProcessImages(c, rg)
	}
	for _, p := range args {
		wg.Add(1)
		go GetImages(p, c, wg)
	}
	wg.Wait()
	close(c)
	rg.Wait()
}
