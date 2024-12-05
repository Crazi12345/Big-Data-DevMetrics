package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"os/exec"
)

func check(e error, message string) {
	if e != nil {
		log.Fatal(message)
	}
}

func popLine(f *os.File) ([]byte, error) {
	// Use tail to get the first line from the file
	cmd := exec.Command("tail", "-n", "1", f.Name())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Read the output from tail
	reader := bufio.NewReader(stdout)
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// Truncate the original file by one line
	fi, err := f.Stat()
	check(err, "file statistics failed")

	// Calculate the new size of the file after removing the first line
	newSize := fi.Size() - int64(len(line))
	if newSize < 0 {
		newSize = 0
	}

	err = f.Truncate(newSize)
	check(err, "truncating failed")

	err = f.Sync()
	check(err, "syncing failed")
	return line, nil
}

func main() {

}
