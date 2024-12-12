package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"github.com/fatih/structs"
	"github.com/linkedin/goavro/v2"
	"io"
	"log"
	"os"
	"os/exec"
)

/*
type Post struct {
	Id                    int    `xml:"Id,attr"`
	PostTypeId            int    `xml:"PostTypeId,attr"`
	ParentId              int    `xml:"ParentId,attr"`
	CreationDate          string `xml:"CreationDate,attr"`
	Score                 int    `xml:"Score,attr"`
	OwnerUserId           int    `xml:"OwnerUserId,attr"`
	LastEditorUserId      int    `xml:"LastEditorUserId,attr"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr"`
	LastEditDate          string `xml:"LastEditDate,attr"`
	LastActivityDate      string `xml:"LastActivityDate,attr"`
	CommentCount          int    `xml:"CommentCount,attr"`
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr"`
	ContentLicense        string `xml:"ContentLicense,attr"`
}
*/

type Post struct {
	Id                    int    `xml:"Id,attr" avro:"Id"`
	PostTypeId            int    `xml:"PostTypeId,attr" avro:"postTypeId"`
	ParentId              int    `xml:"ParentId,attr" avro:"parentId"`
	AcceptedAnswerId      int    `xml:"AcceptedAnswerId,attr" avro:"acceptedAnswerId"` // New field
	CreationDate          string `xml:"CreationDate,attr" avro:"creationDate"`
	Score                 int    `xml:"Score,attr" avro:"score"`
	ViewCount             int    `xml:"ViewCount,attr" avro:"viewCount"` // New field
	Body                  string `xml:"Body,attr" avro:"body"`
	OwnerUserId           int    `xml:"OwnerUserId,attr" avro:"ownerUserId"`
	OwnerDisplayName      string `xml:"OwnerDisplayName,attr" avro:"ownerDisplayName"` // New field
	LastEditorUserId      int    `xml:"LastEditorUserId,attr" avro:"lastEditorUserId"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" avro:"lastEditorDisplayName"`
	LastEditDate          string `xml:"LastEditDate,attr" avro:"lastEditDate"`
	LastActivityDate      string `xml:"LastActivityDate,attr" avro:"lastActivityDate"`
	Title                 string `xml:"Title,attr" avro:"title"`             // New field
	Tags                  string `xml:"Tags,attr" avro:"tags"`               // New field
	AnswerCount           int    `xml:"AnswerCount,attr" avro:"answerCount"` // New field
	CommentCount          int    `xml:"CommentCount,attr" avro:"commentCount"`
	FavoriteCount         int    `xml:"FavoriteCount,attr" avro:"favoriteCount"` // New field
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr" avro:"communityOwnedDate"`
	ContentLicense        string `xml:"ContentLicense,attr" avro:"contentLicense"`
}

func check(e error, message string) {
	if e != nil {
		log.Fatal(message, e)
	}
}

func saveToFile(filename string, data []byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Then write the Avro data
	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
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

	file, err := os.OpenFile("/run/media/tired_atlas/Maxtor/test.xml", os.O_CREATE, 0644)
	check(err, "Could not open file")
	reader := bufio.NewReader(file)
	check(err, "unable to parse schema") // Handle the error properly

	questionSchema, err := os.ReadFile("/home/tired_atlas/Projects/Big-Data-DevMetrics/producers/LiveDataEngine/pqSchema.avsc")
	postSchema, err := os.ReadFile("/home/tired_atlas/Projects/Big-Data-DevMetrics/producers/LiveDataEngine/postSchema.avsc")

	reader.ReadLine()
	reader.ReadLine()
	reader.ReadLine()
	line, _, _ := reader.ReadLine()
	fmt.Println(string(line[:]))
	data := &Post{}
	error := xml.Unmarshal(line, data)
	if nil != error {
		fmt.Println("Error unmarshalling from XML", err)
		return
	}
	ocfFile, err := os.Create("pp.avro")
	cfFile, err := os.Create("pq.avro")
	check(err, "cannot create Avro file")
	defer ocfFile.Close()

	postWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      ocfFile,
		Schema: string(postSchema), // Use string(avroSchema) here
	})
	questionWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      cfFile,
		Schema: string(questionSchema), // Use string(avroSchema) here
	})

	check(err, "cannot create OCF writer")

	native := structs.Map(data)
	check(err, "cannot convert to map")

	// Append the data to the OCF writer
	if data.PostTypeId == 1 {
		err = questionWriter.Append([]map[string]interface{}{native})
	} else {
		err = postWriter.Append([]map[string]interface{}{native})
	}
	check(err, "cannot append data to OCF writer")
	// You can write this to a file, send it over a network, etc.
}
