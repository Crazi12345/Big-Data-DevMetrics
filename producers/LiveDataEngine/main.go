package main

import (
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/fatih/structs"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

type Post struct {
	Id                    int    `xml:"Id,attr" avro:"Id"`
	PostTypeId            int    `xml:"PostTypeId,attr" avro:"postTypeId"`
	ParentId              int    `xml:"ParentId,attr" avro:"parentId"`
	AcceptedAnswerId      int    `xml:"AcceptedAnswerId,attr" avro:"acceptedAnswerId"`
	CreationDate          string `xml:"CreationDate,attr" avro:"creationDate"`
	Score                 int    `xml:"Score,attr" avro:"score"`
	ViewCount             int    `xml:"ViewCount,attr" avro:"viewCount"`
	Body                  string `xml:"Body,attr" avro:"body"`
	OwnerUserId           int    `xml:"OwnerUserId,attr" avro:"ownerUserId"`
	OwnerDisplayName      string `xml:"OwnerDisplayName,attr" avro:"ownerDisplayName"`
	LastEditorUserId      int    `xml:"LastEditorUserId,attr" avro:"lastEditorUserId"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" avro:"lastEditorDisplayName"`
	LastEditDate          string `xml:"LastEditDate,attr" avro:"lastEditDate"`
	LastActivityDate      string `xml:"LastActivityDate,attr" avro:"lastActivityDate"`
	Title                 string `xml:"Title,attr" avro:"title"`
	Tags                  string `xml:"Tags,attr" avro:"tags"`
	AnswerCount           int    `xml:"AnswerCount,attr" avro:"answerCount"`
	CommentCount          int    `xml:"CommentCount,attr" avro:"commentCount"`
	FavoriteCount         int    `xml:"FavoriteCount,attr" avro:"favoriteCount"`
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr" avro:"communityOwnedDate"`
	ContentLicense        string `xml:"ContentLicense,attr" avro:"contentLicense"`
}

func check(e error, message string) {
	if e != nil {
		log.Fatal(message, e)
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

func sendKafkaMessage(text string) {

	topic := "INGESTION"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte(`{"Nikolaj": "Wonder Why!"}`)},
		kafka.Message{Value: []byte(`{"Katrine": "C# is better!"}`)},
		kafka.Message{Value: []byte(`{"Oliver": "Not with this library!"}`)},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
func main() {

	file, err := os.OpenFile("/run/media/tired_atlas/Maxtor/test.xml", os.O_CREATE|os.O_RDWR, 0644)
	check(err, "unable to parse schema") // Handle the error properly

	sendKafkaMessage("test")
	questionSchema, err := os.ReadFile("pqSchema.avsc")
	postSchema, err := os.ReadFile("postSchema.avsc")
	ocfFile, err := os.Create("pp.avro")
	cfFile, err := os.Create("pq.avro")
	check(err, "cannot create Avro file")
	defer ocfFile.Close()
	for i := 0; i < 100; i++ {
		line, _ := popLine(file)

		fmt.Println(string(line[:]))
		data := &Post{}
		error := xml.Unmarshal(line, data)
		if nil != error {
			fmt.Println("Error unmarshalling from XML", err)
			return
		}
		postWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W:      ocfFile,
			Schema: string(postSchema),
		})
		questionWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W:      cfFile,
			Schema: string(questionSchema),
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
	}
}
