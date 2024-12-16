package main

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"os/exec"
	"time"
)

type Post struct {
	Id                    int    `json:"Id" xml:"Id,attr" avro:"Id"`
	PostTypeId            int    `json:"PostTypeId" xml:"PostTypeId,attr" avro:"postTypeId"`
	ParentId              int    `json:"ParentId" xml:"ParentId,attr" avro:"parentId"`
	AcceptedAnswerId      int    `json:"AcceptedAnswerId" xml:"AcceptedAnswerId,attr" avro:"acceptedAnswerId"`
	CreationDate          string `json:"CreationDate" xml:"CreationDate,attr" avro:"creationDate"`
	Score                 int    `json:"Score" xml:"Score,attr" avro:"score"`
	ViewCount             int    `json:"ViewCount" xml:"ViewCount,attr" avro:"viewCount"`
	Body                  string `json:"Body" xml:"Body,attr" avro:"body"`
	OwnerUserId           int    `json:"OwnerUserId" xml:"OwnerUserId,attr" avro:"ownerUserId"`
	OwnerDisplayName      string `json:"OwnerDisplayName" xml:"OwnerDisplayName,attr" avro:"ownerDisplayName"`
	LastEditorUserId      int    `json:"LastEditorUserId" xml:"LastEditorUserId,attr" avro:"lastEditorUserId"`
	LastEditorDisplayName string `json:"LastEditorDisplayName" xml:"LastEditorDisplayName,attr" avro:"lastEditorDisplayName"`
	LastEditDate          string `json:"LastEditDate" xml:"LastEditDate,attr" avro:"lastEditDate"`
	LastActivityDate      string `json:"LastActivityDate" xml:"LastActivityDate,attr" avro:"lastActivityDate"`
	AnswerCount           int    `json:"AnswerCount" xml:"AnswerCount,attr" avro:"answerCount"`
	CommentCount          int    `json:"CommentCount" xml:"CommentCount,attr" avro:"commentCount"`
	FavoriteCount         int    `json:"FavoriteCount" xml:"FavoriteCount,attr" avro:"favoriteCount"`
	CommunityOwnedDate    string `json:"CommunityOwnedDate" xml:"CommunityOwnedDate,attr" avro:"communityOwnedDate"`
	ContentLicense        string `json:"ContentLicense" xml:"ContentLicense,attr" avro:"contentLicense"`
}

type User struct {
	Id             int    `json:"Id"`
	Reputation     int    `json:"Reputation"`
	CreationDate   string `json:"CreationDate"`
	DisplayName    string `json:"DisplayName"`
	LastAccessDate string `json:"LastAccessDate"`
	AboutMe        string `json:"AboutMe"`
	Views          int    `json:"Views"`
	UpVotes        int    `json:"UpVotes"`
	DownVotes      int    `json:"DownVotes"`
	AccountId      int    `json:"AccountId,omitempty"`  // Optional field
	WebsiteUrl     string `json:"WebsiteUrl,omitempty"` // Optional field
	Location       string `json:"Location,omitempty"`   // Optional field
}

type Country struct {
	Country    string `json:"country"`
	Population int    `json:"population"`
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

func sendKafkaMessage(data []byte, topic string) {

	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: data},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
func ingest(ingestSize int, file *os.File, data interface{}) {

	for i := 0; i < ingestSize; i++ {
		line, _ := popLine(file)
		err := xml.Unmarshal(line, data)
		if nil != err {
			fmt.Println("Error unmarshalling from XML", err)
			return
		}
		switch data.(type) {
		case *User:
			dada := &User{}
            localfile,_ :=os.OpenFile("locations.txt",os.O_APPEND, 0644)
            local,err:=popLine(localfile)
			dada.Location = string(local)
			jsonData, err := json.Marshal(dada)
			check(err, "could not make to json file")
			sendKafkaMessage(jsonData, "Users")
		case *Post:
			dada := &Post{}
			jsonData, err := json.Marshal(dada)
			check(err, "could not make to json file")
			if dada.PostTypeId == 1 {
				sendKafkaMessage(jsonData, "Post")
			} else {
				sendKafkaMessage(jsonData, "Question")
			}
		}
	}
}
func main() {

	option1, err := os.OpenFile("test.xml", os.O_CREATE|os.O_RDWR, 0644)
	//	option2, err := os.OpenFile("testUsers.xml", os.O_CREATE|os.O_RDWR, 0644)
	check(err, "Could not open file")
	//	data := &User{}
	//	ingest(1000, option2, data, countries, weights)
	data := &Post{}
	ingest(1000, option1, data)
}
