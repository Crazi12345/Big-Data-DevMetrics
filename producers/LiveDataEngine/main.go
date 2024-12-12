package main

import (
	"bufio"
	"context"
	"encoding/json"
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
	Title                 string `json:"Title" xml:"Title,attr" avro:"title"`
	Tags                  string `json:"Tags" xml:"Tags,attr" avro:"tags"`
	AnswerCount           int    `json:"AnswerCount" xml:"AnswerCount,attr" avro:"answerCount"`
	CommentCount          int    `json:"CommentCount" xml:"CommentCount,attr" avro:"commentCount"`
	FavoriteCount         int    `json:"FavoriteCount" xml:"FavoriteCount,attr" avro:"favoriteCount"`
	CommunityOwnedDate    string `json:"CommunityOwnedDate" xml:"CommunityOwnedDate,attr" avro:"communityOwnedDate"`
	ContentLicense        string `json:"ContentLicense" xml:"ContentLicense,attr" avro:"contentLicense"`
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
		Title                 string `json:"Title" xml:"Title,attr" avro:"title"`
		Tags                  string `json:"Tags" xml:"Tags,attr" avro:"tags"`
		AnswerCount           int    `json:"AnswerCount" xml:"AnswerCount,attr" avro:"answerCount"`
		CommentCount          int    `json:"CommentCount" xml:"CommentCount,attr" avro:"commentCount"`
		FavoriteCount         int    `json:"FavoriteCount" xml:"FavoriteCount,attr" avro:"favoriteCount"`
		CommunityOwnedDate    string `json:"CommunityOwnedDate" xml:"CommunityOwnedDate,attr" avro:"communityOwnedDate"`
		ContentLicense        string `json:"ContentLicense" xml:"ContentLicense,attr" avro:"contentLicense"`
	}
	err = f.Sync()
	check(err, "syncing failed")
	return line, nil
}

func sendKafkaMessage(data []byte, topic string) {

	partition := 0

	//file, err := os.ReadFile("pp.avro")
	//log.Println(data)
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
	log.Println("Data Send, now is Katrines problem")
}
func main() {

	file, err := os.OpenFile("test.xml", os.O_CREATE|os.O_RDWR, 0644)
	check(err, "Could not open file")
	check(err, "unable to parse schema") // Handle the error properly

	//	sendKafkaMessage("test")
	questionSchema, err := os.ReadFile("pqSchema.avsc")
	postSchema, err := os.ReadFile("postSchema.avsc")
	ocfFile, err := os.Create("pp.avro")
	cfFile, err := os.Create("pq.avro")
	check(err, "cannot create Avro file")
	defer ocfFile.Close()
	//postCodec, err := goavro.NewCodec(string(postSchema))
	check(err, "Could not create Codec")
	//questionCodec, err := goavro.NewCodec(string(questionSchema))
	check(err, "Could not create Codec")
	postWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      ocfFile,
		Schema: string(postSchema), // Use string(avroSchema) here
	})
	questionWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      cfFile,
		Schema: string(questionSchema), // Use string(avroSchema) here
	})

	for i := 0; i < 10; i++ {
		line, _ := popLine(file)
		//		fmt.Println(string(line[:]))
		data := &Post{}
		error := xml.Unmarshal(line, data)
		if nil != error {
			fmt.Println("Error unmarshalling from XML", err)
			return
		}

		jsonData, err := json.Marshal(data)
		check(err, "could not make to jj")
		native := structs.Map(data)
		check(err, "cannot convert to map")

		log.Println(data.Tags)
		// Append the data to the OCF writer
		if data.PostTypeId == 1 {
			err = questionWriter.Append([]map[string]interface{}{native})
			//binary, err := questionCodec.BinaryFromNative(nil, structs.Map(data))
			check(err, "could not convert QuestionData")
			sendKafkaMessage(jsonData, "INGESTION")
		} else {
			err = postWriter.Append([]map[string]interface{}{native})

			//binary, err := postCodec.BinaryFromNative(nil, structs.Map(data))
			check(err, "could not convert PostData")
			sendKafkaMessage(jsonData, "ingestion2")
		}
		check(err, "cannot append data to OCF writer")
		// You can write this to a file, send it over a network, etc.
	}
}

