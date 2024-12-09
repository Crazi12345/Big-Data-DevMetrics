package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"github.com/fatih/structs"
	"github.com/linkedin/goavro/v2"
)

type Post struct {
	Id                    int    `xml:"Id,attr" avro:"Id"`
	PostTypeId            int    `xml:"PostTypeId,attr" avro:"post_type_id"`
	ParentId              int    `xml:"ParentId,attr" avro:"parent_id"`
	CreationDate          string `xml:"CreationDate,attr" avro:"creation_date"`
	Score                 int    `xml:"Score,attr" avro:"score"`
	OwnerUserId           int    `xml:"OwnerUserId,attr" avro:"owner_user_id"`
	LastEditorUserId      int    `xml:"LastEditorUserId,attr" avro:"last_editor_user_id"`
	LastEditorDisplayName string `xml:"LastEditorDisplayName,attr" avro:"last_editor_display_name"`
	LastEditDate          string `xml:"LastEditDate,attr" avro:"last_edit_date"`
	LastActivityDate      string `xml:"LastActivityDate,attr" avro:"last_activity_date"`
	CommentCount          int    `xml:"CommentCount,attr" avro:"comment_count"`
	CommunityOwnedDate    string `xml:"CommunityOwnedDate,attr" avro:"community_owned_date"`
	ContentLicense        string `xml:"ContentLicense,attr" avro:"content_license"`
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
	avroSchema, err := os.ReadFile("/home/tired_atlas/Projects/Big-Data-DevMetrics/producers/LiveDataEngine/postSchema.avsc")
	check(err, "unable to parse schema") // Handle the error properly

	reader.ReadLine()
	reader.ReadLine()
	line, _, _ := reader.ReadLine()
	data := &Post{}
	error := xml.Unmarshal(line, data)
	if nil != error {
		fmt.Println("Error unmarshalling from XML", err)
		return
	}
    ocfFile, err := os.Create("pp.avro")
	check(err, "cannot create Avro file")
	defer ocfFile.Close()

	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      ocfFile,
		Schema: string(avroSchema), // Use string(avroSchema) here
	})
	check(err, "cannot create OCF writer")

	native := structs.Map(data)
	check(err, "cannot convert to map")

	// Append the data to the OCF writer
	err = writer.Append([]map[string]interface{}{native})
	check(err, "cannot append data to OCF writer")
	// You can write this to a file, send it over a network, etc.
}
