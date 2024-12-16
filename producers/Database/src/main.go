package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"reflect"
)

type User struct {
	Hireable      *bool          `json:"hirable"`
	PublicRepos   *int64         `json:"public_repos"`
	IsSuspicious  *bool          `json:"is_suspicious"`
	UpdatedAt     *string        `json:"updated_at"`
	ID            *int64         `json:"id"`
	Blog          *string        `json:"blog"`
	Followers     *int64         `json:"followers"`
	Location      *string        `json:"location"`
	FollowerList  *[]interface{} `json:"follower_list"`
	Type          *string        `json:"type"`
	CommitList    *[]Commit      `json:"commit_list"`
	Bio           *string        `json:"bio"`
	Commits       *int64         `json:"commits"`
	Company       *string        `json:"company"`
	FollowingList *[]interface{} `json:"following_list"`
	PublicGists   *int64         `json:"public_gists"`
	Name          *string        `json:"name"`
	CreatedAt     *string        `json:"created_at"`
	Email         *string        `json:"email"`
	Following     *int64         `json:"following"`
	Login         *string        `json:"login"`
	RepoList      *[]Repo        `json:"repo_list"`
}

type Commit struct {
	RepoID          *int64  `json:"repo_id"`
	RepoOwnerID     *int64  `json:"repo_owner_id"`
	CommitAt        *string `json:"commit_at"`
	CommitterID     *int64  `json:"committer_id"`
	Message         *string `json:"message"`
	RepoDescription *string `json:"repo_description"`
	GenerateAt      *string `json:"generate_at"`
	AuthorID        *int64  `json:"author_id"`
	RepoName        *string `json:"repo_name"`
}

type Repo struct {
	Fork            *bool   `json:"fork"`
	License         *string `json:"license"`
	HasWiki         *bool   `json:"has_wiki"`
	Description     *string `json:"description"`
	Language        *string `json:"language"`
	DefaultBranch   *string `json:"default_branch"`
	CreatedAt       *string `json:"created_at"`
	ForksCount      *int64  `json:"forks_count"`
	UpdatedAt       *string `json:"updated_at"`
	PushedAt        *string `json:"pushed_at"`
	FullName        *string `json:"full_name"`
	OpenIssues      *int64  `json:"open_issues"`
	StargazersCount *int64  `json:"stargazers_count"`
	OwnerID         *int64  `json:"owner_id"`
	ID              *int64  `json:"id"`
	Size            *int64  `json:"size"`
}

var test_commit_file_location = "/run/media/tired_atlas/Maxtor/1M_Commits.csv"
var test_repo_file_location = "/run/media/tired_atlas/Maxtor/1M_Repos.csv"
var test_user_file_location = "/run/media/tired_atlas/Maxtor/1M_Users.csv"

func check(e error, message string) {
	if e != nil {
		log.Fatal(message)
	}
}

func writeHeadersFromStruct(s interface{}, file_location string) []string {

	file, err := os.OpenFile(file_location, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	check(err, "Could not open or create file")
	defer file.Close()
	file.Truncate(0)
	writer := csv.NewWriter(file)
	var headers []string
	t := reflect.TypeOf(s)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		headers = append(headers, field.Name)
	}

	writer.Write(headers)
	defer writer.Flush()
	return headers
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
func subJsonToCSV(text interface{}) (string, error) {
	file, err := os.OpenFile("/home/tired_atlas/Desktop/error.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	check(err, "Filed to open test_commit")

	writer := csv.NewWriter(file)
	defer file.Close()
	defer writer.Flush()
	ids := ""
	switch v := text.(type) {
	case []Commit:
		file, err = os.OpenFile(test_commit_file_location, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		check(err, "Failed to open meme.csv")
		defer file.Close()
		writer = csv.NewWriter(file)
		ids, _ = writeSlice(v, writer, 3)

	case []Repo:
		file, err = os.OpenFile(test_repo_file_location, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		check(err, "Failed to open meme2.csv")
		defer file.Close()
		writer = csv.NewWriter(file)
		ids, _ = writeSlice(v, writer, 13)

	default:
		return "unknown", fmt.Errorf("unsupported type: %T", text)
	}
	return ids, nil
}
func writeSlice(slice interface{}, writer *csv.Writer, idIndex int) (string, error) {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice {
		return "", fmt.Errorf("input is not a slice: %T", slice)
	}
	var result string
	var records []string
	for i := 0; i < v.Len(); i++ {
		element := v.Index(i)
		for j := 0; j < element.NumField(); j++ {
			field := element.Field(j)
			if !field.IsNil() {
				switch field.Elem().Kind() {
				case reflect.Int64:
					records = append(records, fmt.Sprintf("%v", field.Elem().Int()))
					if j == idIndex {

						result = fmt.Sprintf("%s;%v", result, field.Elem().Int()) // Add semicolon

					}
				case reflect.Bool:
					records = append(records, fmt.Sprintf("%v", field.Elem().Bool()))
				case reflect.String:
					records = append(records, fmt.Sprintf("%v", field.Elem().String()))
				default:
					records = append(records, "unknown_type")
				}
			} else {
				records = append(records, "null")
			}
		}
		writer.Write(records)
		records = nil
	}
	defer writer.Flush()

	return result, nil
}
func UserToCSV(user User) {
	file, err := os.OpenFile(test_user_file_location, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	check(err, "Filed to open test_commit")

	defer file.Close()
	writer := csv.NewWriter(file)
	v := reflect.ValueOf(user)

	var record []string
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fi := v.Field(i)

		if !field.IsNil() {
			field = field.Elem()
		}

		switch i {
		case 8, 14: // Handle FollowerList and FollowingList
			if fi.IsNil() {
				record = append(record, "null")
				continue
			}

			var ids string
			for j, item := range field.Interface().([]interface{}) {
				if j > 0 {
					ids += ";"
				}
				ids += fmt.Sprintf("%v", item)
			}
			record = append(record, ids)

		case 10, 21: // Handle CommitList and RepoList
			if fi.IsNil() {
				record = append(record, "null")
				continue
			}

			ids, err := subJsonToCSV(field.Interface())
			check(err, "Error in subJsonToCSV")
			record = append(record, ids)

		default:
			if fi.IsNil() {
				record = append(record, "null")
				continue
			}
			record = append(record, fmt.Sprintf("%v", field.Interface()))
		}
	}

	writer.Write(record)
	defer writer.Flush()
}
func main() {
	var user User
	var repo Repo
	var commit Commit

	writeHeadersFromStruct(user, test_user_file_location)
	writeHeadersFromStruct(repo, test_repo_file_location)
	writeHeadersFromStruct(commit, test_commit_file_location)
	file, err := os.OpenFile("/home/tired_atlas/Desktop/test.json", os.O_RDWR, 0644)
	check(err, "Could not open file")
	defer file.Close()
	//for i := 0; i < 10649574; i++ {
	procent := 0
	for i := 0; i < 100000; i++ {
		if i%1000 == 0 {
			procent++
			fmt.Printf("%d %%\n", procent)
		}
		data, err := popLine(file)
		check(err, "could not read file")
		var user User
		error := json.Unmarshal([]byte(data), &user)
		check(error, "failed to decode json")
		user.Location = new(string) // Allocate memory for the string
		*user.Location = "Danma"
		UserToCSV(user)
	}
}

//
//func readChunk()

//func serialize

//ChunkToSerializeQueue{}
//go serialize(ChunkToSerializeQueue)
//go readChunk(ChunkToSerializeQueue
//   )
