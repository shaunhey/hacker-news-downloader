package main

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

func createTables(db *sql.DB) error {
	_, err := db.Exec("create table if not exists news (id integer primary key, data text not null);")
	if err != nil {
		return fmt.Errorf("createTables: failed to execute script: %w", err)
	}

	return nil
}

func downloadItem(db *sql.DB, id int) error {
	res, err := http.Get(fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d.json", id))
	if err != nil {
		return fmt.Errorf("downloadItem: failed to download item %d: %w", id, err)
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("downloadItem: failed to download item %d (http %d)", id, res.StatusCode)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("downloadItem: failed to read response body: %w", err)
	}
	res.Body.Close()

	_, err = db.Exec("insert into news(id, data) values (?,?);", id, string(bytes))
	if err != nil {
		return fmt.Errorf("downloadItem: failed to save item %d: %w", id, err)
	}

	return nil
}

func downloadRange(db *sql.DB, start, end, numDownloaders int) error {
	c := make(chan struct{}, numDownloaders)

	for i := start; i <= end; i++ {
		c <- struct{}{}
		go func() {
			fmt.Printf("\r%d", i)
			err := downloadItem(db, i)
			if err != nil {
				panic(err)
			}
			<-c
		}()
	}

	return nil
}

func getLastDownloadedId(db *sql.DB) (int, error) {
	row := db.QueryRow("select count(*) from news;")
	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("getLastDownloadedId: could not get count from DB: %w", err)
	}

	if count == 0 {
		return 0, nil
	}

	row = db.QueryRow("select max(id) from news;")
	var id int
	err = row.Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("getLastDownloadedId: could not get max(id) from DB: %w", err)
	}
	return id, nil
}

func getLastPostedId() (int, error) {
	res, err := http.Get("https://hacker-news.firebaseio.com/v0/maxitem.json")
	if err != nil {
		return 0, fmt.Errorf("getLastPostedId: failed to obtain last posted ID: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("getLastPostedId: http status code %d", res.StatusCode)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, fmt.Errorf("getLastPostedId: failed to read response body: %w", err)
	}

	id, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, fmt.Errorf("getLastPostedId: failed to parse response body: %w", err)
	}

	return id, nil
}

func main() {
	db, err := sql.Open("sqlite3", "hn.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	err = createTables(db)
	if err != nil {
		panic(err)
	}

	lastPostedId, err := getLastPostedId()
	if err != nil {
		panic(err)
	}

	lastDownloadedId, err := getLastDownloadedId(db)
	if err != nil {
		panic(err)
	}

	fmt.Printf("    Last posted ID: %d\n", lastPostedId)
	fmt.Printf("Last downloaded ID: %d\n", lastDownloadedId)
	fmt.Println("----------------------------------------")
	fmt.Println("Downloading item:")

	numDownloaders := 3 //choose an appropriate number based on your needs
	err = downloadRange(db, lastDownloadedId+1, lastPostedId, numDownloaders)
	if err != nil {
		panic(err)
	}
}
