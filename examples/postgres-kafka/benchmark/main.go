package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	if os.Getenv("BENCHMARK") == "true" {
		parseBenchmarkLog()
		return
	}

	if os.Getenv("DATABASE_URL") == "" {
		log.Fatal("DATABASE_URL is required")
	}

	driver, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer driver.Close(context.Background())

	http.HandleFunc("/create-user", func(w http.ResponseWriter, r *http.Request) {
		type User struct {
			Name string `json:"name"`
		}
		var user User
		if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		_, err = driver.Exec(context.Background(), "INSERT INTO users (name) VALUES ($1)", user.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("User %s created", user.Name)
		w.WriteHeader(http.StatusCreated)
	})

	log.Print("Server started at :8082")
	if err = http.ListenAndServe(":8082", nil); err != nil {
		panic(err)
	}
}

func parseBenchmarkLog() {
	type DataRow struct {
		CreatedAt time.Time `json:"created_at"`
	}

	file, err := os.Open("benchmark.log")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var totalDiff int64
	var count int64

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Split line into CreateTime, operation, and JSON part
		parts := strings.Split(line, "\t")
		if len(parts) < 3 {
			continue // Skip malformed lines
		}

		// Parse the CreateTime (first part), it is in milliseconds (Unix timestamp)
		createTimeStr := strings.Split(parts[0], ":")[1]
		createTimeMs, err := strconv.ParseInt(createTimeStr, 10, 64)
		if err != nil {
			log.Fatal("Error parsing CreateTime:", err)
		}

		// Parse the created_at field from the JSON (third part)
		row := DataRow{}
		err = json.Unmarshal([]byte(parts[2]), &row)
		if err != nil {
			log.Fatal("Error parsing JSON:", err)
		}

		createdAtMs := row.CreatedAt.UnixNano() / int64(time.Millisecond)
		diff := createTimeMs - createdAtMs
		totalDiff += diff
		count++
	}

	// Handle errors during reading
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Calculate the average difference
	if count > 0 {
		avgDiff := totalDiff / count
		fmt.Printf("Average latency: %vms\n", avgDiff)
	} else {
		fmt.Println("No data processed.")
	}
}
