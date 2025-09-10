package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/reugn/go-streams/duckdb"
	"github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	// Create data source
	source := extension.NewChanSource(generateData())

	// Transform data
	mapFlow := flow.NewMap(transformUser, 1)

	// Setup database
	db, err := sql.Open("duckdb", "test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create table
	db.Exec(`DROP TABLE IF EXISTS users`)
	db.Exec(`CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)`)

	// Create sink
	config := duckdb.SinkConfig{TableName: "users", BatchSize: 4}
	sink := duckdb.NewSink(context.Background(), db, config)

	// Build pipeline
	source.Via(mapFlow).To(sink)
	sink.AwaitCompletion()

	// Display results
	displayResults()
}

func generateData() chan any {
	ch := make(chan any, 5)
	go func() {
		defer close(ch)
		users := []User{
			{ID: 1, Name: "Alice", Email: "alice@example.com"},
			{ID: 2, Name: "Bob", Email: "bob@example.com"},
			{ID: 3, Name: "Charlie", Email: "charlie@example.com"},
			{ID: 4, Name: "David", Email: "david@example.com"},
			{ID: 5, Name: "Eve", Email: "eve@example.com"},
			{ID: 6, Name: "Frank", Email: "frank@example.com"},
			{ID: 7, Name: "Grace", Email: "grace@example.com"},
			{ID: 8, Name: "Henry", Email: "henry@example.com"},
			{ID: 9, Name: "Ivy", Email: "ivy@example.com"},
			{ID: 10, Name: "Jack", Email: "jack@example.com"},
			{ID: 11, Name: "Kate", Email: "kate@example.com"},
			{ID: 12, Name: "Liam", Email: "liam@example.com"},
			{ID: 13, Name: "Mia", Email: "mia@example.com"},
			{ID: 14, Name: "Noah", Email: "noah@example.com"},
			{ID: 15, Name: "Olivia", Email: "olivia@example.com"},
			{ID: 16, Name: "Patrick", Email: "patrick@example.com"},
			{ID: 17, Name: "Quinn", Email: "quinn@example.com"},
			{ID: 18, Name: "Ryan", Email: "ryan@example.com"},
			{ID: 19, Name: "Sarah", Email: "sarah@example.com"},
			{ID: 20, Name: "Thomas", Email: "thomas@example.com"},
		}
		for _, user := range users {
			ch <- user
		}
	}()
	return ch
}

func transformUser(user User) duckdb.Record {
	return duckdb.Record{
		"id":    user.ID,
		"name":  user.Name,
		"email": user.Email,
	}
}

func displayResults() {
	db, err := sql.Open("duckdb", "test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name, email FROM users ORDER BY id")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	fmt.Println("Users in database:")
	for rows.Next() {
		var id int
		var name, email string
		rows.Scan(&id, &name, &email)
		fmt.Printf("ID: %d, Name: %s, Email: %s\n", id, name, email)
	}
}
