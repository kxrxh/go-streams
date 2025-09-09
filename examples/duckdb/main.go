package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/reugn/go-streams/duckdb"
	"github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const DB_PATH = "out.db"

type User struct {
	ID       int       `json:"id"`
	Name     string    `json:"name"`
	Email    string    `json:"email"`
	Created  time.Time `json:"created"`
	IsActive bool      `json:"is_active"`
}

func main() {
	// Enable debug logging to see transformation details
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Create a context for the sink (can be cancelled for graceful shutdown)
	ctx := context.Background()

	// Create a channel source for generating sample data
	source := extension.NewChanSource(generateSampleData())

	// Create a map flow to transform data
	mapFlow := flow.NewMap(transformUser, 1)

	// Open database connection (user manages connection)
	db, err := sql.Open("duckdb", DB_PATH)
	if err != nil {
		fmt.Printf("[ERROR] Failed to open database: %v\n", err)
		return
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		fmt.Printf("[ERROR] Failed to ping database: %v\n", err)
		return
	}

	// Drop and recreate table to reset the schema
	_, err = db.Exec(`DROP TABLE IF EXISTS users`)
	if err != nil {
		fmt.Printf("[ERROR] Failed to drop table: %v\n", err)
		return
	}

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			full_name VARCHAR,
			email VARCHAR,
			email_domain VARCHAR,
			created TIMESTAMP,
			created_formatted VARCHAR,
			is_active BOOLEAN,
			user_category VARCHAR,
			account_age_days INTEGER,
			account_age_cat VARCHAR,
			processed_at TIMESTAMP,
			record_version INTEGER
		)
	`)
	if err != nil {
		fmt.Printf("[ERROR] Failed to create table: %v\n", err)
		return
	}

	// Create DuckDB sink configuration
	config := duckdb.SinkConfig{
		TableName: "users",
		BatchSize: 5, // Batch every 5 records
	}

	// Create DuckDB sink with context for cancellation support
	sink := duckdb.NewSink(ctx, db, config)

	// Build the stream pipeline
	source.
		Via(mapFlow).
		To(sink)

	// Wait for processing to complete
	sink.AwaitCompletion()

	// Query and display the results from the database
	queryAndDisplayResults()
}

func generateSampleData() chan any {
	outChan := make(chan any, 10)

	go func() {
		defer close(outChan)

		// Generate sample user data with some edge cases
		users := []User{
			{ID: 1, Name: "alice johnson", Email: "ALICE@EXAMPLE.COM", Created: time.Now(), IsActive: true},           // Test normalization
			{ID: 2, Name: "Bob Smith", Email: "invalid-email", Created: time.Now(), IsActive: false},                  // Test validation
			{ID: 3, Name: "charlie brown", Email: "charlie.brown@company.co.uk", Created: time.Now(), IsActive: true}, // Test domain extraction
			{ID: 4, Name: "Diana", Email: "diana@example.com", Created: time.Now(), IsActive: true},                   // Test single name
			{ID: 5, Name: "eve wilson", Email: "eve.wilson@gmail.com", Created: time.Now(), IsActive: false},          // Test Gmail domain
			{ID: 6, Name: "Frank Miller", Email: "frank.miller@tech-startup.io", Created: time.Now(), IsActive: true}, // Test .io domain
			{ID: 7, Name: "Grace Lee", Email: "grace.lee@university.edu", Created: time.Now(), IsActive: true},        // Test .edu domain
			{ID: 8, Name: "Henry Davis", Email: "henry.davis@corporate.com", Created: time.Now(), IsActive: false},    // Test .com domain
		}

		// Send users to the channel
		for _, user := range users {
			outChan <- user
			time.Sleep(100 * time.Millisecond) // Simulate data arrival rate
		}
	}()

	return outChan
}

func transformUser(user User) duckdb.Record {
	if user.ID <= 0 {
		slog.Warn("Invalid user ID, skipping", slog.Int("id", user.ID))
		return nil // Return nil to skip this record
	}

	// Normalize email (lowercase and trim spaces)
	email := strings.ToLower(strings.TrimSpace(user.Email))

	// Validate email format
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(email) {
		slog.Warn("Invalid email format detected, replacing with placeholder",
			slog.String("original_email", email),
			slog.String("replacement", "invalid@example.com"),
			slog.Int("user_id", user.ID))
		email = "invalid@example.com"
	}

	// Extract domain from email
	emailParts := strings.Split(email, "@")
	domain := emailParts[1] // Safe because we validated email format

	// Normalize name (title case)
	name := cases.Title(language.Und).String(strings.ToLower(strings.TrimSpace(user.Name)))

	// Create full name (assuming first and last name)
	nameParts := strings.Fields(name)
	var fullName string
	if len(nameParts) >= 2 {
		fullName = strings.Join(nameParts, " ")
	} else {
		fullName = name // Use as-is if only one name
	}

	// Determine user category based on ID
	var userCategory string
	switch {
	case user.ID <= 3:
		userCategory = "premium"
	case user.ID <= 6:
		userCategory = "standard"
	default:
		userCategory = "basic"
	}

	// Format creation date
	formattedDate := user.Created.Format("2006-01-02 15:04:05")

	// Calculate days since creation (as of now)
	daysSinceCreation := int(time.Since(user.Created).Hours() / 24)

	// Determine account age category
	var accountAge string
	switch {
	case daysSinceCreation < 1:
		accountAge = "new"
	case daysSinceCreation < 7:
		accountAge = "recent"
	case daysSinceCreation < 30:
		accountAge = "established"
	default:
		accountAge = "veteran"
	}

	// Create enhanced record with computed fields
	record := duckdb.Record{
		"id":                user.ID,
		"name":              name,
		"full_name":         fullName,
		"email":             email,
		"email_domain":      domain,
		"created":           user.Created,
		"created_formatted": formattedDate,
		"is_active":         user.IsActive,
		"user_category":     userCategory,
		"account_age_days":  daysSinceCreation,
		"account_age_cat":   accountAge,
		"processed_at":      time.Now(),
		"record_version":    1,
	}

	// Log transformation for monitoring
	slog.Debug("Transformed user record",
		slog.Int("id", user.ID),
		slog.String("category", userCategory),
		slog.String("domain", domain))

	return record
}

func queryAndDisplayResults() {
	// Open database connection
	db, err := sql.Open("duckdb", DB_PATH)
	if err != nil {
		fmt.Printf("[ERROR] Failed to open database: %v\n", err)
		return
	}
	defer db.Close()

	// Query all users with enhanced fields
	rows, err := db.Query(`
		SELECT id, name, full_name, email, email_domain, user_category,
			   account_age_cat, is_active, processed_at
		FROM users ORDER BY id`)
	if err != nil {
		fmt.Printf("[ERROR] Failed to query database: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("[ENHANCED DATABASE CONTENTS - After Complex Transformation]")

	count := 0
	for rows.Next() {
		var id int
		var name, fullName, email, domain, category, ageCat string
		var isActive bool
		var processedAt time.Time

		err := rows.Scan(&id, &name, &fullName, &email, &domain, &category, &ageCat, &isActive, &processedAt)
		if err != nil {
			fmt.Printf("[ERROR] Failed to scan row: %v\n", err)
			continue
		}

		activeStr := "No"
		if isActive {
			activeStr = "Yes"
		}

		fmt.Printf("User %d: %s (%s) - %s @ %s [%s/%s] Active: %s Processed: %s\n",
			id, name, fullName, email, domain, category, ageCat, activeStr,
			processedAt.Format("15:04:05"))
		count++
	}

	if err = rows.Err(); err != nil {
		fmt.Printf("[ERROR] Error iterating rows: %v\n", err)
		return
	}

	fmt.Printf("\n[SUMMARY] Total records in database: %d\n", count)
}
