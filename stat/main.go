package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// CurrentNLPVersion must match the lexicon / rules in nlp/app.py when reanalysis is required.
const CurrentNLPVersion = 1

type Post struct {
	ID        int    `json:"id"`
	Platform  string `json:"platform"`
	Author    string `json:"author"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
	Sentiment string `json:"sentiment"`
}

type StatsRequest struct {
	IncludeKeywords []string `json:"include_keywords"`
	ExcludeKeywords []string `json:"exclude_keywords"`
	FromDate        string   `json:"from_date"`
	ToDate          string   `json:"to_date"`
	ExampleLimit    int      `json:"example_limit"`
	PostLimit       int      `json:"post_limit"`
}

type KeywordCount struct {
	Keyword string `json:"keyword"`
	Count   int    `json:"count"`
}

type TrendPoint struct {
	Date  string `json:"date"`
	Count int    `json:"count"`
}

type StatsResponse struct {
	MentionCount          int                 `json:"mention_count"`
	TopKeywords           []KeywordCount      `json:"top_keywords"`
	Trends                []TrendPoint        `json:"trends"`
	ExamplePosts          []Post              `json:"example_posts"`
	Posts                 []Post              `json:"posts,omitempty"`
	SentimentPercentage   map[string]float64  `json:"sentiment_percentage"`
	TotalAnalyzedPosts    int                 `json:"total_analyzed_posts"`
}

type InsertPostRequest struct {
	Platform  string `json:"platform"`
	Author    string `json:"author"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

type InsertPostResponse struct {
	ID int `json:"id"`
}

func parseDateRange(fromDate, toDate string) (string, string, error) {
	layout := "2006-01-02"
	now := time.Now()

	if fromDate == "" {
		fromDate = now.AddDate(0, 0, -30).Format(layout)
	}
	if toDate == "" {
		toDate = now.Format(layout)
	}

	if _, err := time.Parse(layout, fromDate); err != nil {
		return "", "", fmt.Errorf("invalid from_date: %w", err)
	}
	if _, err := time.Parse(layout, toDate); err != nil {
		return "", "", fmt.Errorf("invalid to_date: %w", err)
	}

	return fromDate, toDate, nil
}

func filterNonEmptyKeywords(in []string) []string {
	out := make([]string, 0, len(in))
	for _, w := range in {
		w = strings.TrimSpace(w)
		if w != "" {
			out = append(out, w)
		}
	}
	return out
}

func filterNonEmptyLower(in []string) []string {
	out := make([]string, 0, len(in))
	for _, w := range in {
		w = strings.TrimSpace(strings.ToLower(w))
		if w != "" {
			out = append(out, w)
		}
	}
	return out
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	b := strings.Builder{}
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("?")
	}
	return b.String()
}

func fetchFilteredPosts(db *sql.DB, req StatsRequest) ([]Post, error) {
	fromDate, toDate, err := parseDateRange(req.FromDate, req.ToDate)
	if err != nil {
		return nil, err
	}

	postLimit := req.PostLimit
	if postLimit <= 0 {
		postLimit = 500
	}

	inc := filterNonEmptyKeywords(req.IncludeKeywords)
	exc := filterNonEmptyKeywords(req.ExcludeKeywords)

	var sb strings.Builder
	sb.WriteString(`SELECT id, platform, author, content, created_at, COALESCE(sentiment_label, 'neutral')
		FROM posts
		WHERE date(created_at) BETWEEN date(?) AND date(?) `)
	args := []interface{}{fromDate, toDate}

	if len(inc) > 0 {
		sb.WriteString(` AND (`)
		for i, kw := range inc {
			if i > 0 {
				sb.WriteString(` OR `)
			}
			sb.WriteString(`LOWER(content) LIKE ?`)
			args = append(args, "%"+strings.ToLower(kw)+"%")
		}
		sb.WriteString(`)`)
	}

	for _, kw := range exc {
		sb.WriteString(` AND LOWER(content) NOT LIKE ?`)
		args = append(args, "%"+strings.ToLower(kw)+"%")
	}

	sb.WriteString(` ORDER BY datetime(created_at) DESC LIMIT ?`)
	args = append(args, postLimit)

	rows, err := db.Query(sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	posts := make([]Post, 0, postLimit)
	for rows.Next() {
		var p Post
		if err := rows.Scan(&p.ID, &p.Platform, &p.Author, &p.Content, &p.CreatedAt, &p.Sentiment); err != nil {
			return nil, err
		}
		posts = append(posts, p)
	}
	return posts, rows.Err()
}

func topKeywordsFromDB(db *sql.DB, postIDs []int, excludeKeywords []string, topN int) ([]KeywordCount, error) {
	if len(postIDs) == 0 {
		return nil, nil
	}

	excluded := filterNonEmptyLower(excludeKeywords)
	excludedMap := map[string]bool{}
	for _, e := range excluded {
		excludedMap[e] = true
	}

	args := make([]interface{}, 0, len(postIDs)+len(excluded)+1)
	for _, id := range postIDs {
		args = append(args, id)
	}

	q := `SELECT token, SUM(cnt) AS s FROM post_tokens WHERE post_id IN (` + placeholders(len(postIDs)) + `)`
	if len(excluded) > 0 {
		q += ` AND LOWER(token) NOT IN (` + placeholders(len(excluded)) + `)`
		for _, e := range excluded {
			args = append(args, e)
		}
	}
	q += ` GROUP BY token ORDER BY s DESC LIMIT ?`
	args = append(args, max(50, topN*5))

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []KeywordCount
	for rows.Next() {
		var tok string
		var sum int
		if err := rows.Scan(&tok, &sum); err != nil {
			return nil, err
		}
		if excludedMap[strings.ToLower(tok)] {
			continue
		}
		if isTooShortKeyword(tok) || stopWords[strings.ToLower(tok)] {
			continue
		}
		items = append(items, KeywordCount{Keyword: tok, Count: sum})
		if len(items) >= topN {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func trendsFromDB(db *sql.DB, postIDs []int) ([]TrendPoint, error) {
	if len(postIDs) == 0 {
		return nil, nil
	}
	args := make([]interface{}, 0, len(postIDs))
	for _, id := range postIDs {
		args = append(args, id)
	}
	q := `SELECT substr(created_at, 1, 10) AS d, COUNT(*) AS c FROM posts WHERE id IN (` + placeholders(len(postIDs)) + `) GROUP BY d ORDER BY d`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TrendPoint
	for rows.Next() {
		var d string
		var c int
		if err := rows.Scan(&d, &c); err != nil {
			return nil, err
		}
		out = append(out, TrendPoint{Date: d, Count: c})
	}
	return out, rows.Err()
}

func replacePostTokens(db *sql.DB, postID int, freq map[string]int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM post_tokens WHERE post_id = ?`, postID); err != nil {
		return err
	}
	for tok, cnt := range freq {
		if cnt <= 0 {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO post_tokens (post_id, token, cnt) VALUES (?, ?, ?)`, postID, tok, cnt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func analyzePost(db *sql.DB, nlpURL string, postID int, content string) error {
	freq := keywordTokenCounts(content)
	if err := replacePostTokens(db, postID, freq); err != nil {
		return err
	}

	labels, err := classifyTexts(nlpURL, []string{content})
	label := "neutral"
	score := 0
	if err != nil {
		log.Printf("nlp classify failed for post %d: %v; using defaults", postID, err)
	} else if len(labels) > 0 {
		label = labels[0].Label
		score = labels[0].Score
	}

	_, err = db.Exec(
		`UPDATE posts SET sentiment_label = ?, sentiment_score = ?, nlp_version = ? WHERE id = ?`,
		label, score, CurrentNLPVersion, postID,
	)
	return err
}

func columnExists(db *sql.DB, table, name string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var cname, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &cname, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(cname, name) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func setupDatabase(db *sql.DB) error {
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return err
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		return err
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			platform TEXT NOT NULL,
			author TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TEXT NOT NULL
		)
	`); err != nil {
		return err
	}

	migrations := []struct {
		name string
		sql  string
	}{
		{"sentiment_label", `ALTER TABLE posts ADD COLUMN sentiment_label TEXT DEFAULT 'neutral'`},
		{"sentiment_score", `ALTER TABLE posts ADD COLUMN sentiment_score INTEGER DEFAULT 0`},
		{"nlp_version", `ALTER TABLE posts ADD COLUMN nlp_version INTEGER DEFAULT 0`},
	}
	for _, m := range migrations {
		ok, err := columnExists(db, "posts", m.name)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := db.Exec(m.sql); err != nil {
				return err
			}
		}
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS post_tokens (
			post_id INTEGER NOT NULL,
			token TEXT NOT NULL,
			cnt INTEGER NOT NULL,
			PRIMARY KEY (post_id, token),
			FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
		)
	`); err != nil {
		return err
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_tokens_token ON post_tokens(token)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_post_tokens_post_id ON post_tokens(post_id)`); err != nil {
		return err
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func normalizeCreatedAt(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Now().UTC().Format(time.RFC3339), nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return "", fmt.Errorf("created_at must be RFC3339 format")
	}
	return t.UTC().Format(time.RFC3339), nil
}

func runBackfill(db *sql.DB, nlpURL string) (int, error) {
	type row struct {
		id      int
		content string
	}
	const batchSize = 64
	processed := 0
	lastID := 0

	flush := func(batch []row) error {
		if len(batch) == 0 {
			return nil
		}
		texts := make([]string, len(batch))
		for i, r := range batch {
			texts[i] = r.content
		}
		labels, err := classifyTexts(nlpURL, texts)
		if err != nil {
			log.Printf("backfill batch nlp error: %v", err)
			for _, r := range batch {
				if err := replacePostTokens(db, r.id, keywordTokenCounts(r.content)); err != nil {
					return err
				}
				if _, err := db.Exec(`UPDATE posts SET sentiment_label = ?, sentiment_score = ?, nlp_version = ? WHERE id = ?`,
					"neutral", 0, CurrentNLPVersion, r.id); err != nil {
					return err
				}
			}
			return nil
		}
		for i, r := range batch {
			if err := replacePostTokens(db, r.id, keywordTokenCounts(r.content)); err != nil {
				return err
			}
			label := "neutral"
			score := 0
			if i < len(labels) {
				label = labels[i].Label
				score = labels[i].Score
			}
			if _, err := db.Exec(`UPDATE posts SET sentiment_label = ?, sentiment_score = ?, nlp_version = ? WHERE id = ?`,
				label, score, CurrentNLPVersion, r.id); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		rows, err := db.Query(
			`SELECT id, content FROM posts WHERE COALESCE(nlp_version, 0) < ? AND id > ? ORDER BY id LIMIT ?`,
			CurrentNLPVersion, lastID, batchSize,
		)
		if err != nil {
			return processed, err
		}
		var batch []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.content); err != nil {
				_ = rows.Close()
				return processed, err
			}
			batch = append(batch, r)
			lastID = r.id
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return processed, err
		}
		_ = rows.Close()

		if len(batch) == 0 {
			break
		}
		if err := flush(batch); err != nil {
			return processed, err
		}
		processed += len(batch)
	}

	return processed, nil
}

func main() {
	port := os.Getenv("STAT_PORT")
	if port == "" {
		port = "8002"
	}
	sqlitePath := os.Getenv("SQLITE_PATH")
	if sqlitePath == "" {
		sqlitePath = "./listenai.db"
	}
	nlpURL := strings.TrimSpace(os.Getenv("NLP_URL"))

	log.Printf("using SQLite database at %s", sqlitePath)

	db, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		log.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	if err := setupDatabase(db); err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "stat", "port": port})
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		var req StatsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}

		posts, err := fetchFilteredPosts(db, req)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		exampleLimit := req.ExampleLimit
		if exampleLimit <= 0 {
			exampleLimit = 5
		}

		examplePosts := posts
		if len(examplePosts) > exampleLimit {
			examplePosts = examplePosts[:exampleLimit]
		}

		ids := make([]int, len(posts))
		for i := range posts {
			ids[i] = posts[i].ID
		}

		topN := 10
		topKW, err := topKeywordsFromDB(db, ids, req.ExcludeKeywords, topN)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if len(topKW) == 0 && len(posts) > 0 {
			topKW = extractTopKeywords(posts, req.IncludeKeywords, req.ExcludeKeywords, topN)
		}

		trends, err := trendsFromDB(db, ids)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if len(trends) == 0 && len(posts) > 0 {
			trends = buildTrendsFromPosts(posts)
		}

		resp := StatsResponse{
			MentionCount:        len(posts),
			TopKeywords:         topKW,
			Trends:              trends,
			ExamplePosts:        examplePosts,
			SentimentPercentage: sentimentPercentages(posts),
			TotalAnalyzedPosts:  len(posts),
		}
		writeJSON(w, http.StatusOK, resp)
	})

	http.HandleFunc("/posts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		var req InsertPostRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}

		req.Platform = strings.TrimSpace(req.Platform)
		req.Author = strings.TrimSpace(req.Author)
		req.Content = strings.TrimSpace(req.Content)

		if req.Platform == "" || req.Author == "" || req.Content == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "platform, author, and content are required"})
			return
		}

		createdAt, err := normalizeCreatedAt(req.CreatedAt)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		result, err := db.Exec(
			`INSERT INTO posts (platform, author, content, created_at, sentiment_label, sentiment_score, nlp_version) VALUES (?, ?, ?, ?, 'neutral', 0, 0)`,
			req.Platform,
			req.Author,
			req.Content,
			createdAt,
		)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to insert post"})
			return
		}

		id64, err := result.LastInsertId()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to retrieve inserted id"})
			return
		}
		postID := int(id64)

		if err := analyzePost(db, nlpURL, postID, req.Content); err != nil {
			log.Printf("analyze post %d: %v", postID, err)
		}

		writeJSON(w, http.StatusCreated, InsertPostResponse{ID: postID})
	})

	http.HandleFunc("/admin/backfill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if nlpURL == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "NLP_URL is not configured"})
			return
		}
		n, err := runBackfill(db, nlpURL)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"updated_posts": n, "nlp_version": CurrentNLPVersion})
	})

	addr := ":" + port
	log.Printf("stat service listening on %s (NLP_URL=%q)", addr, nlpURL)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
