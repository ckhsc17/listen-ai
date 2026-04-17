package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type nlpSentimentRequest struct {
	Texts []string `json:"texts"`
}

type nlpSentimentResponse struct {
	Classifications []struct {
		Label string `json:"label"`
		Score int    `json:"score"`
	} `json:"classifications"`
}

func classifyTexts(nlpURL string, texts []string) ([]struct {
	Label string
	Score int
}, error) {
	empty := []struct {
		Label string
		Score int
	}{}
	if nlpURL == "" {
		return empty, fmt.Errorf("NLP_URL is not set")
	}
	if len(texts) == 0 {
		return empty, nil
	}

	body, err := json.Marshal(nlpSentimentRequest{Texts: texts})
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 120 * time.Second}
	req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(strings.TrimSpace(nlpURL), "/")+"/sentiment", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nlp returned status %d", resp.StatusCode)
	}

	var out nlpSentimentResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	result := make([]struct {
		Label string
		Score int
	}, len(out.Classifications))
	for i, c := range out.Classifications {
		result[i].Label = c.Label
		result[i].Score = c.Score
	}
	return result, nil
}
