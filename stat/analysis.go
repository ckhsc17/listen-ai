package main

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

var keywordRegexp = regexp.MustCompile(`[a-zA-Z']+|[\p{Han}]+`)

var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "to": true,
	"of": true, "in": true, "on": true, "for": true, "with": true, "is": true,
	"are": true, "it": true, "this": true, "that": true, "my": true, "our": true,
	"your": true, "but": true, "from": true, "at": true, "was": true,
}

func extractKeywordTokens(content string, re *regexp.Regexp) []string {
	matches := re.FindAllString(strings.ToLower(content), -1)
	tokens := make([]string, 0, len(matches)*2)

	for _, m := range matches {
		if isHanOnly(m) {
			tokens = append(tokens, hanBigrams(m)...)
			continue
		}
		tokens = append(tokens, m)
	}

	return tokens
}

func isHanOnly(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if !unicode.Is(unicode.Han, r) {
			return false
		}
	}
	return true
}

func hanBigrams(text string) []string {
	runes := []rune(text)
	if len(runes) < 2 {
		return nil
	}
	if len(runes) == 2 {
		return []string{text}
	}

	bigrams := make([]string, 0, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		bigrams = append(bigrams, string(runes[i:i+2]))
	}
	return bigrams
}

func isTooShortKeyword(token string) bool {
	if isHanOnly(token) {
		return len([]rune(token)) < 2
	}
	return len(token) <= 2
}

// keywordTokenCounts returns per-token counts for persistence (stopwords / length only; no per-request exclude).
func keywordTokenCounts(content string) map[string]int {
	tokens := extractKeywordTokens(content, keywordRegexp)
	freq := make(map[string]int)
	for _, t := range tokens {
		if isTooShortKeyword(t) || stopWords[t] {
			continue
		}
		freq[t]++
	}
	return freq
}

func extractTopKeywords(posts []Post, include, exclude []string, topN int) []KeywordCount {
	excludedMap := map[string]bool{}
	for _, w := range exclude {
		w = strings.ToLower(strings.TrimSpace(w))
		if w != "" {
			excludedMap[w] = true
		}
	}

	freq := map[string]int{}
	for _, post := range posts {
		tokens := extractKeywordTokens(post.Content, keywordRegexp)
		for _, t := range tokens {
			if isTooShortKeyword(t) || stopWords[t] || excludedMap[t] {
				continue
			}
			freq[t]++
		}
	}

	items := make([]KeywordCount, 0, len(freq))
	for k, c := range freq {
		items = append(items, KeywordCount{Keyword: k, Count: c})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Keyword < items[j].Keyword
		}
		return items[i].Count > items[j].Count
	})

	if len(items) > topN {
		items = items[:topN]
	}
	return items
}

func buildTrendsFromPosts(posts []Post) []TrendPoint {
	counts := map[string]int{}
	for _, p := range posts {
		if len(p.CreatedAt) >= 10 {
			counts[p.CreatedAt[:10]]++
		}
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	trends := make([]TrendPoint, 0, len(keys))
	for _, d := range keys {
		trends = append(trends, TrendPoint{Date: d, Count: counts[d]})
	}
	return trends
}

func sentimentPercentages(posts []Post) map[string]float64 {
	counts := map[string]int{"positive": 0, "neutral": 0, "negative": 0}
	for _, p := range posts {
		l := p.Sentiment
		if l == "" {
			l = "neutral"
		}
		if _, ok := counts[l]; !ok {
			l = "neutral"
		}
		counts[l]++
	}
	total := len(posts)
	if total == 0 {
		return map[string]float64{"positive": 0, "neutral": 0, "negative": 0}
	}
	t := float64(total)
	return map[string]float64{
		"positive": round2(100.0 * float64(counts["positive"]) / t),
		"neutral":  round2(100.0 * float64(counts["neutral"]) / t),
		"negative": round2(100.0 * float64(counts["negative"]) / t),
	}
}

func round2(x float64) float64 {
	return float64(int(x*100+0.5)) / 100
}
