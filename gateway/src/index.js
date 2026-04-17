const express = require("express");
const cors = require("cors");
const jwt = require("jsonwebtoken");
const axios = require("axios");
const dotenv = require("dotenv");

dotenv.config();

const app = express();
const port = process.env.GATEWAY_PORT || 8000;
const statUrl = process.env.STAT_URL || "http://localhost:8002";
const jwtSecret = process.env.JWT_SECRET || "supersecret";
const demoUser = process.env.DEMO_USER || "admin";
const demoPass = process.env.DEMO_PASS || "admin123";

app.use(cors());
app.use(express.json());

function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization || "";
  const [, token] = authHeader.split(" ");

  if (!token) {
    return res.status(401).json({ error: "Missing bearer token" });
  }

  try {
    const payload = jwt.verify(token, jwtSecret);
    req.user = payload;
    return next();
  } catch (err) {
    return res.status(401).json({ error: "Invalid or expired token" });
  }
}

app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "gateway", port });
});

app.post("/auth/login", (req, res) => {
  const { username, password } = req.body || {};

  if (username !== demoUser || password !== demoPass) {
    return res.status(401).json({ error: "Invalid credentials" });
  }

  const token = jwt.sign({ username }, jwtSecret, { expiresIn: "12h" });
  return res.json({ token });
});

app.post("/api/dashboard", authMiddleware, async (req, res) => {
  const {
    includeKeywords = [],
    excludeKeywords = [],
    fromDate = "",
    toDate = "",
    sampleSize = 5,
  } = req.body || {};

  try {
    const statResp = await axios.post(`${statUrl}/stats`, {
      include_keywords: includeKeywords,
      exclude_keywords: excludeKeywords,
      from_date: fromDate,
      to_date: toDate,
      example_limit: sampleSize,
      post_limit: 500,
    });

    const stats = statResp.data;
    const rawExamples = Array.isArray(stats.example_posts) ? stats.example_posts : [];
    const examples = rawExamples.slice(0, sampleSize).map((p) => ({
      ...p,
      sentiment: p.sentiment || "neutral",
    }));

    return res.json({
      sentimentPercentage: stats.sentiment_percentage || {},
      topKeywords: stats.top_keywords || [],
      trends: stats.trends || [],
      examplePosts: examples,
      mentionCount: stats.mention_count || 0,
      totalAnalyzedPosts: stats.total_analyzed_posts ?? stats.mention_count ?? 0,
    });
  } catch (err) {
    const detail = err.response?.data || err.message;
    return res.status(500).json({
      error: "Failed to build dashboard response",
      detail,
    });
  }
});

app.post("/api/posts", authMiddleware, async (req, res) => {
  const { platform = "", author = "", content = "", createdAt = "" } = req.body || {};

  try {
    const statResp = await axios.post(`${statUrl}/posts`, {
      platform,
      author,
      content,
      created_at: createdAt,
    });
    return res.status(201).json(statResp.data);
  } catch (err) {
    const status = err.response?.status || 500;
    const detail = err.response?.data || err.message;
    return res.status(status).json({
      error: "Failed to insert post",
      detail,
    });
  }
});

app.listen(port, () => {
  console.log(`gateway listening on :${port}`);
});
