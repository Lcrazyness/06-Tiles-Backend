const express = require("express");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const JWT_SECRET = process.env.JWT_SECRET;

if (!DATABASE_URL) {
    console.error("DATABASE_URL is not configured.");
    process.exit(1);
}

if (!JWT_SECRET) {
    console.error("JWT_SECRET is not configured.");
    process.exit(1);
}

const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});

app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: "4mb" }));

async function initializeDatabase() {
    await pool.query("CREATE EXTENSION IF NOT EXISTS pgcrypto");

    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            username VARCHAR(20) NOT NULL UNIQUE,
            email VARCHAR(254) NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            profile_icon TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            games_played INTEGER NOT NULL DEFAULT 0,
            games_completed INTEGER NOT NULL DEFAULT 0,
            total_score BIGINT NOT NULL DEFAULT 0,
            best_score BIGINT NOT NULL DEFAULT 0,
            total_notes_hit BIGINT NOT NULL DEFAULT 0,
            battle_wins INTEGER NOT NULL DEFAULT 0,
            battle_losses INTEGER NOT NULL DEFAULT 0
        )
    `);

    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_icon TEXT");

    await pool.query(`
        CREATE TABLE IF NOT EXISTS levels (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(80) NOT NULL,
            author_id UUID REFERENCES users(id) ON DELETE SET NULL,
            author VARCHAR(20) NOT NULL,
            icon TEXT,
            level_data JSONB NOT NULL DEFAULT '[]'::jsonb,
            effects JSONB NOT NULL DEFAULT '[]'::jsonb,
            lives INTEGER NOT NULL DEFAULT 3,
            fps INTEGER NOT NULL DEFAULT 60,
            audio_offset INTEGER NOT NULL DEFAULT 0,
            disable_holds BOOLEAN NOT NULL DEFAULT FALSE,
            difficulty VARCHAR(20) NOT NULL DEFAULT 'Normal',
            plays INTEGER NOT NULL DEFAULT 0,
            rating_total INTEGER NOT NULL DEFAULT 0,
            rating_count INTEGER NOT NULL DEFAULT 0,
            featured BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);

    await pool.query("CREATE INDEX IF NOT EXISTS levels_created_idx ON levels (created_at DESC)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_plays_idx ON levels (plays DESC)");
    await pool.query("ALTER TABLE levels ADD COLUMN IF NOT EXISTS featured BOOLEAN NOT NULL DEFAULT FALSE");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_author_idx ON levels (LOWER(author))");
}

function createToken(user) {
    return jwt.sign({ userId: user.id, username: user.username }, JWT_SECRET, { expiresIn: "30d" });
}

function publicUser(user, includeEmail = true) {
    return {
        id: user.id,
        username: user.username,
        email: includeEmail ? user.email : undefined,
        createdAt: user.created_at,
        profileIcon: user.profile_icon || null,
        statistics: {
            gamesPlayed: user.games_played,
            gamesCompleted: user.games_completed,
            totalScore: Number(user.total_score),
            bestScore: Number(user.best_score),
            totalNotesHit: Number(user.total_notes_hit),
            battleWins: user.battle_wins,
            battleLosses: user.battle_losses
        }
    };
}

function publicLevel(row) {
    return {
        id: row.id,
        name: row.name,
        author: row.author,
        authorId: row.author_id,
        icon: row.icon || null,
        data: row.level_data || [],
        effects: row.effects || [],
        lives: row.lives,
        fps: row.fps,
        audioOffset: row.audio_offset,
        disableHolds: row.disable_holds,
        difficulty: row.difficulty,
        plays: row.plays,
        ratings: row.rating_count ? [row.rating_total / row.rating_count] : [],
        ratingAverage: row.rating_count ? row.rating_total / row.rating_count : 0,
        ratingCount: row.rating_count,
        featured: !!row.featured,
        createdAt: row.created_at
    };
}

function validEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function validUsername(username) {
    return /^[A-Za-z0-9_]{3,20}$/.test(username);
}

function authenticate(req, res, next) {
    const authorization = req.headers.authorization || "";
    const [scheme, token] = authorization.split(" ");
    if (scheme !== "Bearer" || !token) {
        return res.status(401).json({ success: false, message: "Authentication required" });
    }
    try {
        req.auth = jwt.verify(token, JWT_SECRET);
        next();
    } catch {
        return res.status(401).json({ success: false, message: "Invalid or expired session" });
    }
}

function optionalAuth(req, res, next) {
    const authorization = req.headers.authorization || "";
    const [scheme, token] = authorization.split(" ");
    if (scheme === "Bearer" && token) {
        try { req.auth = jwt.verify(token, JWT_SECRET); } catch {}
    }
    next();
}

app.get("/", (req, res) => res.json({ success: true, message: "06-Tiles Backend is running" }));

app.get("/api/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");
        res.json({ success: true, database: "connected" });
    } catch (error) {
        console.error("Health check error:", error);
        res.status(503).json({ success: false, database: "disconnected" });
    }
});

app.post("/api/auth/register", async (req, res) => {
    try {
        const username = String(req.body.username || "").trim();
        const email = String(req.body.email || "").trim().toLowerCase();
        const password = String(req.body.password || "");

        if (!username || !email || !password) return res.status(400).json({ success: false, message: "Username, email, and password are required" });
        if (!validUsername(username)) return res.status(400).json({ success: false, message: "Username must be 3-20 characters and contain only letters, numbers, and underscores" });
        if (!validEmail(email) || email.length > 254) return res.status(400).json({ success: false, message: "Please enter a valid email address" });
        if (password.length < 8 || password.length > 128) return res.status(400).json({ success: false, message: "Password must be 8-128 characters" });

        const existing = await pool.query(
            "SELECT username, email FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($2) LIMIT 1",
            [username, email]
        );
        if (existing.rows.length > 0) {
            const match = existing.rows[0];
            return res.status(409).json({
                success: false,
                message: match.username.toLowerCase() === username.toLowerCase() ? "That username is already taken" : "That email is already registered"
            });
        }

        const passwordHash = await bcrypt.hash(password, 12);
        const result = await pool.query(
            `INSERT INTO users (username, email, password_hash)
             VALUES ($1, $2, $3)
             RETURNING id, username, email, profile_icon, created_at, games_played, games_completed,
                       total_score, best_score, total_notes_hit, battle_wins, battle_losses`,
            [username, email, passwordHash]
        );
        const user = result.rows[0];
        res.status(201).json({ success: true, message: "Account created successfully", token: createToken(user), user: publicUser(user) });
    } catch (error) {
        console.error("Registration error:", error);
        if (error.code === "23505") return res.status(409).json({ success: false, message: "Username or email is already in use" });
        res.status(500).json({ success: false, message: "Could not create account" });
    }
});

app.post("/api/auth/login", async (req, res) => {
    try {
        const usernameOrEmail = String(req.body.usernameOrEmail || "").trim();
        const password = String(req.body.password || "");
        if (!usernameOrEmail || !password) return res.status(400).json({ success: false, message: "Username/email and password are required" });

        const result = await pool.query(
            `SELECT id, username, email, password_hash, profile_icon, created_at, games_played,
                    games_completed, total_score, best_score, total_notes_hit, battle_wins, battle_losses
             FROM users
             WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1)
             LIMIT 1`,
            [usernameOrEmail]
        );
        if (result.rows.length === 0) return res.status(401).json({ success: false, message: "Invalid username/email or password" });

        const user = result.rows[0];
        if (!(await bcrypt.compare(password, user.password_hash))) return res.status(401).json({ success: false, message: "Invalid username/email or password" });
        res.json({ success: true, message: "Logged in successfully", token: createToken(user), user: publicUser(user) });
    } catch (error) {
        console.error("Login error:", error);
        res.status(500).json({ success: false, message: "Could not log in" });
    }
});

app.get("/api/auth/me", authenticate, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT id, username, email, profile_icon, created_at, games_played, games_completed,
                    total_score, best_score, total_notes_hit, battle_wins, battle_losses
             FROM users WHERE id = $1 LIMIT 1`,
            [req.auth.userId]
        );
        if (result.rows.length === 0) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(result.rows[0]) });
    } catch (error) {
        console.error("Account lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load account" });
    }
});

app.patch("/api/profile", authenticate, async (req, res) => {
    try {
        const icon = req.body.profileIcon ? String(req.body.profileIcon) : null;
        if (icon && icon.length > 1200000) return res.status(400).json({ success: false, message: "Profile icon is too large" });
        const result = await pool.query(
            `UPDATE users SET profile_icon = $2, updated_at = NOW()
             WHERE id = $1
             RETURNING id, username, email, profile_icon, created_at, games_played, games_completed,
                       total_score, best_score, total_notes_hit, battle_wins, battle_losses`,
            [req.auth.userId, icon]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(result.rows[0]) });
    } catch (error) {
        console.error("Profile update error:", error);
        res.status(500).json({ success: false, message: "Could not update profile" });
    }
});

app.post("/api/stats/game", authenticate, async (req, res) => {
    try {
        const completed = !!req.body.completed;
        const started = !!req.body.started;
        const score = Math.max(0, Math.floor(Number(req.body.score) || 0));
        const notesHit = Math.max(0, Math.floor(Number(req.body.notesHit) || 0));
        const result = await pool.query(
            `UPDATE users
             SET games_played = games_played + $2,
                 games_completed = games_completed + $3,
                 total_score = total_score + $4,
                 best_score = GREATEST(best_score, $4),
                 total_notes_hit = total_notes_hit + $5,
                 updated_at = NOW()
             WHERE id = $1
             RETURNING id, username, email, profile_icon, created_at, games_played, games_completed,
                       total_score, best_score, total_notes_hit, battle_wins, battle_losses`,
            [req.auth.userId, started ? 1 : 0, completed ? 1 : 0, score, notesHit]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(result.rows[0]) });
    } catch (error) {
        console.error("Game stats update error:", error);
        res.status(500).json({ success: false, message: "Could not update game statistics" });
    }
});

app.get("/api/levels", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim();
        const tab = String(req.query.tab || "recent");
        let order = "created_at DESC";
        if (tab === "trending") order = "plays DESC, created_at DESC";
        if (tab === "rated" || tab === "featured") order = "CASE WHEN rating_count = 0 THEN 0 ELSE rating_total::numeric / rating_count END DESC, created_at DESC";

        const values = [];
        let where = "";
        if (search) {
            values.push("%" + search.toLowerCase() + "%");
            where = "WHERE LOWER(name) LIKE $1 OR LOWER(author) LIKE $1";
        }
        const result = await pool.query(
            `SELECT * FROM levels ${where} ORDER BY ${order} LIMIT 100`,
            values
        );
        let levels = result.rows.map(publicLevel);
        if (tab === "featured") levels = levels.filter(l => l.featured);
        res.json(levels);
    } catch (error) {
        console.error("Levels lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load levels" });
    }
});

app.post("/api/levels", optionalAuth, async (req, res) => {
    try {
        const level = req.body || {};
        const name = String(level.name || "").trim().slice(0, 80);
        if (!name || !Array.isArray(level.data) || level.data.length === 0) return res.status(400).json({ success: false, message: "A level name and at least one tile are required" });
        const author = req.auth?.username || String(level.author || "Guest").trim().slice(0, 20) || "Guest";
        const authorId = req.auth?.userId || null;
        const result = await pool.query(
            `INSERT INTO levels
             (name, author_id, author, icon, level_data, effects, lives, fps, audio_offset, disable_holds, difficulty)
             VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11)
             RETURNING *`,
            [
                name, authorId, author, level.icon ? String(level.icon) : null,
                JSON.stringify(level.data), JSON.stringify(level.effects || []),
                Math.max(1, Math.min(10, Number(level.lives) || 3)),
                Math.max(10, Math.min(240, Number(level.fps) || 60)),
                Math.floor(Number(level.audioOffset) || 0),
                !!level.disableHolds, String(level.difficulty || "Normal")
            ]
        );
        res.status(201).json({ success: true, level: publicLevel(result.rows[0]) });
    } catch (error) {
        console.error("Level publish error:", error);
        res.status(500).json({ success: false, message: "Could not publish level" });
    }
});

app.post("/api/levels/:id/play", async (req, res) => {
    try {
        const result = await pool.query("UPDATE levels SET plays = plays + 1, updated_at = NOW() WHERE id = $1 RETURNING plays", [req.params.id]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json({ success: true, plays: result.rows[0].plays });
    } catch (error) {
        res.status(500).json({ success: false, message: "Could not register play" });
    }
});

app.post("/api/levels/:id/rate", authenticate, async (req, res) => {
    try {
        const stars = Math.max(1, Math.min(5, Math.floor(Number(req.body.stars) || 0)));
        if (req.auth.username.toLowerCase() !== "wcrazyness") {
            return res.status(403).json({ success: false, message: "Only wCrazyNess can rate levels right now." });
        }
        const result = await pool.query(
            `UPDATE levels SET rating_total = rating_total + $2, rating_count = rating_count + 1, updated_at = NOW()
             WHERE id = $1 RETURNING rating_total, rating_count`,
            [req.params.id, stars]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json({ success: true, ratingAverage: result.rows[0].rating_total / result.rows[0].rating_count });
    } catch (error) {
        res.status(500).json({ success: false, message: "Could not rate level" });
    }
});

app.get("/api/admin/check", authenticate, async (req, res) => {
    res.json({ success: true, isAdmin: req.auth.username.toLowerCase() === "wcrazyness" });
});

app.get("/api/admin/levels", authenticate, async (req, res) => {
    if (req.auth.username.toLowerCase() !== "wcrazyness") return res.status(403).json({ success: false, message: "Admin access required" });
    const result = await pool.query("SELECT * FROM levels ORDER BY created_at DESC LIMIT 200");
    res.json({ success: true, levels: result.rows.map(publicLevel) });
});

app.patch("/api/admin/levels/:id/feature", authenticate, async (req, res) => {
    if (req.auth.username.toLowerCase() !== "wcrazyness") return res.status(403).json({ success: false, message: "Admin access required" });
    const featured = !!req.body.featured;
    const result = await pool.query(
        "UPDATE levels SET featured = $2, updated_at = NOW() WHERE id = $1 RETURNING *",
        [req.params.id, featured]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
    res.json({ success: true, level: publicLevel(result.rows[0]) });
});

app.delete("/api/admin/levels/:id", authenticate, async (req, res) => {
    if (req.auth.username.toLowerCase() !== "wcrazyness") return res.status(403).json({ success: false, message: "Admin access required" });
    const result = await pool.query("DELETE FROM levels WHERE id = $1 RETURNING id", [req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
    res.json({ success: true });
});

app.delete("/api/levels/:id", authenticate, async (req, res) => {
    try {
        const result = await pool.query("DELETE FROM levels WHERE id = $1 AND author_id = $2 RETURNING id", [req.params.id, req.auth.userId]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found or you do not own it" });
        res.json({ success: true });
    } catch (error) {
        console.error("Level delete error:", error);
        res.status(500).json({ success: false, message: "Could not delete level" });
    }
});

app.get("/api/players", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim();
        const values = [];
        let where = "";
        if (search) {
            values.push("%" + search.toLowerCase() + "%");
            where = "WHERE LOWER(username) LIKE $1";
        }
        const result = await pool.query(
            `SELECT id, username, profile_icon, created_at, games_played, games_completed,
                    total_score, best_score, total_notes_hit, battle_wins, battle_losses
             FROM users ${where}
             ORDER BY games_played DESC, username ASC
             LIMIT 100`,
            values
        );
        res.json(result.rows.map(u => publicUser(u, false)));
    } catch (error) {
        console.error("Players lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load players" });
    }
});

app.get("/api/leaderboards", async (req, res) => {
    try {
        const sort = String(req.query.sort || "bestScore");
        const orderMap = {
            bestScore: "best_score DESC, username ASC",
            totalScore: "total_score DESC, username ASC",
            notesHit: "total_notes_hit DESC, username ASC",
            gamesPlayed: "games_played DESC, username ASC"
        };
        const order = orderMap[sort] || orderMap.bestScore;
        const result = await pool.query(
            `SELECT id, username, profile_icon, created_at, games_played, games_completed,
                    total_score, best_score, total_notes_hit, battle_wins, battle_losses
             FROM users ORDER BY ${order} LIMIT 100`
        );
        res.json(result.rows.map(u => publicUser(u, false)));
    } catch (error) {
        console.error("Leaderboard error:", error);
        res.status(500).json({ success: false, message: "Could not load leaderboard" });
    }
});

async function startServer() {
    try {
        await initializeDatabase();
        app.listen(PORT, () => {
            console.log(`06-Tiles Backend running on port ${PORT}`);
            console.log("PostgreSQL database initialized");
        });
    } catch (error) {
        console.error("Database initialization failed:", error);
        process.exit(1);
    }
}

startServer();