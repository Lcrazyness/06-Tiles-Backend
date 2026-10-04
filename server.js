// ============================================================================
// 06-Tiles Backend v2
//   - REST API: accounts, levels, ratings, stats, leaderboards, admin
//   - Realtime: Socket.IO battle server (presence, quick match, challenges,
//     synced start, live progress, server-computed results, rematch)
// ============================================================================
const http = require("http");
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { Server } = require("socket.io");

const app = express();
app.set("trust proxy", 1);
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: true, credentials: true },
    maxHttpBufferSize: 8e6,
    pingInterval: 10000,
    pingTimeout: 15000
});

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const JWT_SECRET = process.env.JWT_SECRET;
// Comma-separated list of admin usernames (case-insensitive). Defaults to the original admin.
const ADMIN_USERNAMES = (process.env.ADMIN_USERNAMES || "wcrazyness").split(",").map(s => s.trim().toLowerCase()).filter(Boolean);

if (!DATABASE_URL) { console.error("DATABASE_URL is not configured."); process.exit(1); }
if (!JWT_SECRET) { console.error("JWT_SECRET is not configured."); process.exit(1); }

const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});

app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: "8mb" }));

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const hits = new Map();
function rateLimit(name, max, windowMs) {
    return (req, res, next) => {
        const key = name + ":" + (req.ip || "");
        const now = Date.now();
        let entry = hits.get(key);
        if (!entry || entry.reset < now) { entry = { count: 0, reset: now + windowMs }; hits.set(key, entry); }
        entry.count++;
        if (entry.count > max) return res.status(429).json({ success: false, message: "Too many requests - slow down a little." });
        next();
    };
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (v.reset < now) hits.delete(k); }, 60000).unref();

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const isUuid = v => UUID_RE.test(String(v || ""));
const isAdminName = name => ADMIN_USERNAMES.includes(String(name || "").trim().toLowerCase());
const clampInt = (v, lo, hi, d = 0) => { const n = Math.floor(Number(v)); return Number.isFinite(n) ? Math.max(lo, Math.min(hi, n)) : d; };
const DIFFICULTIES = ["Easy", "Normal", "Hard", "Insane", "Extreme"];

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
    await pool.query("ALTER TABLE levels ADD COLUMN IF NOT EXISTS featured BOOLEAN NOT NULL DEFAULT FALSE");
    // v2: level appearance + editor grid settings now persist online
    await pool.query("ALTER TABLE levels ADD COLUMN IF NOT EXISTS meta JSONB NOT NULL DEFAULT '{}'::jsonb");
    await pool.query(`
        CREATE TABLE IF NOT EXISTS level_ratings (
            level_id UUID NOT NULL REFERENCES levels(id) ON DELETE CASCADE,
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            stars SMALLINT NOT NULL CHECK (stars BETWEEN 1 AND 5),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (level_id, user_id)
        )
    `);
    await pool.query("CREATE INDEX IF NOT EXISTS levels_created_idx ON levels (created_at DESC)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_plays_idx ON levels (plays DESC)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_author_idx ON levels (LOWER(author))");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_author_id_idx ON levels (author_id)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_featured_idx ON levels (featured) WHERE featured = TRUE");
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
        isAdmin: isAdminName(user.username),
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

// Summaries leave out the heavy note data / effect images so Browse stays fast.
const LEVEL_SUMMARY_COLUMNS = `id, name, author_id, author, icon, lives, fps, audio_offset, disable_holds, difficulty,
    plays, rating_total, rating_count, featured, created_at, meta,
    CASE WHEN jsonb_typeof(level_data) = 'array' THEN jsonb_array_length(level_data) ELSE 0 END AS tile_count`;

function publicLevel(row, full = false) {
    const meta = row.meta || {};
    const level = {
        id: row.id,
        name: row.name,
        author: row.author,
        authorId: row.author_id,
        icon: row.icon || null,
        lives: row.lives,
        fps: row.fps,
        audioOffset: row.audio_offset,
        disableHolds: row.disable_holds,
        difficulty: row.difficulty,
        plays: row.plays,
        ratingAverage: row.rating_count ? row.rating_total / row.rating_count : 0,
        ratingCount: row.rating_count,
        featured: !!row.featured,
        createdAt: row.created_at,
        tileCount: row.tile_count !== undefined ? Number(row.tile_count) : (row.level_data || []).length,
        backgroundColor: meta.backgroundColor || "#202738",
        backgroundBrightness: meta.backgroundBrightness || 100,
        bpm: meta.bpm || 120,
        gridOffset: meta.gridOffset || 0
    };
    if (full) {
        level.data = row.level_data || [];
        level.effects = row.effects || [];
    }
    return level;
}

function sanitizeNotes(data) {
    if (!Array.isArray(data) || data.length === 0 || data.length > 6000) return null;
    const out = [];
    for (const n of data) {
        const lane = Number(n && n.lane);
        const time = Number(n && n.time);
        if (!Number.isInteger(lane) || lane < 0 || lane > 3 || !Number.isFinite(time) || time < 0 || time > 3600) return null;
        const hold = !!n.isHold;
        out.push({ lane, time: Math.round(time * 1000) / 1000, isHold: hold, holdDuration: hold ? Math.max(0, Math.min(30, Number(n.holdDuration) || 0)) : 0 });
    }
    out.sort((a, b) => a.time - b.time || a.lane - b.lane);
    return out;
}

function sanitizeEffects(effects) {
    if (!Array.isArray(effects)) return [];
    return effects
        .filter(e => e && typeof e === "object" && typeof e.type === "string" && Number.isFinite(Number(e.time)))
        .filter(e => !e.src || (typeof e.src === "string" && e.src.length < 3000000))
        .slice(0, 500);
}

function sanitizeLevel(body) {
    const level = body || {};
    const name = String(level.name || "").trim().slice(0, 80);
    const notes = sanitizeNotes(level.data);
    if (!name) return { error: "Give your level a name." };
    if (!notes) return { error: "A level needs between 1 and 6000 valid tiles." };
    const icon = typeof level.icon === "string" && level.icon.startsWith("data:image/") && level.icon.length <= 400000 ? level.icon : null;
    const meta = {};
    if (/^#[0-9a-f]{6}$/i.test(String(level.backgroundColor || ""))) meta.backgroundColor = level.backgroundColor;
    if (Number.isFinite(Number(level.backgroundBrightness))) meta.backgroundBrightness = clampInt(level.backgroundBrightness, 70, 140, 100);
    if (Number.isFinite(Number(level.bpm))) meta.bpm = clampInt(level.bpm, 30, 300, 120);
    if (Number.isFinite(Number(level.gridOffset))) meta.gridOffset = Math.max(-5, Math.min(5, Number(level.gridOffset)));
    return {
        name, notes, icon, meta,
        effects: sanitizeEffects(level.effects),
        lives: clampInt(level.lives, 1, 10, 3),
        fps: clampInt(level.fps, 10, 240, 60),
        audioOffset: clampInt(level.audioOffset, -10000, 10000, 0),
        disableHolds: !!level.disableHolds,
        difficulty: DIFFICULTIES.includes(level.difficulty) ? level.difficulty : "Normal"
    };
}

const validEmail = email => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
const validUsername = username => /^[A-Za-z0-9_]{3,20}$/.test(username);

function authenticate(req, res, next) {
    const [scheme, token] = (req.headers.authorization || "").split(" ");
    if (scheme !== "Bearer" || !token) return res.status(401).json({ success: false, message: "Authentication required" });
    try { req.auth = jwt.verify(token, JWT_SECRET); next(); }
    catch { return res.status(401).json({ success: false, message: "Invalid or expired session" }); }
}

function requireAdmin(req, res, next) {
    authenticate(req, res, () => {
        if (!isAdminName(req.auth.username)) return res.status(403).json({ success: false, message: "Admin access required" });
        next();
    });
}

const USER_COLUMNS = `id, username, email, profile_icon, created_at, games_played, games_completed,
    total_score, best_score, total_notes_hit, battle_wins, battle_losses`;

// ---------------------------------------------------------------------------
// health + auth
// ---------------------------------------------------------------------------
app.get("/", (req, res) => res.json({ success: true, message: "06-Tiles Backend is running", version: 2, realtime: true }));

app.get("/api/health", async (req, res) => {
    try { await pool.query("SELECT 1"); res.json({ success: true, database: "connected", online: lobby.size }); }
    catch (error) { console.error("Health check error:", error); res.status(503).json({ success: false, database: "disconnected" }); }
});

app.post("/api/auth/register", rateLimit("register", 10, 60 * 60 * 1000), async (req, res) => {
    try {
        const username = String(req.body.username || "").trim();
        const email = String(req.body.email || "").trim().toLowerCase();
        const password = String(req.body.password || "");
        if (!username || !email || !password) return res.status(400).json({ success: false, message: "Username, email, and password are required" });
        if (!validUsername(username)) return res.status(400).json({ success: false, message: "Username must be 3-20 characters: letters, numbers, underscores" });
        if (!validEmail(email) || email.length > 254) return res.status(400).json({ success: false, message: "Please enter a valid email address" });
        if (password.length < 8 || password.length > 128) return res.status(400).json({ success: false, message: "Password must be 8-128 characters" });

        const existing = await pool.query("SELECT username, email FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($2) LIMIT 1", [username, email]);
        if (existing.rows.length > 0) {
            const match = existing.rows[0];
            return res.status(409).json({ success: false, message: match.username.toLowerCase() === username.toLowerCase() ? "That username is already taken" : "That email is already registered" });
        }
        const passwordHash = await bcrypt.hash(password, 12);
        const result = await pool.query(`INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING ${USER_COLUMNS}`, [username, email, passwordHash]);
        const user = result.rows[0];
        res.status(201).json({ success: true, message: "Account created successfully", token: createToken(user), user: publicUser(user) });
    } catch (error) {
        console.error("Registration error:", error);
        if (error.code === "23505") return res.status(409).json({ success: false, message: "Username or email is already in use" });
        res.status(500).json({ success: false, message: "Could not create account" });
    }
});

app.post("/api/auth/login", rateLimit("login", 30, 15 * 60 * 1000), async (req, res) => {
    try {
        const usernameOrEmail = String(req.body.usernameOrEmail || "").trim();
        const password = String(req.body.password || "");
        if (!usernameOrEmail || !password) return res.status(400).json({ success: false, message: "Username/email and password are required" });
        const result = await pool.query(`SELECT ${USER_COLUMNS}, password_hash FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1) LIMIT 1`, [usernameOrEmail]);
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
        const result = await pool.query(`SELECT ${USER_COLUMNS} FROM users WHERE id = $1 LIMIT 1`, [req.auth.userId]);
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
        if (icon && (!icon.startsWith("data:image/") || icon.length > 400000)) return res.status(400).json({ success: false, message: "Profile icon is too large" });
        const result = await pool.query(`UPDATE users SET profile_icon = $2, updated_at = NOW() WHERE id = $1 RETURNING ${USER_COLUMNS}`, [req.auth.userId, icon]);
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
            `UPDATE users SET games_played = games_played + $2, games_completed = games_completed + $3,
                total_score = total_score + $4, best_score = GREATEST(best_score, $4),
                total_notes_hit = total_notes_hit + $5, updated_at = NOW()
             WHERE id = $1 RETURNING ${USER_COLUMNS}`,
            [req.auth.userId, started ? 1 : 0, completed ? 1 : 0, score, notesHit]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(result.rows[0]) });
    } catch (error) {
        console.error("Game stats update error:", error);
        res.status(500).json({ success: false, message: "Could not update game statistics" });
    }
});

// ---------------------------------------------------------------------------
// levels
// ---------------------------------------------------------------------------
app.get("/api/levels", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim().slice(0, 60);
        const tab = String(req.query.tab || "recent");
        const orders = {
            recent: "created_at DESC",
            trending: "plays DESC, created_at DESC",
            rated: "(CASE WHEN rating_count = 0 THEN 0 ELSE rating_total::numeric / rating_count END) DESC, rating_count DESC, created_at DESC",
            featured: "created_at DESC"
        };
        const order = orders[tab] || orders.recent;
        const where = [];
        const values = [];
        if (search) {
            values.push("%" + search.toLowerCase().replace(/[\\%_]/g, m => "\\" + m) + "%");
            where.push(`(LOWER(name) LIKE $${values.length} OR LOWER(author) LIKE $${values.length})`);
        }
        if (tab === "featured") where.push("featured = TRUE");
        const result = await pool.query(
            `SELECT ${LEVEL_SUMMARY_COLUMNS} FROM levels ${where.length ? "WHERE " + where.join(" AND ") : ""} ORDER BY ${order} LIMIT 100`,
            values
        );
        res.json(result.rows.map(r => publicLevel(r)));
    } catch (error) {
        console.error("Levels lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load levels" });
    }
});

app.get("/api/levels/:id", async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query("SELECT * FROM levels WHERE id = $1", [req.params.id]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json(publicLevel(result.rows[0], true));
    } catch (error) {
        console.error("Level fetch error:", error);
        res.status(500).json({ success: false, message: "Could not load level" });
    }
});

app.get("/api/my/levels", authenticate, async (req, res) => {
    try {
        const result = await pool.query(`SELECT ${LEVEL_SUMMARY_COLUMNS} FROM levels WHERE author_id = $1 ORDER BY created_at DESC LIMIT 200`, [req.auth.userId]);
        res.json(result.rows.map(r => publicLevel(r)));
    } catch (error) {
        console.error("My levels error:", error);
        res.status(500).json({ success: false, message: "Could not load your published levels" });
    }
});

// Publishing now requires an account: guests could previously publish under any
// author name, including a real player's (or the admin's).
app.post("/api/levels", authenticate, rateLimit("publish", 20, 60 * 60 * 1000), async (req, res) => {
    try {
        const clean = sanitizeLevel(req.body);
        if (clean.error) return res.status(400).json({ success: false, message: clean.error });
        const result = await pool.query(
            `INSERT INTO levels (name, author_id, author, icon, level_data, effects, lives, fps, audio_offset, disable_holds, difficulty, meta)
             VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11,$12::jsonb) RETURNING *`,
            [clean.name, req.auth.userId, req.auth.username, clean.icon, JSON.stringify(clean.notes), JSON.stringify(clean.effects),
             clean.lives, clean.fps, clean.audioOffset, clean.disableHolds, clean.difficulty, JSON.stringify(clean.meta)]
        );
        res.status(201).json({ success: true, level: publicLevel(result.rows[0]) });
    } catch (error) {
        console.error("Level publish error:", error);
        res.status(500).json({ success: false, message: "Could not publish level" });
    }
});

app.post("/api/levels/:id/play", rateLimit("play", 120, 60 * 1000), async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query("UPDATE levels SET plays = plays + 1 WHERE id = $1 RETURNING plays", [req.params.id]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json({ success: true, plays: result.rows[0].plays });
    } catch (error) {
        res.status(500).json({ success: false, message: "Could not register play" });
    }
});

// Any logged-in player can rate (one rating each; re-rating replaces it).
app.post("/api/levels/:id/rate", authenticate, async (req, res) => {
    const client = await pool.connect();
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const stars = clampInt(req.body.stars, 1, 5, 0);
        if (!stars) return res.status(400).json({ success: false, message: "Pick 1 to 5 stars" });
        await client.query("BEGIN");
        const level = await client.query("SELECT id FROM levels WHERE id = $1 FOR UPDATE", [req.params.id]);
        if (!level.rows.length) { await client.query("ROLLBACK"); return res.status(404).json({ success: false, message: "Level not found" }); }
        const prev = await client.query("SELECT stars FROM level_ratings WHERE level_id = $1 AND user_id = $2", [req.params.id, req.auth.userId]);
        await client.query(
            `INSERT INTO level_ratings (level_id, user_id, stars) VALUES ($1,$2,$3)
             ON CONFLICT (level_id, user_id) DO UPDATE SET stars = EXCLUDED.stars`,
            [req.params.id, req.auth.userId, stars]
        );
        const result = prev.rows.length
            ? await client.query("UPDATE levels SET rating_total = rating_total + $2 WHERE id = $1 RETURNING rating_total, rating_count", [req.params.id, stars - prev.rows[0].stars])
            : await client.query("UPDATE levels SET rating_total = rating_total + $2, rating_count = rating_count + 1 WHERE id = $1 RETURNING rating_total, rating_count", [req.params.id, stars]);
        await client.query("COMMIT");
        const row = result.rows[0];
        res.json({ success: true, ratingAverage: row.rating_count ? row.rating_total / row.rating_count : 0, ratingCount: row.rating_count });
    } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        console.error("Rate error:", error);
        res.status(500).json({ success: false, message: "Could not rate level" });
    } finally {
        client.release();
    }
});

app.delete("/api/levels/:id", authenticate, async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query("DELETE FROM levels WHERE id = $1 AND author_id = $2 RETURNING id", [req.params.id, req.auth.userId]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found or you do not own it" });
        res.json({ success: true });
    } catch (error) {
        console.error("Level delete error:", error);
        res.status(500).json({ success: false, message: "Could not delete level" });
    }
});

// ---------------------------------------------------------------------------
// admin
// ---------------------------------------------------------------------------
app.get("/api/admin/check", authenticate, (req, res) => res.json({ success: true, isAdmin: isAdminName(req.auth.username) }));

app.get("/api/admin/overview", requireAdmin, async (req, res) => {
    try {
        const [users, levels, plays] = await Promise.all([
            pool.query("SELECT COUNT(*)::int AS n FROM users"),
            pool.query("SELECT COUNT(*)::int AS n, COUNT(*) FILTER (WHERE featured)::int AS featured FROM levels"),
            pool.query("SELECT COALESCE(SUM(plays),0)::bigint AS n FROM levels")
        ]);
        res.json({ success: true, users: users.rows[0].n, levels: levels.rows[0].n, featured: levels.rows[0].featured, plays: Number(plays.rows[0].n), onlineNow: lobby.size });
    } catch (error) {
        console.error("Admin overview error:", error);
        res.status(500).json({ success: false, message: "Could not load overview" });
    }
});

app.get("/api/admin/levels", requireAdmin, async (req, res) => {
    try {
        const search = String(req.query.search || "").trim().slice(0, 60);
        const values = [];
        let where = "";
        if (search) { values.push("%" + search.toLowerCase().replace(/[\\%_]/g, m => "\\" + m) + "%"); where = "WHERE LOWER(name) LIKE $1 OR LOWER(author) LIKE $1"; }
        const result = await pool.query(`SELECT ${LEVEL_SUMMARY_COLUMNS} FROM levels ${where} ORDER BY created_at DESC LIMIT 200`, values);
        res.json({ success: true, levels: result.rows.map(r => publicLevel(r)) });
    } catch (error) {
        console.error("Admin levels error:", error);
        res.status(500).json({ success: false, message: "Could not load admin levels" });
    }
});

app.patch("/api/admin/levels/:id/feature", requireAdmin, async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query(`UPDATE levels SET featured = $2, updated_at = NOW() WHERE id = $1 RETURNING ${LEVEL_SUMMARY_COLUMNS}`, [req.params.id, !!req.body.featured]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json({ success: true, level: publicLevel(result.rows[0]) });
    } catch (error) {
        console.error("Admin feature error:", error);
        res.status(500).json({ success: false, message: "Could not update featured status" });
    }
});

app.delete("/api/admin/levels/:id", requireAdmin, async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query("DELETE FROM levels WHERE id = $1 RETURNING id", [req.params.id]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        res.json({ success: true });
    } catch (error) {
        console.error("Admin delete error:", error);
        res.status(500).json({ success: false, message: "Could not delete level" });
    }
});

// ---------------------------------------------------------------------------
// players + leaderboards
// ---------------------------------------------------------------------------
app.get("/api/players", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim().slice(0, 30);
        const values = [];
        let where = "";
        if (search) { values.push("%" + search.toLowerCase().replace(/[\\%_]/g, m => "\\" + m) + "%"); where = "WHERE LOWER(username) LIKE $1"; }
        const result = await pool.query(`SELECT ${USER_COLUMNS} FROM users ${where} ORDER BY games_played DESC, username ASC LIMIT 100`, values);
        res.json(result.rows.map(u => publicUser(u, false)));
    } catch (error) {
        console.error("Players lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load players" });
    }
});

app.get("/api/leaderboards", async (req, res) => {
    try {
        const orderMap = {
            bestScore: "best_score DESC, username ASC",
            totalScore: "total_score DESC, username ASC",
            notesHit: "total_notes_hit DESC, username ASC",
            gamesPlayed: "games_played DESC, username ASC",
            battleWins: "battle_wins DESC, username ASC"
        };
        const order = orderMap[String(req.query.sort || "bestScore")] || orderMap.bestScore;
        const result = await pool.query(`SELECT ${USER_COLUMNS} FROM users ORDER BY ${order} LIMIT 100`);
        res.json(result.rows.map(u => publicUser(u, false)));
    } catch (error) {
        console.error("Leaderboard error:", error);
        res.status(500).json({ success: false, message: "Could not load leaderboard" });
    }
});

// ===========================================================================
// REALTIME BATTLES (Socket.IO)
//
// Clients connect only while the Battle screen / a match is open. Identity
// comes from the JWT (logged in) or a per-browser guest id. Everything that
// decides a result lives here, not in the browsers.
// ===========================================================================
const lobby = new Map();              // socket.id -> socket
const queue = [];                     // socket ids waiting for a quick match
const matches = new Map();            // matchId -> match
const challenges = new Map();         // challengeId -> { id, from, to, level }

const newId = prefix => prefix + "_" + crypto.randomBytes(6).toString("hex");

io.use((socket, next) => {
    const auth = socket.handshake.auth || {};
    let name = null, userId = null;
    if (auth.token) {
        try { const p = jwt.verify(String(auth.token), JWT_SECRET); name = p.username; userId = p.userId; } catch {}
    }
    if (!name) {
        const tag = String(auth.guestId || "").replace(/[^a-z0-9]/gi, "").slice(0, 4).toUpperCase() || crypto.randomBytes(2).toString("hex").toUpperCase();
        name = "Guest-" + tag;
    }
    // Two tabs with the same name must still be told apart in the lobby list.
    const taken = new Set([...lobby.values()].map(s => s.data.name));
    let unique = name, n = 2;
    while (taken.has(unique)) unique = name + " (" + (n++) + ")";
    socket.data = { name: unique, userId, status: "lobby", matchId: null };
    next();
});

let presenceTimer = null;
function schedulePresence() {
    if (presenceTimer) return;
    presenceTimer = setTimeout(() => {
        presenceTimer = null;
        const list = [...lobby.values()].map(s => ({ id: s.id, name: s.data.name, status: s.data.status, registered: !!s.data.userId }));
        io.emit("presence", list);
    }, 250);
}

function setStatus(socket, status, matchId = null) {
    if (!socket || !socket.data) return;
    socket.data.status = status;
    socket.data.matchId = matchId;
    schedulePresence();
}

function removeFromQueue(id) {
    const i = queue.indexOf(id);
    if (i !== -1) queue.splice(i, 1);
}

async function pickRandomLevel() {
    const result = await pool.query(
        `SELECT * FROM levels
         WHERE jsonb_typeof(level_data) = 'array' AND jsonb_array_length(level_data) >= 8
         ORDER BY random() LIMIT 1`
    );
    return result.rows.length ? publicLevel(result.rows[0], true) : null;
}

function levelForMatch(level) {
    return {
        id: level.id || null, name: level.name, author: level.author || null,
        data: level.data, effects: level.effects || [], lives: level.lives, fps: level.fps,
        audioOffset: level.audioOffset || 0, disableHolds: !!level.disableHolds, difficulty: level.difficulty || "Normal",
        backgroundColor: level.backgroundColor || "#202738", backgroundBrightness: level.backgroundBrightness || 100,
        icon: level.icon || null
    };
}

function createMatch(socketA, socketB, level) {
    const id = newId("m");
    const player = s => ({ sid: s.id, name: s.data.name, userId: s.data.userId, ready: false, result: null, rematch: false, left: false });
    const match = { id, level: levelForMatch(level), players: { [socketA.id]: player(socketA), [socketB.id]: player(socketB) }, started: false, done: false, readyTimer: null };
    matches.set(id, match);
    setStatus(socketA, "match", id);
    setStatus(socketB, "match", id);
    [[socketA, socketB], [socketB, socketA]].forEach(([me, opp]) => {
        me.emit("match_found", { matchId: id, opponent: { name: opp.data.name }, level: match.level });
    });
    // Both clients must finish loading the level; otherwise cancel instead of hanging forever.
    match.readyTimer = setTimeout(() => {
        if (match.started || match.done) return;
        cancelMatch(match, "Your opponent took too long to load the level.");
    }, 30000);
    return match;
}

function otherOf(match, sid) {
    return Object.values(match.players).find(p => p.sid !== sid);
}

function cancelMatch(match, message) {
    clearTimeout(match.readyTimer);
    match.done = true;
    Object.values(match.players).forEach(p => {
        const s = lobby.get(p.sid);
        if (s) { s.emit("match_cancelled", { matchId: match.id, message }); setStatus(s, "lobby"); }
    });
    matches.delete(match.id);
}

async function applyBattleStats(winnerUserId, loserUserId) {
    try {
        if (winnerUserId) await pool.query("UPDATE users SET battle_wins = battle_wins + 1 WHERE id = $1", [winnerUserId]);
        if (loserUserId) await pool.query("UPDATE users SET battle_losses = battle_losses + 1 WHERE id = $1", [loserUserId]);
    } catch (error) { console.error("Battle stats error:", error); }
}

function resolveMatch(match, forfeitBy = null) {
    if (match.done) return;
    match.done = true;
    clearTimeout(match.readyTimer);
    const [a, b] = Object.values(match.players);
    let outcomeA;
    if (forfeitBy) outcomeA = forfeitBy === a.sid ? "lose" : "win";
    else if (a.result.finished && !b.result.finished) outcomeA = "win";
    else if (!a.result.finished && b.result.finished) outcomeA = "lose";
    else if (a.result.score > b.result.score) outcomeA = "win";
    else if (a.result.score < b.result.score) outcomeA = "lose";
    else outcomeA = "draw";
    const flip = o => (o === "win" ? "lose" : o === "lose" ? "win" : "draw");
    const send = (me, opp, outcome) => {
        const s = lobby.get(me.sid);
        if (s) { s.emit("match_result", { matchId: match.id, outcome, forfeit: !!forfeitBy && forfeitBy !== me.sid, you: { score: me.result ? me.result.score : 0 }, opp: { name: opp.name, score: opp.result ? opp.result.score : 0 } }); setStatus(s, "lobby"); }
    };
    send(a, b, outcomeA);
    send(b, a, flip(outcomeA));
    if (outcomeA === "win") applyBattleStats(a.userId, b.userId);
    else if (outcomeA === "lose") applyBattleStats(b.userId, a.userId);
    // keep the match around briefly so a rematch can reuse the level
    setTimeout(() => matches.delete(match.id), 10 * 60 * 1000).unref();
}

function forfeit(match, sid) {
    if (match.done) return;
    const me = match.players[sid];
    const opp = otherOf(match, sid);
    me.left = true;
    me.result = me.result || { finished: false, score: 0 };
    if (opp && !opp.result) opp.result = { finished: false, score: 0 };
    const oppSocket = opp && lobby.get(opp.sid);
    if (oppSocket) oppSocket.emit("opp_left", { name: me.name });
    resolveMatch(match, sid);
}

async function tryPairQueue() {
    while (queue.length >= 2) {
        const a = lobby.get(queue.shift());
        const b = lobby.get(queue.shift());
        if (!a || a.data.status !== "queue") { if (b && b.data.status === "queue") queue.unshift(b.id); continue; }
        if (!b || b.data.status !== "queue") { queue.unshift(a.id); continue; }
        try {
            const level = await pickRandomLevel();
            if (!level) {
                [a, b].forEach(s => { s.emit("queue_error", { message: "No published levels exist yet. Publish one first!" }); setStatus(s, "lobby"); });
                continue;
            }
            createMatch(a, b, level);
        } catch (error) {
            console.error("Quick match error:", error);
            [a, b].forEach(s => { s.emit("queue_error", { message: "Could not start the match. Try again." }); setStatus(s, "lobby"); });
        }
    }
}

io.on("connection", socket => {
    lobby.set(socket.id, socket);
    socket.emit("welcome", { id: socket.id, name: socket.data.name, registered: !!socket.data.userId });
    schedulePresence();

    socket.on("presence_request", () => schedulePresence());

    socket.on("queue_join", () => {
        if (socket.data.status !== "lobby") return;
        setStatus(socket, "queue");
        queue.push(socket.id);
        tryPairQueue();
    });
    socket.on("queue_leave", () => {
        removeFromQueue(socket.id);
        if (socket.data.status === "queue") setStatus(socket, "lobby");
    });

    // --- challenges ---
    socket.on("challenge_send", payload => {
        const target = lobby.get(String(payload && payload.toId));
        if (!target || target.id === socket.id) return socket.emit("challenge_failed", { message: "That player is no longer online." });
        if (target.data.status !== "lobby" || socket.data.status !== "lobby") return socket.emit("challenge_failed", { message: "That player is busy right now." });
        const clean = sanitizeLevel(payload.level);
        if (clean.error) return socket.emit("challenge_failed", { message: clean.error });
        const level = {
            id: payload.level.id && isUuid(payload.level.id) ? payload.level.id : null,
            name: clean.name, data: clean.notes, effects: clean.effects, lives: clean.lives, fps: clean.fps,
            audioOffset: clean.audioOffset, disableHolds: clean.disableHolds, difficulty: clean.difficulty,
            backgroundColor: clean.meta.backgroundColor, backgroundBrightness: clean.meta.backgroundBrightness, icon: clean.icon,
            author: String(payload.level.author || socket.data.name).slice(0, 20)
        };
        const id = newId("c");
        const challenge = { id, from: socket.id, to: target.id, level, timer: null };
        challenge.timer = setTimeout(() => {
            if (!challenges.has(id)) return;
            challenges.delete(id);
            socket.emit("challenge_expired", { challengeId: id });
            target.emit("challenge_expired", { challengeId: id });
        }, 30000);
        challenges.set(id, challenge);
        socket.emit("challenge_sent", { challengeId: id, to: target.data.name });
        target.emit("challenge_received", { challengeId: id, from: { name: socket.data.name }, level: { name: level.name, tileCount: level.data.length, difficulty: level.difficulty, icon: level.icon } });
    });
    socket.on("challenge_respond", payload => {
        const challenge = challenges.get(String(payload && payload.challengeId));
        if (!challenge || challenge.to !== socket.id) return;
        clearTimeout(challenge.timer);
        challenges.delete(challenge.id);
        const sender = lobby.get(challenge.from);
        if (!sender) return socket.emit("challenge_failed", { message: "The challenger left." });
        if (!payload.accept) return sender.emit("challenge_declined", { name: socket.data.name });
        if (sender.data.status !== "lobby" || socket.data.status !== "lobby") {
            sender.emit("challenge_failed", { message: "Someone became busy. Try again." });
            return socket.emit("challenge_failed", { message: "Someone became busy. Try again." });
        }
        createMatch(sender, socket, challenge.level);
    });
    socket.on("challenge_cancel", payload => {
        const challenge = challenges.get(String(payload && payload.challengeId));
        if (!challenge || challenge.from !== socket.id) return;
        clearTimeout(challenge.timer);
        challenges.delete(challenge.id);
        const target = lobby.get(challenge.to);
        if (target) target.emit("challenge_cancelled", { challengeId: challenge.id });
    });

    // --- the match itself ---
    socket.on("match_ready", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || match.started || match.done) return;
        me.ready = true;
        if (Object.values(match.players).every(p => p.ready)) {
            match.started = true;
            clearTimeout(match.readyTimer);
            Object.values(match.players).forEach(p => { const s = lobby.get(p.sid); if (s) s.emit("match_go", { matchId: match.id, countdown: 3 }); });
        }
    });
    socket.on("match_progress", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || match.done) return;
        const opp = otherOf(match, socket.id);
        const s = opp && lobby.get(opp.sid);
        if (s) s.volatile.emit("opp_progress", { pct: clampInt(payload.pct, 0, 100), score: clampInt(payload.score, 0, 1e9) });
    });
    socket.on("match_finish", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || match.done || me.result) return;
        me.result = { finished: !!payload.finished, score: clampInt(payload.score, 0, 1e9), reason: String(payload.reason || "").slice(0, 30) };
        const opp = otherOf(match, socket.id);
        const s = opp && lobby.get(opp.sid);
        if (s) s.emit("opp_finish", me.result);
        if (opp && opp.result) resolveMatch(match);
    });
    socket.on("match_leave", payload => {
        const match = matches.get(String(payload && payload.matchId));
        if (!match || !match.players[socket.id]) return;
        if (!match.done) forfeit(match, socket.id);
        else { match.players[socket.id].left = true; setStatus(socket, "lobby"); }
    });
    socket.on("rematch_request", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || !match.done) return;
        const opp = otherOf(match, socket.id);
        const oppSocket = opp && !opp.left && lobby.get(opp.sid);
        if (!oppSocket || oppSocket.data.status !== "lobby") return socket.emit("challenge_failed", { message: "Your opponent left." });
        me.rematch = true;
        if (opp.rematch) { matches.delete(match.id); createMatch(socket, oppSocket, match.level); }
        else oppSocket.emit("rematch_requested", { name: socket.data.name });
    });

    socket.on("disconnect", () => {
        removeFromQueue(socket.id);
        lobby.delete(socket.id);
        for (const [id, c] of challenges) {
            if (c.from === socket.id || c.to === socket.id) {
                clearTimeout(c.timer);
                challenges.delete(id);
                const other = lobby.get(c.from === socket.id ? c.to : c.from);
                if (other) other.emit("challenge_cancelled", { challengeId: id });
            }
        }
        const matchId = socket.data && socket.data.matchId;
        const match = matchId && matches.get(matchId);
        if (match && !match.done) {
            if (match.started || Object.values(match.players).some(p => p.ready)) forfeit(match, socket.id);
            else cancelMatch(match, "Your opponent disconnected.");
        }
        schedulePresence();
    });
});

async function startServer() {
    try {
        await initializeDatabase();
        server.listen(PORT, () => {
            console.log(`06-Tiles Backend v2 running on port ${PORT}`);
            console.log("PostgreSQL database initialized; Socket.IO battle server ready");
        });
    } catch (error) {
        console.error("Database initialization failed:", error);
        process.exit(1);
    }
}

startServer();
