// ============================================================================
// 06-Tiles Backend v3
//   v3: admin-rated levels -> stars, creator points, difficulty beaten, bans/warnings,
//       5-star notifications, battles = rated levels only, first to 3 round wins
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

// Official levels shipped with the server (see ./levels/*.json). Seeded once; admins can delete/edit them afterwards.
async function seedOfficialLevels() {
    const fs = require("fs"), path = require("path");
    const dir = path.join(__dirname, "levels");
    if (!fs.existsSync(dir)) return;
    for (const file of fs.readdirSync(dir).filter(f => f.endsWith(".json")).sort()) {
        const key = "seeded:" + file;
        const done = await pool.query("SELECT 1 FROM app_meta WHERE key = $1", [key]);
        if (done.rows.length) continue;
        try {
            const raw = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
            const clean = sanitizeLevel(raw);
            if (clean.error) { console.error("Seed level " + file + " invalid: " + clean.error); continue; }
            await pool.query(
                `INSERT INTO levels (name, author_id, author, icon, level_data, effects, lives, fps, audio_offset, disable_holds, difficulty, meta, featured, rated_stars)
                 VALUES ($1,NULL,$2,$3,$4::jsonb,$5::jsonb,$6,$7,$8,$9,$10,$11::jsonb,TRUE,$12)`,
                [clean.name, String(raw.author || "06 Tiles").slice(0, 20), clean.icon, JSON.stringify(clean.notes), JSON.stringify(clean.effects),
                 clean.lives, clean.fps, clean.audioOffset, clean.disableHolds, clean.difficulty, JSON.stringify(clean.meta), clampInt(raw.ratedStars, 1, 10, 5)]
            );
            await pool.query("INSERT INTO app_meta (key, value) VALUES ($1, NOW()::text) ON CONFLICT DO NOTHING", [key]);
            console.log("Seeded official level: " + clean.name);
        } catch (error) { console.error("Seeding " + file + " failed:", error); }
    }
}

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
    // v3
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_until TIMESTAMPTZ");
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS ban_reason TEXT");
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS pending_warning TEXT");
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS warning_at TIMESTAMPTZ");
    await pool.query("ALTER TABLE levels ADD COLUMN IF NOT EXISTS rated_stars SMALLINT NOT NULL DEFAULT 0");
    await pool.query(`
        CREATE TABLE IF NOT EXISTS level_completions (
            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            level_id UUID NOT NULL REFERENCES levels(id) ON DELETE CASCADE,
            best_score BIGINT NOT NULL DEFAULT 0,
            completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (user_id, level_id)
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS admin_notifications (
            id BIGSERIAL PRIMARY KEY,
            type VARCHAR(30) NOT NULL,
            level_id UUID REFERENCES levels(id) ON DELETE CASCADE,
            user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            message TEXT NOT NULL DEFAULT '',
            read BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (type, level_id, user_id)
        )
    `);
    await pool.query("CREATE INDEX IF NOT EXISTS admin_notifications_unread_idx ON admin_notifications (read, created_at DESC)");
    await pool.query("ALTER TABLE levels ADD COLUMN IF NOT EXISTS list_position INTEGER");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_list_idx ON levels (list_position) WHERE list_position IS NOT NULL");
    await pool.query("CREATE TABLE IF NOT EXISTS app_meta (key TEXT PRIMARY KEY, value TEXT)");
    // v3 launch: everyone starts fresh (runs exactly once)
    const reset = await pool.query("INSERT INTO app_meta (key, value) VALUES ('v3_stats_reset', NOW()::text) ON CONFLICT DO NOTHING RETURNING key");
    if (reset.rows.length) {
        await pool.query(`UPDATE users SET games_played = 0, games_completed = 0, total_score = 0, best_score = 0, total_notes_hit = 0, battle_wins = 0, battle_losses = 0`);
        await pool.query("DELETE FROM level_completions");
        console.log("v3: all player statistics were reset");
    }
    // levels you had already featured before v3 become rated levels worth 1-5 stars by difficulty
    await pool.query(`UPDATE levels SET rated_stars = CASE difficulty WHEN 'Easy' THEN 1 WHEN 'Normal' THEN 2 WHEN 'Hard' THEN 3 WHEN 'Insane' THEN 4 WHEN 'Extreme' THEN 5 ELSE 2 END WHERE featured = TRUE AND rated_stars = 0`);
    await pool.query("CREATE INDEX IF NOT EXISTS levels_rated_idx ON levels (rated_stars) WHERE featured = TRUE");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_created_idx ON levels (created_at DESC)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_plays_idx ON levels (plays DESC)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_author_idx ON levels (LOWER(author))");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_author_id_idx ON levels (author_id)");
    await pool.query("CREATE INDEX IF NOT EXISTS levels_featured_idx ON levels (featured) WHERE featured = TRUE");
}

function createToken(user) {
    return jwt.sign({ userId: user.id, username: user.username }, JWT_SECRET, { expiresIn: "30d" });
}

const DIFF_BY_RANK = [null, "Easy", "Normal", "Hard", "Insane", "Extreme"];
// The level LIST (admin-ranked, #1 = hardest): beating the level at position p is worth EXTREME POINTS.
//   #1 = 250 pts, each step down is worth 6% less, never below 5.
const LIST_POINTS_SQL = "GREATEST(5, CEIL(250 * POWER(0.94, l.list_position - 1)))::int";
const DIFF_RANK_SQL = "CASE l.difficulty WHEN 'Easy' THEN 1 WHEN 'Normal' THEN 2 WHEN 'Hard' THEN 3 WHEN 'Insane' THEN 4 WHEN 'Extreme' THEN 5 ELSE 0 END";

// Everything about a player in one SELECT (alias the table as "u").
// stars / difficulty beaten come from RATED (admin-featured) levels you completed;
// creator points = how many of YOUR levels are rated.
const USER_FULL = `u.id, u.username, u.email, u.profile_icon, u.created_at, u.games_played, u.games_completed,
    u.total_score, u.best_score, u.total_notes_hit, u.battle_wins, u.battle_losses,
    u.banned_until, u.ban_reason, u.pending_warning,
    COALESCE((SELECT SUM(l.rated_stars) FROM level_completions c JOIN levels l ON l.id = c.level_id WHERE c.user_id = u.id AND l.featured), 0)::int AS stars,
    (SELECT COUNT(*) FROM levels l WHERE l.author_id = u.id AND l.featured)::int AS creator_points,
    COALESCE((SELECT SUM(${LIST_POINTS_SQL}) FROM level_completions c JOIN levels l ON l.id = c.level_id WHERE c.user_id = u.id AND l.list_position IS NOT NULL), 0)::int AS extreme_points,
    (SELECT COUNT(*) FROM level_completions c JOIN levels l ON l.id = c.level_id WHERE c.user_id = u.id AND l.list_position IS NOT NULL)::int AS list_beaten,
    COALESCE((SELECT MAX(${DIFF_RANK_SQL}) FROM level_completions c JOIN levels l ON l.id = c.level_id WHERE c.user_id = u.id AND l.featured), 0)::int AS hardest_rank,
    COALESCE((SELECT jsonb_object_agg(d, n) FROM (SELECT l.difficulty AS d, COUNT(*)::int AS n FROM level_completions c JOIN levels l ON l.id = c.level_id WHERE c.user_id = u.id AND l.featured GROUP BY l.difficulty) x), '{}'::jsonb) AS difficulty_counts`;

async function fetchUser(id) {
    const r = await pool.query(`SELECT ${USER_FULL} FROM users u WHERE u.id = $1 LIMIT 1`, [id]);
    return r.rows[0] || null;
}

const isBanned = user => !!(user && user.banned_until && new Date(user.banned_until).getTime() > Date.now());

function publicUser(user, includeEmail = true) {
    const out = {
        id: user.id,
        username: user.username,
        email: includeEmail ? user.email : undefined,
        createdAt: user.created_at,
        profileIcon: user.profile_icon || null,
        isAdmin: isAdminName(user.username),
        stars: user.stars || 0,
        creatorPoints: user.creator_points || 0,
        extremePoints: user.extreme_points || 0,
        listBeaten: user.list_beaten || 0,
        hardestDifficulty: DIFF_BY_RANK[user.hardest_rank || 0] || null,
        hardestRank: user.hardest_rank || 0,
        difficultyCounts: user.difficulty_counts || {},
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
    // only ever sent to the player themselves (includeEmail doubles as "this is me")
    if (includeEmail) out.warning = user.pending_warning || null;
    return out;
}

// Summaries leave out the heavy note data / effect images so Browse stays fast.
const LEVEL_SUMMARY_COLUMNS = `id, name, author_id, author, icon, lives, fps, audio_offset, disable_holds, difficulty,
    plays, rating_total, rating_count, featured, rated_stars, list_position, created_at, meta,
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
        ratedStars: row.featured ? (row.rated_stars || 0) : 0,
        listPosition: row.list_position || null,
        listPoints: row.list_position ? Math.max(5, Math.ceil(250 * Math.pow(0.94, row.list_position - 1))) : 0,
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

// Ban status is cached for a few seconds so every request doesn't hit the database.
const banCache = new Map();
async function getBan(userId) {
    const hit = banCache.get(userId);
    if (hit && hit.exp > Date.now()) return hit.value;
    const r = await pool.query("SELECT banned_until, ban_reason FROM users WHERE id = $1", [userId]);
    let value;
    if (!r.rows.length) value = { missing: true };
    else if (isBanned(r.rows[0])) value = { until: r.rows[0].banned_until, reason: r.rows[0].ban_reason || "" };
    else value = null;
    banCache.set(userId, { value, exp: Date.now() + 8000 });
    return value;
}
function banPayload(ban) {
    const permanent = new Date(ban.until).getFullYear() >= 9000;
    return {
        success: false, banned: true, permanent, bannedUntil: ban.until, reason: ban.reason,
        message: permanent ? "You have been permanently banned." : "You are banned until " + new Date(ban.until).toUTCString() + "."
    };
}

function authenticate(req, res, next) {
    const [scheme, token] = (req.headers.authorization || "").split(" ");
    if (scheme !== "Bearer" || !token) return res.status(401).json({ success: false, message: "Authentication required" });
    let payload;
    try { payload = jwt.verify(token, JWT_SECRET); }
    catch { return res.status(401).json({ success: false, message: "Invalid or expired session" }); }
    getBan(payload.userId).then(ban => {
        if (ban && ban.missing) return res.status(401).json({ success: false, message: "Account not found" });
        if (ban) return res.status(403).json(banPayload(ban));
        req.auth = payload;
        next();
    }).catch(error => {
        console.error("Auth check error:", error);
        res.status(500).json({ success: false, message: "Could not verify your session" });
    });
}

function requireAdmin(req, res, next) {
    authenticate(req, res, () => {
        if (!isAdminName(req.auth.username)) return res.status(403).json({ success: false, message: "Admin access required" });
        next();
    });
}

// ---------------------------------------------------------------------------
// health + auth
// ---------------------------------------------------------------------------
app.get("/", (req, res) => res.json({ success: true, message: "06-Tiles Backend is running", version: 3, realtime: true }));

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
        const inserted = await pool.query("INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id", [username, email, passwordHash]);
        const user = await fetchUser(inserted.rows[0].id);
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
        const result = await pool.query("SELECT id, password_hash, banned_until, ban_reason FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1) LIMIT 1", [usernameOrEmail]);
        if (result.rows.length === 0) return res.status(401).json({ success: false, message: "Invalid username/email or password" });
        const row = result.rows[0];
        if (!(await bcrypt.compare(password, row.password_hash))) return res.status(401).json({ success: false, message: "Invalid username/email or password" });
        if (isBanned(row)) return res.status(403).json(banPayload({ until: row.banned_until, reason: row.ban_reason || "" }));
        const user = await fetchUser(row.id);
        res.json({ success: true, message: "Logged in successfully", token: createToken(user), user: publicUser(user) });
    } catch (error) {
        console.error("Login error:", error);
        res.status(500).json({ success: false, message: "Could not log in" });
    }
});

app.get("/api/auth/me", authenticate, async (req, res) => {
    try {
        const user = await fetchUser(req.auth.userId);
        if (!user) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(user) });
    } catch (error) {
        console.error("Account lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load account" });
    }
});

// The player pressed "I understand" on a warning screen.
app.post("/api/auth/warning/ack", authenticate, async (req, res) => {
    try {
        await pool.query("UPDATE users SET pending_warning = NULL WHERE id = $1", [req.auth.userId]);
        res.json({ success: true });
    } catch (error) {
        console.error("Warning ack error:", error);
        res.status(500).json({ success: false, message: "Could not clear the warning" });
    }
});

app.patch("/api/profile", authenticate, async (req, res) => {
    try {
        const icon = req.body.profileIcon ? String(req.body.profileIcon) : null;
        if (icon && (!icon.startsWith("data:image/") || icon.length > 400000)) return res.status(400).json({ success: false, message: "Profile icon is too large" });
        const result = await pool.query("UPDATE users SET profile_icon = $2, updated_at = NOW() WHERE id = $1 RETURNING id", [req.auth.userId, icon]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Account not found" });
        res.json({ success: true, user: publicUser(await fetchUser(req.auth.userId)) });
    } catch (error) {
        console.error("Profile update error:", error);
        res.status(500).json({ success: false, message: "Could not update profile" });
    }
});

// Stats only count games played on levels from the Browse > Levels tab, and never on
// your own levels. Completing an admin-rated level also records the completion that
// powers stars + "difficulty beaten".
app.post("/api/stats/game", authenticate, async (req, res) => {
    try {
        const me = req.auth.userId;
        const levelId = String(req.body.levelId || "");
        let counted = false;
        if (isUuid(levelId)) {
            const lvl = await pool.query("SELECT author_id FROM levels WHERE id = $1", [levelId]);
            counted = lvl.rows.length > 0 && lvl.rows[0].author_id !== me;
        }
        if (counted) {
            const completed = !!req.body.completed;
            const score = Math.max(0, Math.min(1e9, Math.floor(Number(req.body.score) || 0)));
            const notesHit = Math.max(0, Math.min(1e6, Math.floor(Number(req.body.notesHit) || 0)));
            await pool.query(
                `UPDATE users SET games_played = games_played + 1, games_completed = games_completed + $2,
                    total_score = total_score + $3, best_score = GREATEST(best_score, $3),
                    total_notes_hit = total_notes_hit + $4, updated_at = NOW()
                 WHERE id = $1`,
                [me, completed ? 1 : 0, score, notesHit]
            );
            if (completed) {
                await pool.query(
                    `INSERT INTO level_completions (user_id, level_id, best_score) VALUES ($1, $2, $3)
                     ON CONFLICT (user_id, level_id) DO UPDATE SET best_score = GREATEST(level_completions.best_score, EXCLUDED.best_score), completed_at = NOW()`,
                    [me, levelId, score]
                );
            }
        }
        res.json({ success: true, counted, user: publicUser(await fetchUser(me)) });
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
            featured: "rated_stars DESC, created_at DESC"
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

// Publishing requires an account.
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
// A 5-star rating from someone other than the author drops a notification in the admin panel.
app.post("/api/levels/:id/rate", authenticate, async (req, res) => {
    const client = await pool.connect();
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const stars = clampInt(req.body.stars, 1, 5, 0);
        if (!stars) return res.status(400).json({ success: false, message: "Pick 1 to 5 stars" });
        await client.query("BEGIN");
        const level = await client.query("SELECT id, name, author, author_id, featured FROM levels WHERE id = $1 FOR UPDATE", [req.params.id]);
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
        const lv = level.rows[0];
        if (stars === 5 && lv.author_id !== req.auth.userId && !lv.featured) {
            await client.query(
                `INSERT INTO admin_notifications (type, level_id, user_id, message) VALUES ('five_star', $1, $2, $3)
                 ON CONFLICT (type, level_id, user_id) DO UPDATE SET read = FALSE, created_at = NOW()`,
                [req.params.id, req.auth.userId, req.auth.username + " gave \"" + lv.name + "\" by " + lv.author + " 5 stars"]
            );
        }
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

// Update one of YOUR published levels in place (ratings, plays, rated status and completions are kept).
// Rated levels also ping the admin so they can re-check the change.
app.put("/api/levels/:id", authenticate, rateLimit("publish", 40, 60 * 60 * 1000), async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const clean = sanitizeLevel(req.body);
        if (clean.error) return res.status(400).json({ success: false, message: clean.error });
        const result = await pool.query(
            `UPDATE levels SET name=$3, icon=$4, level_data=$5::jsonb, effects=$6::jsonb, lives=$7, fps=$8, audio_offset=$9,
                disable_holds=$10, difficulty=$11, meta=$12::jsonb, updated_at=NOW()
             WHERE id=$1 AND author_id=$2 RETURNING *`,
            [req.params.id, req.auth.userId, clean.name, clean.icon, JSON.stringify(clean.notes), JSON.stringify(clean.effects),
             clean.lives, clean.fps, clean.audioOffset, clean.disableHolds, clean.difficulty, JSON.stringify(clean.meta)]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found or you do not own it" });
        const row = result.rows[0];
        if (row.featured || row.list_position) {
            await pool.query(
                `INSERT INTO admin_notifications (type, level_id, user_id, message) VALUES ('level_updated', $1, $2, $3)
                 ON CONFLICT (type, level_id, user_id) DO UPDATE SET read = FALSE, created_at = NOW(), message = EXCLUDED.message`,
                [row.id, req.auth.userId, req.auth.username + " updated the rated level \"" + row.name + "\""]
            );
        }
        res.json({ success: true, level: publicLevel(row) });
    } catch (error) {
        console.error("Level update error:", error);
        res.status(500).json({ success: false, message: "Could not update level" });
    }
});

// The LIST: admin-ranked levels, #1 first.
app.get("/api/list", async (req, res) => {
    try {
        const result = await pool.query(`SELECT ${LEVEL_SUMMARY_COLUMNS} FROM levels WHERE list_position IS NOT NULL ORDER BY list_position ASC LIMIT 200`);
        res.json(result.rows.map(r => publicLevel(r)));
    } catch (error) {
        console.error("List error:", error);
        res.status(500).json({ success: false, message: "Could not load the list" });
    }
});

// ---------------------------------------------------------------------------
// admin
// ---------------------------------------------------------------------------
app.get("/api/admin/check", authenticate, (req, res) => res.json({ success: true, isAdmin: isAdminName(req.auth.username) }));

app.get("/api/admin/overview", requireAdmin, async (req, res) => {
    try {
        const [users, levels, plays, notes, banned] = await Promise.all([
            pool.query("SELECT COUNT(*)::int AS n FROM users"),
            pool.query("SELECT COUNT(*)::int AS n, COUNT(*) FILTER (WHERE featured)::int AS rated FROM levels"),
            pool.query("SELECT COALESCE(SUM(plays),0)::bigint AS n FROM levels"),
            pool.query("SELECT COUNT(*)::int AS n FROM admin_notifications WHERE read = FALSE"),
            pool.query("SELECT COUNT(*)::int AS n FROM users WHERE banned_until > NOW()")
        ]);
        res.json({ success: true, users: users.rows[0].n, levels: levels.rows[0].n, rated: levels.rows[0].rated, featured: levels.rows[0].rated,
            plays: Number(plays.rows[0].n), onlineNow: lobby.size, unreadNotifications: notes.rows[0].n, banned: banned.rows[0].n });
    } catch (error) {
        console.error("Admin overview error:", error);
        res.status(500).json({ success: false, message: "Could not load overview" });
    }
});

// --- notifications (5-star ratings etc.) ---
app.get("/api/admin/notifications", requireAdmin, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT n.id, n.type, n.level_id, n.message, n.read, n.created_at, l.name AS level_name, l.author AS level_author, l.featured AS level_rated
             FROM admin_notifications n LEFT JOIN levels l ON l.id = n.level_id
             ORDER BY n.read ASC, n.created_at DESC LIMIT 50`
        );
        res.json({ success: true, notifications: result.rows.map(r => ({
            id: Number(r.id), type: r.type, levelId: r.level_id, message: r.message, read: r.read, createdAt: r.created_at,
            levelName: r.level_name, levelAuthor: r.level_author, levelRated: !!r.level_rated
        })) });
    } catch (error) {
        console.error("Admin notifications error:", error);
        res.status(500).json({ success: false, message: "Could not load notifications" });
    }
});
app.post("/api/admin/notifications/read", requireAdmin, async (req, res) => {
    try {
        if (req.body.id) await pool.query("UPDATE admin_notifications SET read = TRUE WHERE id = $1", [clampInt(req.body.id, 1, 9e15, 0)]);
        else await pool.query("UPDATE admin_notifications SET read = TRUE WHERE read = FALSE");
        res.json({ success: true });
    } catch (error) {
        console.error("Admin notifications read error:", error);
        res.status(500).json({ success: false, message: "Could not update notifications" });
    }
});

// --- levels ---
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

// Rate a level: rated=true makes it a rated (featured) level worth `stars` stars with the given difficulty.
// rated=false removes the rating (completions stay, but stop counting until it is rated again).
app.patch("/api/admin/levels/:id/rate", requireAdmin, async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const rated = req.body.rated !== undefined ? !!req.body.rated : !!req.body.featured;
        const difficulty = DIFFICULTIES.includes(req.body.difficulty) ? req.body.difficulty : null;
        let stars = clampInt(req.body.stars, 0, 10, 0);
        if (rated && !stars) stars = Math.max(1, DIFFICULTIES.indexOf(difficulty || "Normal") + 1);
        const result = await pool.query(
            `UPDATE levels SET featured = $2, rated_stars = $3, difficulty = COALESCE($4, difficulty), updated_at = NOW()
             WHERE id = $1 RETURNING ${LEVEL_SUMMARY_COLUMNS}`,
            [req.params.id, rated, rated ? stars : 0, difficulty]
        );
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        if (rated) await pool.query("UPDATE admin_notifications SET read = TRUE WHERE level_id = $1", [req.params.id]);
        res.json({ success: true, level: publicLevel(result.rows[0]) });
    } catch (error) {
        console.error("Admin rate error:", error);
        res.status(500).json({ success: false, message: "Could not update the level's rating" });
    }
});
// Put a level on the list at a position (1 = top), or take it off with position: null. Others shift to make room.
app.patch("/api/admin/levels/:id/list", requireAdmin, async (req, res) => {
    const client = await pool.connect();
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        await client.query("BEGIN");
        await client.query("LOCK TABLE levels IN SHARE ROW EXCLUSIVE MODE");
        const cur = await client.query("SELECT list_position FROM levels WHERE id = $1 FOR UPDATE", [req.params.id]);
        if (!cur.rows.length) { await client.query("ROLLBACK"); return res.status(404).json({ success: false, message: "Level not found" }); }
        const old = cur.rows[0].list_position;
        if (old) {   // lift it out and close the gap
            await client.query("UPDATE levels SET list_position = NULL WHERE id = $1", [req.params.id]);
            await client.query("UPDATE levels SET list_position = list_position - 1 WHERE list_position > $1", [old]);
        }
        if (req.body.position !== null && req.body.position !== undefined && req.body.position !== "") {
            const count = (await client.query("SELECT COUNT(*)::int AS n FROM levels WHERE list_position IS NOT NULL")).rows[0].n;
            const pos = clampInt(req.body.position, 1, count + 1, count + 1);
            await client.query("UPDATE levels SET list_position = list_position + 1 WHERE list_position >= $1", [pos]);
            await client.query("UPDATE levels SET list_position = $2, updated_at = NOW() WHERE id = $1", [req.params.id, pos]);
        }
        await client.query("COMMIT");
        res.json({ success: true });
    } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        console.error("Admin list error:", error);
        res.status(500).json({ success: false, message: "Could not update the list" });
    } finally { client.release(); }
});

app.delete("/api/admin/levels/:id", requireAdmin, async (req, res) => {
    try {
        if (!isUuid(req.params.id)) return res.status(404).json({ success: false, message: "Level not found" });
        const result = await pool.query("DELETE FROM levels WHERE id = $1 RETURNING id, list_position", [req.params.id]);
        if (!result.rows.length) return res.status(404).json({ success: false, message: "Level not found" });
        if (result.rows[0].list_position) await pool.query("UPDATE levels SET list_position = list_position - 1 WHERE list_position > $1", [result.rows[0].list_position]);
        res.json({ success: true });
    } catch (error) {
        console.error("Admin delete error:", error);
        res.status(500).json({ success: false, message: "Could not delete level" });
    }
});

// --- players: view, ban (timed), unban, remove stats, warn ---
function adminPlayer(u) {
    return {
        id: u.id, username: u.username, email: u.email, profileIcon: u.profile_icon || null, createdAt: u.created_at,
        isAdmin: isAdminName(u.username), banned: isBanned(u), bannedUntil: isBanned(u) ? u.banned_until : null,
        permanent: isBanned(u) && new Date(u.banned_until).getFullYear() >= 9000, banReason: isBanned(u) ? (u.ban_reason || "") : "",
        pendingWarning: u.pending_warning || null, levelCount: u.level_count || 0, difficultyCounts: u.difficulty_counts || {},
        stars: u.stars || 0, creatorPoints: u.creator_points || 0, extremePoints: u.extreme_points || 0,
        hardestDifficulty: DIFF_BY_RANK[u.hardest_rank || 0] || null,
        statistics: {
            gamesPlayed: u.games_played, gamesCompleted: u.games_completed, totalScore: Number(u.total_score), bestScore: Number(u.best_score),
            totalNotesHit: Number(u.total_notes_hit), battleWins: u.battle_wins, battleLosses: u.battle_losses
        }
    };
}

app.get("/api/admin/players", requireAdmin, async (req, res) => {
    try {
        const search = String(req.query.search || "").trim().slice(0, 30);
        const values = [];
        let where = "";
        if (search) { values.push("%" + search.toLowerCase().replace(/[\\%_]/g, m => "\\" + m) + "%"); where = "WHERE LOWER(u.username) LIKE $1 OR LOWER(u.email) LIKE $1"; }
        const result = await pool.query(
            `SELECT ${USER_FULL}, (SELECT COUNT(*)::int FROM levels l2 WHERE l2.author_id = u.id) AS level_count
             FROM users u ${where} ORDER BY (u.banned_until > NOW()) DESC NULLS LAST, u.created_at DESC LIMIT 100`, values);
        res.json({ success: true, players: result.rows.map(adminPlayer) });
    } catch (error) {
        console.error("Admin players error:", error);
        res.status(500).json({ success: false, message: "Could not load players" });
    }
});

async function loadTarget(req, res) {
    if (!isUuid(req.params.id)) { res.status(404).json({ success: false, message: "Player not found" }); return null; }
    const r = await pool.query("SELECT id, username FROM users WHERE id = $1", [req.params.id]);
    if (!r.rows.length) { res.status(404).json({ success: false, message: "Player not found" }); return null; }
    return r.rows[0];
}
function socketsOf(userId) { return [...lobby.values()].filter(s => s.data && s.data.userId === userId); }

app.post("/api/admin/players/:id/ban", requireAdmin, async (req, res) => {
    try {
        const target = await loadTarget(req, res); if (!target) return;
        if (isAdminName(target.username)) return res.status(400).json({ success: false, message: "You can't ban an admin." });
        const reason = String(req.body.reason || "").trim().slice(0, 300);
        const permanent = !!req.body.permanent;
        const minutes = clampInt(req.body.minutes, 1, 5256000, 0);
        if (!permanent && !minutes) return res.status(400).json({ success: false, message: "Pick how long the ban lasts." });
        const result = permanent
            ? await pool.query("UPDATE users SET banned_until = '9999-12-31T00:00:00Z', ban_reason = $2 WHERE id = $1 RETURNING banned_until, ban_reason", [target.id, reason])
            : await pool.query("UPDATE users SET banned_until = NOW() + make_interval(mins => $2), ban_reason = $3 WHERE id = $1 RETURNING banned_until, ban_reason", [target.id, minutes, reason]);
        banCache.delete(target.id);
        const payload = banPayload({ until: result.rows[0].banned_until, reason });
        socketsOf(target.id).forEach(s => { s.emit("banned", payload); s.disconnect(true); });
        res.json({ success: true, bannedUntil: result.rows[0].banned_until, permanent });
    } catch (error) {
        console.error("Admin ban error:", error);
        res.status(500).json({ success: false, message: "Could not ban that player" });
    }
});

app.post("/api/admin/players/:id/unban", requireAdmin, async (req, res) => {
    try {
        const target = await loadTarget(req, res); if (!target) return;
        await pool.query("UPDATE users SET banned_until = NULL, ban_reason = NULL WHERE id = $1", [target.id]);
        banCache.delete(target.id);
        res.json({ success: true });
    } catch (error) {
        console.error("Admin unban error:", error);
        res.status(500).json({ success: false, message: "Could not unban that player" });
    }
});

// Wipes games/score/notes/battle stats AND level completions (so stars + difficulty beaten reset too).
// Creator points come from the player's rated levels, so those are untouched; delete the levels to remove them.
app.post("/api/admin/players/:id/reset-stats", requireAdmin, async (req, res) => {
    try {
        const target = await loadTarget(req, res); if (!target) return;
        await pool.query(
            `UPDATE users SET games_played = 0, games_completed = 0, total_score = 0, best_score = 0, total_notes_hit = 0,
                battle_wins = 0, battle_losses = 0, updated_at = NOW() WHERE id = $1`, [target.id]);
        await pool.query("DELETE FROM level_completions WHERE user_id = $1", [target.id]);
        res.json({ success: true });
    } catch (error) {
        console.error("Admin reset stats error:", error);
        res.status(500).json({ success: false, message: "Could not remove that player's stats" });
    }
});

// Shows a full-screen warning on the player's screen (instantly if they're connected to the battle
// server, otherwise within ~half a minute while they have the game open) until they dismiss it.
app.post("/api/admin/players/:id/warn", requireAdmin, async (req, res) => {
    try {
        const target = await loadTarget(req, res); if (!target) return;
        const message = String(req.body.message || "").trim().slice(0, 600);
        if (!message) return res.status(400).json({ success: false, message: "Write the warning text first." });
        await pool.query("UPDATE users SET pending_warning = $2, warning_at = NOW() WHERE id = $1", [target.id, message]);
        socketsOf(target.id).forEach(s => s.emit("warning", { message }));
        res.json({ success: true });
    } catch (error) {
        console.error("Admin warn error:", error);
        res.status(500).json({ success: false, message: "Could not send the warning" });
    }
});

// ---------------------------------------------------------------------------
// players + leaderboards
// ---------------------------------------------------------------------------
const NOT_BANNED = "(u.banned_until IS NULL OR u.banned_until < NOW())";

app.get("/api/players", async (req, res) => {
    try {
        const search = String(req.query.search || "").trim().slice(0, 30);
        const values = [];
        const where = [NOT_BANNED];
        if (search) { values.push("%" + search.toLowerCase().replace(/[\\%_]/g, m => "\\" + m) + "%"); where.push("LOWER(u.username) LIKE $1"); }
        const result = await pool.query(`SELECT ${USER_FULL} FROM users u WHERE ${where.join(" AND ")} ORDER BY games_played DESC, username ASC LIMIT 100`, values);
        res.json(result.rows.map(u => publicUser(u, false)));
    } catch (error) {
        console.error("Players lookup error:", error);
        res.status(500).json({ success: false, message: "Could not load players" });
    }
});

app.get("/api/leaderboards", async (req, res) => {
    try {
        const orderMap = {
            points: "extreme_points DESC, stars DESC, username ASC",
            stars: "stars DESC, hardest_rank DESC, username ASC",
            difficulty: "hardest_rank DESC, stars DESC, username ASC",
            creator: "creator_points DESC, stars DESC, username ASC",
            bestScore: "best_score DESC, username ASC",
            totalScore: "total_score DESC, username ASC",
            notesHit: "total_notes_hit DESC, username ASC",
            gamesPlayed: "games_played DESC, username ASC",
            battleWins: "battle_wins DESC, username ASC"
        };
        const order = orderMap[String(req.query.sort || "points")] || orderMap.points;
        const result = await pool.query(`SELECT ${USER_FULL} FROM users u WHERE ${NOT_BANNED} ORDER BY ${order} LIMIT 100`);
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

io.use(async (socket, next) => {
    const auth = socket.handshake.auth || {};
    let name = null, userId = null;
    if (auth.token) {
        try { const p = jwt.verify(String(auth.token), JWT_SECRET); name = p.username; userId = p.userId; } catch {}
    }
    if (userId) {
        try {
            const ban = await getBan(userId);
            if (ban && ban.missing) { name = null; userId = null; }
            else if (ban) return next(new Error("banned"));
        } catch (error) { console.error("Socket ban check error:", error); }
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

// Battles only use RATED (admin-featured) levels.
async function pickRandomLevel() {
    const result = await pool.query(
        `SELECT * FROM levels
         WHERE featured = TRUE AND jsonb_typeof(level_data) = 'array' AND jsonb_array_length(level_data) >= 8
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
        icon: level.icon || null, ratedStars: level.ratedStars || 0
    };
}

// First to WIN_TARGET round wins takes the match. Each round is a race on the same level:
// the LAST PERSON STANDING wins the round (if you die, your opponent wins it on the spot).
// If both of you finish the level, the higher score wins; equal scores replay the round.
const WIN_TARGET = 3;
const ROUND_PAUSE_MS = 4000;

function createMatch(socketA, socketB, level) {
    const id = newId("m");
    const player = s => ({ sid: s.id, name: s.data.name, userId: s.data.userId, ready: false, result: null, rounds: 0, rematch: false, left: false });
    const match = { id, level: levelForMatch(level), players: { [socketA.id]: player(socketA), [socketB.id]: player(socketB) },
        started: false, done: false, round: 1, roundDone: false, readyTimer: null, nextTimer: null };
    matches.set(id, match);
    setStatus(socketA, "match", id);
    setStatus(socketB, "match", id);
    [[socketA, socketB], [socketB, socketA]].forEach(([me, opp]) => {
        me.emit("match_found", { matchId: id, opponent: { name: opp.data.name }, level: match.level, target: WIN_TARGET });
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
    clearTimeout(match.nextTimer);
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

function startRound(match) {
    if (match.done) return;
    match.started = true;
    match.roundDone = false;
    Object.values(match.players).forEach(p => {
        p.result = null;
        const opp = otherOf(match, p.sid);
        const s = lobby.get(p.sid);
        if (s) s.emit("match_go", { matchId: match.id, countdown: 3, round: match.round, target: WIN_TARGET, you: p.rounds, opp: opp.rounds });
    });
}

// winnerSid === null means a tie: nobody scores and the round is replayed.
function endRound(match, winnerSid) {
    if (match.roundDone || match.done) return;
    match.roundDone = true;
    const winner = winnerSid ? match.players[winnerSid] : null;
    if (winner) winner.rounds++;
    if (winner && winner.rounds >= WIN_TARGET) return finishMatch(match, winnerSid);
    const finishedRound = match.round;
    match.round++;
    Object.values(match.players).forEach(p => {
        const opp = otherOf(match, p.sid);
        const s = lobby.get(p.sid);
        if (s) s.emit("round_end", {
            matchId: match.id, round: finishedRound, target: WIN_TARGET,
            outcome: !winner ? "draw" : winner.sid === p.sid ? "win" : "lose",
            you: p.rounds, opp: opp.rounds,
            yourScore: p.result ? p.result.score : 0, oppScore: opp.result ? opp.result.score : 0,
            nextInMs: ROUND_PAUSE_MS
        });
    });
    clearTimeout(match.nextTimer);
    match.nextTimer = setTimeout(() => startRound(match), ROUND_PAUSE_MS);
}

function finishMatch(match, winnerSid, forfeitBy = null) {
    if (match.done) return;
    match.done = true;
    clearTimeout(match.readyTimer);
    clearTimeout(match.nextTimer);
    const [a, b] = Object.values(match.players);
    const send = (me, opp) => {
        const s = lobby.get(me.sid);
        if (s) {
            s.emit("match_result", {
                matchId: match.id, outcome: me.sid === winnerSid ? "win" : "lose", forfeit: !!forfeitBy && forfeitBy !== me.sid, target: WIN_TARGET,
                you: { rounds: me.rounds, score: me.result ? me.result.score : 0 }, opp: { name: opp.name, rounds: opp.rounds, score: opp.result ? opp.result.score : 0 }
            });
            setStatus(s, "lobby");
        }
    };
    send(a, b);
    send(b, a);
    const w = match.players[winnerSid], l = otherOf(match, winnerSid);
    applyBattleStats(w && w.userId, l && l.userId);
    // keep the match around briefly so a rematch can reuse the level
    setTimeout(() => matches.delete(match.id), 10 * 60 * 1000).unref();
}

function forfeit(match, sid) {
    if (match.done) return;
    const me = match.players[sid];
    const opp = otherOf(match, sid);
    me.left = true;
    const oppSocket = opp && lobby.get(opp.sid);
    if (oppSocket) oppSocket.emit("opp_left", { name: me.name });
    finishMatch(match, opp.sid, sid);
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
                [a, b].forEach(s => { s.emit("queue_error", { message: "No rated levels exist yet. Ask the admin to rate some levels!" }); setStatus(s, "lobby"); });
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
    socket.on("challenge_send", async payload => {
        try {
            const target = lobby.get(String(payload && payload.toId));
            if (!target || target.id === socket.id) return socket.emit("challenge_failed", { message: "That player is no longer online." });
            const levelId = String(payload && payload.levelId || "");
            if (!isUuid(levelId)) return socket.emit("challenge_failed", { message: "Pick a rated level to battle on." });
            const row = await pool.query("SELECT * FROM levels WHERE id = $1 AND featured = TRUE", [levelId]);
            if (!row.rows.length) return socket.emit("challenge_failed", { message: "Battles can only use rated levels." });
            // re-check after the database round trip
            if (!lobby.has(target.id) || target.data.status !== "lobby" || socket.data.status !== "lobby") return socket.emit("challenge_failed", { message: "That player is busy right now." });
            const level = publicLevel(row.rows[0], true);
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
            target.emit("challenge_received", { challengeId: id, from: { name: socket.data.name }, target: WIN_TARGET,
                level: { name: level.name, tileCount: level.data.length, difficulty: level.difficulty, icon: level.icon, ratedStars: level.ratedStars } });
        } catch (error) {
            console.error("Challenge error:", error);
            socket.emit("challenge_failed", { message: "Could not send the challenge." });
        }
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
            clearTimeout(match.readyTimer);
            startRound(match);
        }
    });
    socket.on("match_progress", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || match.done || match.roundDone) return;
        const opp = otherOf(match, socket.id);
        const s = opp && lobby.get(opp.sid);
        if (s) s.volatile.emit("opp_progress", { pct: clampInt(payload.pct, 0, 100), score: clampInt(payload.score, 0, 1e9) });
    });
    socket.on("match_finish", payload => {
        const match = matches.get(String(payload && payload.matchId));
        const me = match && match.players[socket.id];
        if (!me || match.done || match.roundDone || !match.started || me.result) return;
        me.result = { finished: !!payload.finished, score: clampInt(payload.score, 0, 1e9), reason: String(payload.reason || "").slice(0, 30) };
        const opp = otherOf(match, socket.id);
        const s = opp && lobby.get(opp.sid);
        if (s) s.emit("opp_finish", me.result);
        // knocked out -> the other player is the last one standing
        if (!me.result.finished) return endRound(match, opp.sid);
        // finished while the opponent is still racing: wait for them (they win the round only by finishing with a higher score)
        if (!opp.result) return;
        if (me.result.score > opp.result.score) endRound(match, me.sid);
        else if (me.result.score < opp.result.score) endRound(match, opp.sid);
        else endRound(match, null);
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
        await seedOfficialLevels();
        server.listen(PORT, () => {
            console.log(`06-Tiles Backend v3 running on port ${PORT}`);
            console.log("PostgreSQL database initialized; Socket.IO battle server ready");
        });
    } catch (error) {
        console.error("Database initialization failed:", error);
        process.exit(1);
    }
}

startServer();
