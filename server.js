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

app.use(cors({
    origin: true,
    credentials: true
}));

app.use(express.json({ limit: "1mb" }));

async function initializeDatabase() {
    await pool.query("CREATE EXTENSION IF NOT EXISTS pgcrypto");

    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            username VARCHAR(20) NOT NULL UNIQUE,
            email VARCHAR(254) NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
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

    await pool.query(`
        CREATE INDEX IF NOT EXISTS users_username_lower_idx
        ON users (LOWER(username))
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS users_email_lower_idx
        ON users (LOWER(email))
    `);
}

function createToken(user) {
    return jwt.sign(
        {
            userId: user.id,
            username: user.username
        },
        JWT_SECRET,
        {
            expiresIn: "30d"
        }
    );
}

function publicUser(user) {
    return {
        id: user.id,
        username: user.username,
        email: user.email,
        createdAt: user.created_at,
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
        return res.status(401).json({
            success: false,
            message: "Authentication required"
        });
    }

    try {
        req.auth = jwt.verify(token, JWT_SECRET);
        next();
    } catch {
        return res.status(401).json({
            success: false,
            message: "Invalid or expired session"
        });
    }
}

app.get("/", (req, res) => {
    res.json({
        success: true,
        message: "06-Tiles Backend is running"
    });
});

app.get("/api/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");

        res.json({
            success: true,
            database: "connected"
        });
    } catch (error) {
        console.error("Health check error:", error);

        res.status(503).json({
            success: false,
            database: "disconnected"
        });
    }
});

app.post("/api/auth/register", async (req, res) => {
    try {
        const username = String(req.body.username || "").trim();
        const email = String(req.body.email || "").trim().toLowerCase();
        const password = String(req.body.password || "");

        if (!username || !email || !password) {
            return res.status(400).json({
                success: false,
                message: "Username, email, and password are required"
            });
        }

        if (!validUsername(username)) {
            return res.status(400).json({
                success: false,
                message: "Username must be 3-20 characters and contain only letters, numbers, and underscores"
            });
        }

        if (!validEmail(email) || email.length > 254) {
            return res.status(400).json({
                success: false,
                message: "Please enter a valid email address"
            });
        }

        if (password.length < 8 || password.length > 128) {
            return res.status(400).json({
                success: false,
                message: "Password must be 8-128 characters"
            });
        }

        const existing = await pool.query(
            "SELECT username, email FROM users WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($2) LIMIT 1",
            [username, email]
        );

        if (existing.rows.length > 0) {
            const match = existing.rows[0];

            if (match.username.toLowerCase() === username.toLowerCase()) {
                return res.status(409).json({
                    success: false,
                    message: "That username is already taken"
                });
            }

            return res.status(409).json({
                success: false,
                message: "That email is already registered"
            });
        }

        const passwordHash = await bcrypt.hash(password, 12);

        const result = await pool.query(
            `INSERT INTO users (username, email, password_hash)
             VALUES ($1, $2, $3)
             RETURNING id, username, email, created_at, games_played, games_completed,
                       total_score, best_score, total_notes_hit, battle_wins, battle_losses`,
            [username, email, passwordHash]
        );

        const user = result.rows[0];

        res.status(201).json({
            success: true,
            message: "Account created successfully",
            token: createToken(user),
            user: publicUser(user)
        });
    } catch (error) {
        console.error("Registration error:", error);

        if (error.code === "23505") {
            return res.status(409).json({
                success: false,
                message: "Username or email is already in use"
            });
        }

        res.status(500).json({
            success: false,
            message: "Could not create account"
        });
    }
});

app.post("/api/auth/login", async (req, res) => {
    try {
        const usernameOrEmail = String(req.body.usernameOrEmail || "").trim();
        const password = String(req.body.password || "");

        if (!usernameOrEmail || !password) {
            return res.status(400).json({
                success: false,
                message: "Username/email and password are required"
            });
        }

        const result = await pool.query(
            `SELECT id, username, email, password_hash, created_at, games_played,
                    games_completed, total_score, best_score, total_notes_hit,
                    battle_wins, battle_losses
             FROM users
             WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1)
             LIMIT 1`,
            [usernameOrEmail]
        );

        if (result.rows.length === 0) {
            return res.status(401).json({
                success: false,
                message: "Invalid username/email or password"
            });
        }

        const user = result.rows[0];
        const passwordMatches = await bcrypt.compare(password, user.password_hash);

        if (!passwordMatches) {
            return res.status(401).json({
                success: false,
                message: "Invalid username/email or password"
            });
        }

        res.json({
            success: true,
            message: "Logged in successfully",
            token: createToken(user),
            user: publicUser(user)
        });
    } catch (error) {
        console.error("Login error:", error);

        res.status(500).json({
            success: false,
            message: "Could not log in"
        });
    }
});

app.get("/api/auth/me", authenticate, async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT id, username, email, created_at, games_played, games_completed,
                    total_score, best_score, total_notes_hit, battle_wins, battle_losses
             FROM users
             WHERE id = $1
             LIMIT 1`,
            [req.auth.userId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: "Account not found"
            });
        }

        res.json({
            success: true,
            user: publicUser(result.rows[0])
        });
    } catch (error) {
        console.error("Account lookup error:", error);

        res.status(500).json({
            success: false,
            message: "Could not load account"
        });
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
