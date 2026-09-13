const express = require("express");
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const cors = require("cors");
const dotenv = require("dotenv");

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

const JWT_SECRET = process.env.JWT_SECRET;

mongoose.connect(process.env.MONGODB_URI)
    .then(() => {
        console.log("Connected to MongoDB");
    })
    .catch((error) => {
        console.error("MongoDB connection error:", error);
    });

const userSchema = new mongoose.Schema(
    {
        username: {
            type: String,
            required: true,
            unique: true,
            trim: true,
            minlength: 3,
            maxlength: 20
        },

        email: {
            type: String,
            required: true,
            unique: true,
            lowercase: true,
            trim: true
        },

        password: {
            type: String,
            required: true
        },

        createdAt: {
            type: Date,
            default: Date.now
        },

        statistics: {
            gamesPlayed: {
                type: Number,
                default: 0
            },

            gamesCompleted: {
                type: Number,
                default: 0
            },

            totalScore: {
                type: Number,
                default: 0
            },

            bestScore: {
                type: Number,
                default: 0
            },

            totalNotesHit: {
                type: Number,
                default: 0
            },

            battleWins: {
                type: Number,
                default: 0
            },

            battleLosses: {
                type: Number,
                default: 0
            }
        }
    },
    {
        versionKey: false
    }
);

const User = mongoose.model("User", userSchema);

app.get("/", (req, res) => {
    res.json({
        success: true,
        message: "06-Tiles Backend is running"
    });
});

app.post("/api/auth/register", async (req, res) => {
    try {
        const { username, email, password } = req.body;

        if (!username || !email || !password) {
            return res.status(400).json({
                success: false,
                message: "Username, email, and password are required"
            });
        }

        if (username.length < 3 || username.length > 20) {
            return res.status(400).json({
                success: false,
                message: "Username must be between 3 and 20 characters"
            });
        }

        if (password.length < 6) {
            return res.status(400).json({
                success: false,
                message: "Password must be at least 6 characters"
            });
        }

        const existingUsername = await User.findOne({
            username: username.trim()
        });

        if (existingUsername) {
            return res.status(409).json({
                success: false,
                message: "Username is already taken"
            });
        }

        const existingEmail = await User.findOne({
            email: email.trim().toLowerCase()
        });

        if (existingEmail) {
            return res.status(409).json({
                success: false,
                message: "Email is already registered"
            });
        }

        const hashedPassword = await bcrypt.hash(password, 12);

        const user = await User.create({
            username: username.trim(),
            email: email.trim().toLowerCase(),
            password: hashedPassword
        });

        const token = jwt.sign(
            {
                userId: user._id.toString(),
                username: user.username
            },
            JWT_SECRET,
            {
                expiresIn: "30d"
            }
        );

        res.status(201).json({
            success: true,
            message: "Account created successfully",
            token,
            user: {
                id: user._id,
                username: user.username,
                email: user.email,
                createdAt: user.createdAt,
                statistics: user.statistics
            }
        });
    } catch (error) {
        console.error("Registration error:", error);

        res.status(500).json({
            success: false,
            message: "Could not create account"
        });
    }
});

app.post("/api/auth/login", async (req, res) => {
    try {
        const { usernameOrEmail, password } = req.body;

        if (!usernameOrEmail || !password) {
            return res.status(400).json({
                success: false,
                message: "Username/email and password are required"
            });
        }

        const user = await User.findOne({
            $or: [
                {
                    username: usernameOrEmail.trim()
                },
                {
                    email: usernameOrEmail.trim().toLowerCase()
                }
            ]
        });

        if (!user) {
            return res.status(401).json({
                success: false,
                message: "Invalid username/email or password"
            });
        }

        const passwordMatches = await bcrypt.compare(
            password,
            user.password
        );

        if (!passwordMatches) {
            return res.status(401).json({
                success: false,
                message: "Invalid username/email or password"
            });
        }

        const token = jwt.sign(
            {
                userId: user._id.toString(),
                username: user.username
            },
            JWT_SECRET,
            {
                expiresIn: "30d"
            }
        );

        res.json({
            success: true,
            message: "Logged in successfully",
            token,
            user: {
                id: user._id,
                username: user.username,
                email: user.email,
                createdAt: user.createdAt,
                statistics: user.statistics
            }
        });
    } catch (error) {
        console.error("Login error:", error);

        res.status(500).json({
            success: false,
            message: "Could not log in"
        });
    }
});

app.listen(PORT, () => {
    console.log(`06-Tiles Backend running on port ${PORT}`);
});
