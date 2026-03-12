-- Platform database schema for user management and auth

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS app_users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'client',  -- 'admin' or 'client'
    status          TEXT NOT NULL DEFAULT 'pending',  -- 'pending', 'approved', 'disabled'
    business_name   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at     TIMESTAMP,
    last_login_at   TIMESTAMP,
    last_activity_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_clients (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL UNIQUE REFERENCES app_users(id),
    db_path     TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email  ON app_users(email);
CREATE INDEX IF NOT EXISTS idx_users_status ON app_users(status);
CREATE INDEX IF NOT EXISTS idx_users_role   ON app_users(role);
