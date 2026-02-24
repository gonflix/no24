CREATE DATABASE IF NOT EXISTS ticketing
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE ticketing;

CREATE TABLE events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    status ENUM('A', 'N') DEFAULT 'A',              -- A: Active, N: Not Active
    start_at TIMESTAMP NOT NULL,
    end_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- INSERT INTO events (name, status, start_at, end_at) VALUES ('bts_2026_seoul', 'A', NOW(), NOW());
-- INSERT INTO events (name, status, start_at, end_at) VALUES ('newjeans_2027_seoul', 'N', NOW(), NOW());
-- INSERT INTO events (name, status, start_at, end_at) VALUES ('ive_2026_seoul', 'A', NOW(), NOW());