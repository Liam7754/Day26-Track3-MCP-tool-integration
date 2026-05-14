from __future__ import annotations

import sqlite3
from pathlib import Path


DEFAULT_DB_PATH = Path(__file__).resolve().parent / "lab.db"


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS students;

CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cohort TEXT NOT NULL,
    score REAL NOT NULL CHECK (score >= 0 AND score <= 100),
    email TEXT UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    credits INTEGER NOT NULL CHECK (credits > 0)
);

CREATE TABLE enrollments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    enrolled_on TEXT NOT NULL,
    FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
    FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE,
    UNIQUE (student_id, course_id)
);
"""


STUDENTS = [
    ("An Nguyen", "A1", 91.5, "an.nguyen@example.edu"),
    ("Binh Tran", "A1", 84.0, "binh.tran@example.edu"),
    ("Chi Le", "A1", 97.0, "chi.le@example.edu"),
    ("Dung Pham", "B2", 73.5, "dung.pham@example.edu"),
    ("Em Vo", "B2", 88.0, "em.vo@example.edu"),
    ("Giang Ho", "C3", 79.5, "giang.ho@example.edu"),
]

COURSES = [
    ("MCP101", "Model Context Protocol Foundations", 3),
    ("SQL201", "Safe SQL for Application Developers", 4),
    ("PY150", "Python Integration Lab", 3),
]

ENROLLMENTS = [
    (1, 1, "active", "2026-01-15"),
    (1, 2, "active", "2026-01-16"),
    (2, 1, "active", "2026-01-15"),
    (3, 1, "active", "2026-01-17"),
    (3, 3, "active", "2026-01-18"),
    (4, 2, "completed", "2026-01-20"),
    (5, 2, "active", "2026-01-21"),
    (6, 3, "active", "2026-01-22"),
]


def create_database(db_path: str | Path = DEFAULT_DB_PATH) -> Path:
    """Create a clean, reproducible SQLite database and return its path."""
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.executemany(
            "INSERT INTO students (name, cohort, score, email) VALUES (?, ?, ?, ?)",
            STUDENTS,
        )
        conn.executemany(
            "INSERT INTO courses (code, title, credits) VALUES (?, ?, ?)",
            COURSES,
        )
        conn.executemany(
            """
            INSERT INTO enrollments (student_id, course_id, status, enrolled_on)
            VALUES (?, ?, ?, ?)
            """,
            ENROLLMENTS,
        )
        conn.commit()

    return path


if __name__ == "__main__":
    database_path = create_database()
    print(f"Created reproducible SQLite database at {database_path}")
