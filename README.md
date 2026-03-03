# Job Market Analyzer

Job Market Analyzer is a backend data engineering project that ingests live U.S. federal job postings and transforms them into a structured, queryable analytics dataset.

The system is designed to demonstrate production-ready backend architecture, secure infrastructure practices, and relational data modeling for market analysis.

---

## What It Does

- Fetches job listings from the USAJobs API
- Normalizes companies, locations, and job metadata
- Enforces idempotent ingestion using database constraints
- Models many-to-many relationships for skill analysis
- Serves a secure HTTPS API behind a reverse proxy

---

## Architecture Highlights

- lighttpd for TLS termination and reverse proxy
- Gunicorn + Flask for application layer
- PostgreSQL isolated to localhost
- SSH tunneling for secure remote database access
- Conflict-safe ETL using `ON CONFLICT` constraints

---

## Why It Matters

This project demonstrates:

- Secure service deployment
- Proper database hardening
- Idempotent ingestion pipelines
- Relational normalization
- Backend system design from infrastructure to data layer

It reflects practical backend and data engineering skills rather than isolated scripts or notebook-only workflows.
