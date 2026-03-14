import os
from itertools import count

import psycopg2
from dotenv import load_dotenv
from flask import Flask, render_template, request
from livereload import Server

load_dotenv()

dev_mode = os.getenv("DEV_MODE", "1") == "1"  # set DEV_MODE=0 for production

app = Flask(__name__)


@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


DB_URL = os.getenv("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DB_URL)


def count_all_jobs():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM jobs
                WHERE is_active = TRUE;
                """)
            total_jobs = cur.fetchone()[0]
    return total_jobs


def get_industries():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT i.name
                FROM industries i
                join jobs j on j.industry_id = i.id
                where j.is_active = TRUE
                ORDER BY name;
                """)
            industries = [row[0].strip() for row in cur.fetchall()]
    return industries


def get_skills():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.name
                FROM skills s
                join job_skills js on js.skill_id = s.id
                join jobs j on j.id = js.job_id
                where j.is_active = TRUE
                ORDER BY name;
                """)
            skills = [row[0] for row in cur.fetchall()]
    return skills


def get_job_count():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT  COUNT(DISTINCT(id)) AS jobs, COUNT(distinct(source)) AS sources
                        FROM jobs
                        WHERE is_active = TRUE;
                        """)
            jobs, sources = cur.fetchone()

    # return 13,13,13
    return jobs, sources


@app.route("/health")
def health():
    return {"status": "awesome"}


@app.route("/search")
def search():

    q = request.args.get("q")

    jobs = []

    if q:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        j.title,
                        c.name,
                        l.state,
                        l.country,
                        j.source_url
                    FROM jobs j
                    LEFT JOIN companies c ON c.id = j.company_id
                    LEFT JOIN locations l ON l.id = j.location_id
                    LEFT JOIN job_skills js ON js.job_id = j.id
                    LEFT JOIN skills s ON s.id = js.skill_id
                    WHERE
                        j.is_active = TRUE
                        AND (
                            j.title ILIKE %s OR
                            j.description_raw ILIKE %s OR
                            c.name ILIKE %s OR
                            s.name ILIKE %s
                        )
                    GROUP BY j.id, c.name, l.state, l.country
                    ORDER BY j.last_seen DESC
                    LIMIT 100;
                """,
                    (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"),
                )

                jobs = cur.fetchall()

    return render_template("search.html", jobs=jobs, q=q)


@app.route("/find")
def find():

    q = request.args.get("q")
    job_total = count_all_jobs()
    jobs = []

    if q:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        j.title,
                        c.name,
                        l.state,
                        l.country,
                        j.source_url,
                        i.name AS industry,
                        j.external_id
                    FROM jobs j
                    LEFT JOIN companies c ON c.id = j.company_id
                    LEFT JOIN locations l ON l.id = j.location_id
                    LEFT JOIN job_skills js ON js.job_id = j.id
                    LEFT JOIN skills s ON s.id = js.skill_id
                    LEFT JOIN industries i on i.id = j.industry_id
                    WHERE
                        j.is_active = TRUE
                        AND (
                            j.title ILIKE %s OR
                            j.description_raw ILIKE %s OR
                            c.name ILIKE %s OR
                            s.name ILIKE %s
                        )
                    GROUP BY j.id, c.name, l.state, l.country, i.name, j.external_id
                    ORDER BY j.last_seen DESC
                    LIMIT 100;
                """,
                    (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"),
                )

                jobs = cur.fetchall()

    return render_template("find.html", jobs=jobs, q=q, job_total=job_total)


@app.route("/industries")
def industries_page():
    skills = get_skills()
    industries = get_industries()
    jobs, sources = get_job_count()
    return render_template(
        "industries.html",
        skills=skills,
        industries=industries,
        jobs=jobs,
        sources=sources,
    )


@app.route("/industries/<path:i_name>")
def i_jobs(i_name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.title as title, c.name as company, l.state as state, l.country as country, j.source_url as source_url, j.source as source, i.name as industry, j.external_id as external_id
                from jobs j
                join companies c on c.id = j.company_id
                join locations l on l.id = j.location_id
                join industries i on i.id = j.industry_id
                where j.is_active = TRUE and i.name = %s
                order by company;
            """,
                (i_name,),
            )
            jobs = [
                {
                    "title": row[0],
                    "company": row[1],
                    "state": row[2],
                    "country": row[3],
                    "source_url": row[4],
                    "source": row[5],
                    "industry": row[6],
                    "external_id": row[7],
                }
                for row in cur.fetchall()
            ]

    return render_template("i_jobs.html", industry=i_name, jobs=jobs)


@app.route("/industries/<path:i_name>/<path:external_id>")
def job_focus(i_name, external_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select j.title as title, c.name as company, l.state as state,
                        l.country as country, j.source_url as source_url, j.source as source,
                        i.name as industry, j.external_id as external_id, j.description_raw,
                        j.salary_min, j.salary_max, j.first_seen
                from jobs j
                join companies c on c.id = j.company_id
                join locations l on l.id = j.location_id
                join industries i on i.id = j.industry_id
                where j.external_id = %s
                order by company;
            """,
                (external_id,),
            )
            row = cur.fetchone()
        if row:
            job = {
                "title": row[0],
                "company": row[1],
                "state": row[2],
                "country": row[3],
                "source_url": row[4],
                "source": row[5],
                "industry": row[6],
                "external_id": row[7],
                "description_raw": row[8],
                "salary_min": row[9],
                "salary_max": row[10],
                "first_seen": row[11],
            }
        else:
            job = None

    return render_template("job_focus.html", industry=i_name, job=job)


@app.route("/")
def home():
    return render_template("index.html")


if __name__ == "__main__":
    if dev_mode:
        port = int(os.environ.get("PORT", 5500))  # dev port for livereload
        server = Server(app.wsgi_app)
        # watch templates and static CSS
        server.watch("templates/")
        server.watch("static/css/")
        # optional: watch Python files and reload server
        server.watch("*.py")
        server.serve(host="0.0.0.0", port=port, debug=True)
    else:
        port = int(os.environ.get("PORT", 8000))
        app.run(host="0.0.0.0", port=port, debug=True)
