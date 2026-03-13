import os

import psycopg2
from dotenv import load_dotenv
from flask import Flask, render_template
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
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=True)
