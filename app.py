import os
from flask import Flask, render_template
import psycopg2
from livereload import Server
from dotenv import load_dotenv
load_dotenv()

dev_mode = os.getenv("DEV_MODE", "1") == "1"  # set DEV_MODE=0 for production

app = Flask(__name__)

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

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
                """
            )
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

    #return 13,13,13
    return jobs, sources

@app.route("/health")
def health():
    return {"status": "awesome"}

@app.route("/skills")
def skills_page():
    skills = get_skills()
    jobs, sources = get_job_count()
    return render_template("skills.html", skills=skills, jobs=jobs, sources=sources)

@app.route("/skills/<path:skill_name>")
def skills_jobs(skill_name):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select j.title, c.name, l.state, l.country, j.source_url
                from jobs j
                join job_skills js on js.job_id = j.id
                join skills s on s.id = js.skill_id
                join companies c on c.id = j.company_id
                join locations l on l.id = j.location_id
                where j.is_active = TRUE and s.name = %s;
            """, (skill_name,))
            jobs = [
                {
                    "title": row[0],
                    "company": row[1],
                    "state": row[2],
                    "country": row[3],
                    "source_url": row[4]
                } for row in cur.fetchall()
            ]

    return render_template("skills_jobs.html", skill=skill_name, jobs=jobs)

@app.route("/")
def home():
    return render_template("index.html")

if __name__ == "__main__":
    if dev_mode:
        port = int(os.environ.get("PORT", 5500))  # dev port for livereload
        server = Server(app.wsgi_app)
        # watch templates and static CSS
        server.watch('templates/')
        server.watch('static/css/')
        # optional: watch Python files and reload server
        server.watch('*.py')
        server.serve(host="0.0.0.0", port=port, debug=True)
    else:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port,debug=True)
