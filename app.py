import os
from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def get_job_count():
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                        SELECT 
                            COUNT(jobs.id) as jobs,
                            COUNT(DISTINCT(skills.id)) as skills,
                            COUNT(DISTINCT source) as sources
                        from jobs
                        join job_skills
                        on jobs.id = job_skills.job_id
                        join skills
                        on job_skills.skill_id = skills.id
                        WHERE is_active = TRUE;
                        """)
            job_count, company_count, sources_count = cur.fetchone()

        cur.close()
    conn.close()

    #return 13,13,13
    return job_count, company_count, sources_count

@app.route("/health")
def health():
    return {"status": "awesome"}

@app.route("/")
def home():
    job_count, company_count, sources_count = get_job_count()
    return render_template("index.html", job_count=job_count, company_count=company_count, sources_count=sources_count)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port,debug=True)
