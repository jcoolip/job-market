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
                        SELECT COUNT(distinct(source)) AS sources, COUNT(DISTINCT(id)) AS jobs 
                        FROM jobs
                        WHERE is_active = TRUE;
                        """)
            jobs, sources = cur.fetchone()

        cur.close()
    conn.close()

    #return 13,13,13
    return jobs, sources

@app.route("/health")
def health():
    return {"status": "awesome"}

@app.route("/")
def home():
    jobs, sources = get_job_count()
    return render_template("index.html", jobs=jobs, sources=sources)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port,debug=True)
