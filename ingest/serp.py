import os
import requests
import psycopg2
import json
import re
from dotenv import load_dotenv

debug = True

load_dotenv()

API_URL = "https://serpapi.com/search.json?q=data-analyst&engine=google&api_key=fa14df0786becf9eaa9c4645f91fb0fa5462b702aa9dcf6086eb5e8ce73ac464"

DB_URL = os.getenv("DATABASE_URL")

def fetch_jobs():
    r = requests.get(API_URL, timeout=10)
    r.raise_for_status()
    return r.json()['results']

def save_json(jobs):
    with open("1serpTest.json", "w") as f:
        json.dump(jobs, f, indent=4)

def get_connection():
    return psycopg2.connect(DB_URL)

def upsert_company(cur, name):
    cur.execute(
        """
        INSERT INTO companies (name)
        VALUES (%s)
        ON CONFLICT (name) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id;
    """,
        (name,),
    )
    return cur.fetchone()[0]

def upsert_location(cur, city, state, country):

    cur.execute(
        """
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        ON CONFLICT (city, state, country)
        DO UPDATE SET city = EXCLUDED.city
        RETURNING id;
    """,
        (city, state, country),
    )

    return cur.fetchone()[0]

def insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_predicted, employment_type):
    cur.execute(
        """
        INSERT INTO jobs (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url,
            employment_type,
            salary_min,
            salary_max,
            salary_currency,
            salary_predicted
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET 
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title = EXCLUDED.title,
            description_raw = EXCLUDED.description_raw,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            salary_currency = EXCLUDED.salary_currency,
            salary_predicted = EXCLUDED.salary_predicted
        RETURNING id, (xmax = 0) AS inserted;
    """,
        (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            "adzuna",
            source_url,
            employment_type,
            salary_min,
            salary_max,
            "$",
            salary_predicted
        ),
    )
    job_id, inserted = cur.fetchone()

    return job_id, int(inserted)

def db_close(cur, conn):
    if not debug:
        conn.commit()
    cur.close()
    conn.close()

def db_open():
    conn = get_connection()
    cur = conn.cursor()
    return conn, cur

def assign_job_info(cur, jobs):
    rows_added = 0
    for job in jobs:
        title = job['title']
        external_id = job['id']
        description_raw = job['description']
        salary_min = job['salary_min']
        salary_max = job['salary_max']
        salary_predicted = job['salary_is_predicted']
        if len(job['location']['area']) == 0:
            country = "Unknown"
            state = "Unknown"
            city = ""
        else:
            country = job['location']['area'][0]
            state = job['location']['area'][1]
            city = ""
        company = job['company']['display_name']
        source = "adzuna"
        source_url = job['redirect_url']
        employment_type = (
            job.get("contract_time")
            or job.get("contract_type")
            or None
        )
        company_id = upsert_company(cur, company)
        location_id = upsert_location(cur, city, state, country)
        job_id, inserted = insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_predicted, employment_type)
        rows_added += inserted
        fetch_dbskills(cur, job_id, description_raw)
        return rows_added

def fetch_dbskills(cur, job_id, job_desc):
    # populate our skills from table
    cur.execute(
        """
        SELECT s.normalized_name, s.id 
        FROM skills as s;
    """,
    )
    skills = cur.fetchall()

    compiled_skills = [
        (s_id, s_name, re.compile(rf"\b{re.escape(s_name)}\b", re.IGNORECASE))
        for s_name, s_id in skills
    ]

    if job_desc == "":
        return
    job_desc = job_desc.lower()
    for s_id, s_name, s_patt in compiled_skills:
        weight = len(s_patt.findall(job_desc))
        if weight:
            tag_skill_on_job(cur, job_id, s_id, weight)

def tag_skill_on_job(cur, job, skill, weight):
    cur.execute(
        """
        INSERT INTO job_skills(job_id, skill_id, weight)
        VALUES (%s, %s, %s)
        ON CONFLICT (job_id, skill_id)
        DO UPDATE SET weight = EXCLUDED.weight
    """,
        (job, skill, weight),
    )


def main():

    ## api call to retrieve and store jobs
    jobs = fetch_jobs()
    ## save api call results in json 
    save_json(jobs)

    ## establish db connection and cursor
    conn, cur = db_open()

    ## assign job variables from retrieved job,
    ## upsert company, location, 
    ## insert job
    ## scan job description for known skills and insert
    rows_added = assign_job_info(cur, jobs)
    
    ## commit our sql 
    ## close our cursor and connection
    db_close(cur, conn)

    print(f"Serp added {rows_added}")

if __name__ == "__main__":
    main()
