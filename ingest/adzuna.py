import os
import requests
import psycopg2
import json
import re
from dotenv import load_dotenv

debug = False

load_dotenv()

API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/1?app_id=42b6f469&app_key=65b227d0c211e8eb15d4817c030afc82&results_per_page=50&category=it-jobs&content-type=application/"

DB_URL = os.getenv("DATABASE_URL")

def fetch_jobs():
    r = requests.get(API_URL, timeout=10)
    r.raise_for_status()
    return r.json()['results']

def save_json(jobs):
    with open("adzunaTests.json", "w") as f:
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
        RETURNING id;
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
    return cur.fetchone()[0]

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
    for job in jobs:
        title = job['title']
        external_id = job['id']
        description_raw = job['description']
        salary_min = job['salary_min']
        salary_max = job['salary_max']
        salary_predicted = job['salary_is_predicted']
        country = job['location']['area'][0]
        state = job['location']['area'][1]
        county = job['location']['area'][2]
        if len(job['location']['area']) > 3:
            city = job['location']['area'][3]
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
        job_id = insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_predicted, employment_type)
        fetch_dbskills(cur, job_id, description_raw)

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
    assign_job_info(cur, jobs)
    
    ## commit our sql 
    ## close our cursor and connection
    db_close(cur, conn)

if __name__ == "__main__":
    main()
