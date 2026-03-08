import os
import requests
import psycopg2
import json
import re
from dotenv import load_dotenv





debug = False

load_dotenv()

API_URL = "https://jsearch.p.rapidapi.com/search"

querystring = {"query":"data analyst United states","page":"1","num_pages":"1","country":"us","date_posted":"all","work_from_home":"true"}

headers = {
	"x-rapidapi-key": "c8e82f8d37msh3470d768e679d12p12fc0djsn0cac4e2107d1",
	"x-rapidapi-host": "jsearch.p.rapidapi.com"
}

DB_URL = os.getenv("DATABASE_URL")

def fetch_jobs():
    r = requests.get(API_URL, headers=headers, params=querystring, timeout=30)
    r.raise_for_status()
    return r.json()['data']

def save_json(jobs):
    with open("jsearch_results.json", "w") as f:
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

def insert_job(cur, external_id, company_id, location_id, 
               title, description_raw, source, source_url, 
               salary_min, salary_max, salary_freq,
               employment_type, qualifications, benefits, 
               responsibilities, is_remote):
    
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
            salary_min,
            salary_max,
            salary_freq,
            employment_type,
            qualifications,
            benefits,
            responsibilities,
            work_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET 
            last_seen = now(),
            source_url = EXCLUDED.source_url,
            title = EXCLUDED.title,
            description_raw = EXCLUDED.description_raw,
            employment_type = EXCLUDED.employment_type,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            qualifications = EXCLUDED.qualifications,
            benefits = EXCLUDED.benefits,
            responsibilities = EXCLUDED.responsibilities,
            work_mode = EXCLUDED.work_mode
        RETURNING id;
    """,
        (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url,
            salary_min,
            salary_max,
            salary_freq,
            employment_type,
            qualifications,
            benefits,
            responsibilities,
            is_remote
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

def main():
    jobs = fetch_jobs()
    save_json(jobs)

    conn, cur = db_open()

    for job in jobs:
        title = job['job_title']
        external_id = job['job_id']
        description_raw = job['job_description']
        salary_min = job['job_min_salary']
        salary_max = job['job_max_salary']
        salary_freq = job['job_salary_period']
        country = job['job_country'] or "US"
        state = job['job_state'] or "Unknown"
        city = job['job_city'] or "Unknown"
        company = job['employer_name']
        source = job['job_publisher']
        source_url = job['job_apply_link']
        employment_type = job['job_employment_type']
        if job['job_highlights'].get("Qualifications"):
            qualifications = ', '.join(job['job_highlights'].get("Qualifications"))
        else:
            qualifications = None        
        if job['job_highlights'].get("Benefits"):
            benefits = ', '.join(job['job_highlights'].get("Benefits"))
        else:
            benefits = None
        if job['job_highlights'].get("Responsibilities"):
            responsibilities = ', '.join(job['job_highlights'].get("Responsibilities"))
        else:
            responsibilities = None
        is_remote = job['job_is_remote']
        if is_remote and is_remote == 'true':
            is_remote = "remote"
        else:
            is_remote = "unknown"
        # if job['job_posted_at_datetime_utc']:
        #     job_posted_date = job['job_posted_at_datetime_utc']
        company_id = upsert_company(cur, company)
        location_id = upsert_location(cur, city, state, country)
        insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_freq, employment_type, qualifications, benefits, responsibilities, is_remote)

        # print(f"{external_id}, {title}, {source}, {salary_min}, {salary_max}, {salary_freq}, {employment_type}, {is_remote}, {benefits}")

    db_close(cur, conn)



if __name__ == "__main__":
    main()
