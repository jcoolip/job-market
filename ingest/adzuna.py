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
    
    #print(f"@@@@{tags_row}")
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
        print("Here goes nothin'...")
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
        title = job['title']
        external_id = job['id']
        description_raw = job['description']
        salary_min = job['salary_min']
        salary_max = job['salary_max']
        salary_predicted = job['salary_is_predicted']
        country = job['location']['area'][0]
        state = job['location']['area'][1]
        county = job['location']['area'][2]
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
        insert_job(cur, external_id, company_id, location_id, title, description_raw, source, source_url, salary_min, salary_max, salary_predicted, employment_type)

    db_close(cur, conn)
    print("...cya")



if __name__ == "__main__":
    main()
