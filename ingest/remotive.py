import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://remotive.com/api/remote-jobs"

PARAMS = {"Category": "data"}

DB_URL = os.getenv("DATABASE_URL")


def fetch_jobs():
    r = requests.get(API_URL, params=PARAMS, timeout=10)
    r.raise_for_status()
    return r.json()


def transform(jobs):
    for (
        id,
        url,
        title,
        c_name,
        c_logo,
        category,
        job_type,
        pub_date,
        req_loc,
        salary,
        desc,
    ) in jobs:
        print(f"{title} - {c_name}, salary: {salary}")


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


def upsert_location(cur, location_obj):
    city = location_obj.get("CityName") or ""
    state = location_obj.get("CountrySubDivisionCode") or ""
    country = location_obj.get("CountryCode") or ""

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


def insert_job(cur, job, company_id, location_id):
    cur.execute(
        """
        INSERT INTO jobs (
            company_id,
            location_id,
            title,
            description_raw,
            source,
            source_url
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, source_url)
        DO UPDATE SET last_seen = now()
        RETURNING id;
    """,
        (
            company_id,
            location_id,
            job["PositionTitle"],
            job.get("QualificationSummary", ""),
            "usajobs",
            job["PositionURI"],
        ),
    )

    return cur.fetchone()[0]


def main():
    jobs = fetch_jobs()
    transform(jobs)


if __name__ == "__main__":
    main()
