import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://data.usajobs.gov/api/search"

HEADERS = {
    "Host": "data.usajobs.gov",
    "User-Agent": os.getenv("USAJOBS_EMAIL"),
    "Authorization-Key": os.getenv("USAJOBS_API_KEY"),
}

PARAMS = {"Keyword": "data analyst", "ResultsPerPage": 25}

DB_URL = os.getenv("DATABASE_URL")


def fetch_jobs():
    r = requests.get(API_URL, headers=HEADERS, params=PARAMS, timeout=10)
    r.raise_for_status()
    return r.json()["SearchResult"]["SearchResultItems"]


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
    conn = get_connection()
    cur = conn.cursor()

    for item in jobs:
        job = item["MatchedObjectDescriptor"]

        company_id = upsert_company(cur, job["OrganizationName"])

        location_id = upsert_location(cur, job["PositionLocation"][0])

        insert_job(cur, job, company_id, location_id)

    conn.commit()
    cur.close()
    conn.close()

    print("Inserted jobs successfully.")


if __name__ == "__main__":
    main()
