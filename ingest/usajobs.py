import os
import requests
import psycopg2
from dotenv import load_dotenv
import json

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
        ON CONFLICT (name)
        DO UPDATE SET name = companies.name
        RETURNING id;
    """,
        (name,),
    )
    return cur.fetchone()[0]


def upsert_location(cur, location_obj):

    location = location_obj.get("CityName", "")
    parts = [p.strip() for p in location.split(",")]

    state = parts[-1] if len(parts) >= 1 else ""
    city = parts[-2] if len(parts) >= 2 else ""
    site = ", ".join(parts[:-2]) if len(parts) > 2 else ""

    # if site != "":
    #     print(f"{site}, {city}, {state}")
    # else:
    #     print(f"{city}, {state}")
    
    # city = location_obj.get("CityName") or ""
    # state = location_obj.get("CountrySubDivisionCode") or ""
    country = location_obj.get("CountryCode") or ""

    cur.execute(
        """
        INSERT INTO locations (city, state, country)
        VALUES (%s, %s, %s)
        ON CONFLICT (city, state, country)
        DO UPDATE SET city = locations.city
        RETURNING id;
    """,
        (city, state, country),
    )

    return cur.fetchone()[0]


def insert_job(cur, external_id, job, company_id, location_id):
    r = job.get("PositionRemuneration", [{}])[0]
    salary_min = float(r.get("MinimumRange")) if r.get("MinimumRange") else None
    salary_max = float(r.get("MaximumRange")) if r.get("MaximumRange") else None

    cur.execute(
        """
        INSERT INTO jobs (
            external_id,
            company_id,
            location_id,
            title,
            description_raw,
            qualifications,
            source,
            source_url,
            salary_min,
            salary_max
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id)
        DO UPDATE SET 
            last_seen = now(),
            title = EXCLUDED.title,
            qualifications = EXCLUDED.qualifications,
            description_raw = EXCLUDED.description_raw,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            is_active = TRUE
        RETURNING id;
    """,
        (
            external_id,
            company_id,
            location_id,
            job["PositionTitle"],
            job["UserArea"]["Details"]["AgencyMarketingStatement"],
            job["QualificationSummary"],
            "usajobs",
            job["PositionURI"],
            salary_min,
            salary_max,
        ),
    )

    # print(job["QualificationSummary"])
    # print(f"{salary_min}-{salary_max}")
    # print(job.get("PositionRemuneration"))
    # print(f"Min: {salary_min} -  Max: {salary_max}")
    # print(job.get("PositionRemuneration", "MaximumRange"))
    return cur.fetchone()[0]


def main():
    jobs = fetch_jobs()
    conn = get_connection()
    cur = conn.cursor()

    # jobs.to_json("jsonSave.json", orient="records", indent=2)
    f = "usajobsResults.json"
    with open(f, "w") as json_file:
        json.dump(jobs, json_file, indent=4)

    for item in jobs:
        job = item["MatchedObjectDescriptor"]
        external_id = item["MatchedObjectId"]
        company_id = upsert_company(cur, job["OrganizationName"])
        location = job.get("PositionLocation", [{}])[0]

        location_id = upsert_location(cur, location)

        insert_job(cur, external_id, job, company_id, location_id)

    conn.commit()
    cur.close()
    conn.close()

    print("Inserted jobs successfully.")


if __name__ == "__main__":
    main()
