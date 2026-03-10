import remotive
import remotive_skills
import usajobs
import usajobs_skills
import adzuna
import jsearch
import industry_classify
import logging
from datetime import datetime


now = datetime.now()

logging.basicConfig(
    level=logging.DEBUG,
    filename="pipeline.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def main():
    logging.info("-- BEGIN --")
    print("start")
    remotive.main()
    remotive_skills.main()
    usajobs.main()
    usajobs_skills.main()
    adzuna.main()
    jsearch.main()
    industry_classify.main()
    print("finish")
    logging.info("-- COMPLETE --")


if __name__ == "__main__":
    main()
