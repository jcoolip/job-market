import remotive
import remotive_skills
import usajobs
import usajobs_skills
import adzuna
import jsearch
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
    print("_.-`` remotive ``-._")
    remotive.main()
    print("``-._  skills  _.-``")
    remotive_skills.main()
    print("_.-`` usajobs ``-._")
    usajobs.main()
    print("``-._  skills  _.-``")
    usajobs_skills.main()
    print("_.-``  adzuna  ``-._")
    adzuna.main()
    logging.info("attempt jsearch")
    print("_.-``  jsearch  ``-._")
    jsearch.main()
    logging.info("-- COMPLETE --")


if __name__ == "__main__":
    main()
