import remotive
import remotive_skills
import usajobs
import usajobs_skills
import adzuna
import adzuna_skills
import logging
from datetime import datetime


now = datetime.now()

basicConfig(
    level=logging.DEBUG,
    filename=f"pipeline{now}.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    logging.info("attempt remotive jobs")
    # print("_.-`` remotive ``-._")
    remotive.main()
    logging.info("attempt remotive skills")
    # print("``-._  skills  _.-``")
    remotive_skills.main()
    logging.info("attempt usajobs jobs")
    # print("_.-`` usajobs ``-._")
    usajobs.main()
    logging.info("attempt usajobs skills")
    # print("``-._  skills  _.-``")
    usajobs_skills.main()
    logging.info("attempt adzuna")
    # print("_.-``  adzuna  ``-._")
    adzuna.main()
    logging.info("attempt adzuna skills")
    # print("``-._  skills  _.-``")
    adzuna_skills.main()


if __name__ == "__main__":
    main()

