# config/settings.py

import os
from pathlib import Path

from dotenv import load_dotenv


project_root = Path(__file__).resolve().parent.parent
load_dotenv(project_root / ".env")

CLEANJOBDATA_API_KEY = os.getenv("CLEANJOBDATA_API_KEY")
CLEANJOBDATA_API_URL = "https://api.cleanjobdata.com/jobs"
CLEANJOBDATA_TITLE = os.getenv("CLEANJOBDATA_TITLE", "data engineer")
CLEANJOBDATA_COUNTRIES = tuple(
	country.strip().lower()
	for country in os.getenv("CLEANJOBDATA_COUNTRIES", "za").split(",")
	if country.strip()
)
CLEANJOBDATA_MAX_PAGES = int(os.getenv("CLEANJOBDATA_MAX_PAGES", "5"))
CLEANJOBDATA_PAGE_LIMIT = int(os.getenv("CLEANJOBDATA_PAGE_LIMIT", "20"))