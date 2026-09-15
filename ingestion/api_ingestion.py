import requests
import json
import os
import sys
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.settings import (
    CLEANJOBDATA_API_KEY,
    CLEANJOBDATA_API_URL,
    CLEANJOBDATA_COUNTRIES,
    CLEANJOBDATA_MAX_PAGES,
    CLEANJOBDATA_PAGE_LIMIT,
    CLEANJOBDATA_TITLE,
)

def create_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_tech_jobs(country: str, session: requests.Session | None = None) -> list[dict[str, Any]]:
    if not CLEANJOBDATA_API_KEY:
        raise RuntimeError("Set the CLEANJOBDATA_API_KEY environment variable before running ingestion.")

    all_jobs = []
    next_page = None
    session = session or create_http_session()

    print(f"Starting ingestion for {country}...")

    for page in range(1, CLEANJOBDATA_MAX_PAGES + 1):
        params = {
            "title": CLEANJOBDATA_TITLE,
            "location": country.upper(),
            "limit": CLEANJOBDATA_PAGE_LIMIT,
            "extra_fields": "description",
        }
        if next_page:
            params["next_page"] = next_page

        print(f"Fetching Page {page}...")
        response = session.get(
            CLEANJOBDATA_API_URL,
            headers={"Authorization": f"Bearer {CLEANJOBDATA_API_KEY}"},
            params=params,
            timeout=30,
        )

        response.raise_for_status()
        data = response.json()
        jobs = data.get("data", [])
        if not isinstance(jobs, list):
            raise ValueError("CleanJobData response field 'data' must be a list")
        all_jobs.extend(jobs)
        next_page = data.get("pagination", {}).get("next_page")
        if not next_page:
            break

    return all_jobs


def publish_jobs_to_kafka(jobs: list[dict[str, Any]]) -> None:
    if not jobs:
        print("No jobs were collected.")
        return

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    try:
        from confluent_kafka import Producer
    except ImportError:
        publish_jobs_with_kafka_python(jobs, bootstrap_servers)
        return

    delivery_errors = []

    def delivery_callback(error, message):
        if error is not None:
            delivery_errors.append(str(error))

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
    })
    for job in jobs:
        payload = json.dumps(job, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce("jobs.raw", value=payload, callback=delivery_callback)
                break
            except BufferError:
                producer.poll(1)
        producer.poll(0)
    producer.flush(30)
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
    print(f"Published {len(jobs)} jobs to Kafka topic jobs.raw")


def publish_jobs_with_kafka_python(jobs: list[dict[str, Any]], bootstrap_servers: str) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda job: json.dumps(job, separators=(",", ":")).encode("utf-8"),
        acks="all",
    )
    try:
        for job in jobs:
            producer.send("jobs.raw", job).get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    print(f"Published {len(jobs)} jobs to Kafka topic jobs.raw")

if __name__ == "__main__":
    jobs = []
    for country in CLEANJOBDATA_COUNTRIES:
        jobs.extend(fetch_tech_jobs(country))
    publish_jobs_to_kafka(jobs)
    
