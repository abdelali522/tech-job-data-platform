CREATE TABLE IF NOT EXISTS jobs_gold (
    id TEXT PRIMARY KEY,
    job_title TEXT NOT NULL,
    location_name TEXT,
    published TIMESTAMPTZ,
    job_description TEXT,
    application_url TEXT,
    company_name TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_currency TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs_gold_stage (
    id TEXT,
    job_title TEXT,
    location_name TEXT,
    published TIMESTAMPTZ,
    job_description TEXT,
    application_url TEXT,
    company_name TEXT,
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    salary_currency TEXT
);
