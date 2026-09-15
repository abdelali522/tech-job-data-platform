CREATE OR REPLACE VIEW tech_job_lake.silver_jobs_flat AS
SELECT
  id,
  title AS job_title,
  location AS location_name,
  from_iso8601_timestamp(published) AS published_at,
  description AS job_description,
  application_url,
  has_remote,
  salary_min,
  salary_max,
  salary_currency,
  company.name AS company_name
FROM tech_job_lake.silver_jobs;

CREATE OR REPLACE VIEW tech_job_lake.jobs_by_company AS
SELECT
  company.name AS company_name,
  count(*) AS job_count
FROM tech_job_lake.silver_jobs
WHERE company.name IS NOT NULL
GROUP BY company.name;
