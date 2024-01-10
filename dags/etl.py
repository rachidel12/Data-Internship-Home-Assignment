from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLES_CREATION_QUERY = """
	CREATE TABLE IF NOT EXISTS job (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    title VARCHAR(225),
	    industry VARCHAR(225),
	    description TEXT,
	    employment_type VARCHAR(125),
	    date_posted DATE
	);

	CREATE TABLE IF NOT EXISTS company (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    job_id INTEGER,
	    name VARCHAR(225),
	    link TEXT,
	    FOREIGN KEY (job_id) REFERENCES job(id)
	);

	CREATE TABLE IF NOT EXISTS education (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    job_id INTEGER,
	    required_credential VARCHAR(225),
	    FOREIGN KEY (job_id) REFERENCES job(id)
	);

	CREATE TABLE IF NOT EXISTS experience (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    job_id INTEGER,
	    months_of_experience INTEGER,
	    seniority_level VARCHAR(25),
	    FOREIGN KEY (job_id) REFERENCES job(id)
	);

	CREATE TABLE IF NOT EXISTS salary (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    job_id INTEGER,
	    currency VARCHAR(3),
	    min_value NUMERIC,
	    max_value NUMERIC,
	    unit VARCHAR(12),
	    FOREIGN KEY (job_id) REFERENCES job(id)
	);

	CREATE TABLE IF NOT EXISTS location (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    job_id INTEGER,
	    country VARCHAR(60),
	    locality VARCHAR(60),
	    region VARCHAR(60),
	    postal_code VARCHAR(25),
	    street_address VARCHAR(225),
	    latitude NUMERIC,
	    longitude NUMERIC,
	    FOREIGN KEY (job_id) REFERENCES job(id)
	)
"""


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

extract_path= 'staging/extracted'

@task()
def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv('source/jobs.csv')
    for index, row in df.iterrows():
        with open(f'{extract_path}/{index}.txt', 'w') as file:
            file.write(str(row['context']))


transform_path= 'staging/transformed'
@task()
def transform():
    """Clean and convert extracted elements to json."""
    transformed_data = []
    
    for filename in os.listdir(extract_path):
        if filename.endswith(".txt"):
            with open(os.path.join(extract_path, filename), 'r') as file:
                context_data = json.loads(file.read())
                transformed_job = {
                    "job": {
                        "title": context_data.get("job_title"),
                        "industry": context_data.get("job_industry"),
                        "description": context_data.get("job_description"),
                        "employment_type": context_data.get("job_employment_type"),
                        "date_posted": context_data.get("job_date_posted"),
                    },
                    "company": {
                        "name": context_data.get("company_name"),
                        "link": context_data.get("company_linkedin_link"),
                    },
                    "education": {
                        "required_credential": context_data.get("job_required_credential"),
                    },
                    "experience": {
                        "months_of_experience": context_data.get("job_months_of_experience"),
                        "seniority_level": context_data.get("seniority_level"),
                    },
                    "salary": {
                        "currency": context_data.get("salary_currency"),
                        "min_value": context_data.get("salary_min_value"),
                        "max_value": context_data.get("salary_max_value"),
                        "unit": context_data.get("salary_unit"),
                    },
                    "location": {
                        "country": context_data.get("country"),
                        "locality": context_data.get("locality"),
                        "region": context_data.get("region"),
                        "postal_code": context_data.get("postal_code"),
                        "street_address": context_data.get("street_address"),
                        "latitude": context_data.get("latitude"),
                        "longitude": context_data.get("longitude"),
                    }
                }
                transformed_data.append(transformed_job)

                # Save transformed data as json file
                with open(f'{transform_path}/{filename.replace(".txt", ".json")}', 'w') as json_file:
                    json.dump(transformed_job, json_file, indent=2)

@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id=sqlite_conn_id)

    for filename in os.listdir(transform_path):
        if filename.endswith(".json"):
            with open(os.path.join(transform_path, filename), 'r') as file:
                data = json.load(file)
                # Adjust the following code to match your database schema and table names
                # This is a simple example assuming you have a 'job' table
                sqlite_hook.run(f"""
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (
                        '{data["job"]["title"]}',
                        '{data["job"]["industry"]}',
                        '{data["job"]["description"]}',
                        '{data["job"]["employment_type"]}',
                        '{data["job"]["date_posted"]}'
                    );
                """)


@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )
    etl_pipeline = (extract() >> transform() >> load())

    create_tables >> etl_pipeline

etl_dag()
