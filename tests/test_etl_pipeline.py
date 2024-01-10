# tests/test_etl_pipeline.py
import pytest
from unittest.mock import patch, mock_open
from datetime import datetime
from etl_dag import etl_dag, extract, transform, load

# Assuming your file paths are defined in etl_dag.py
source_file_path = 'source/jobs.csv'
extracted_path = 'staging/extracted'
transformed_path = 'staging/transformed'
sqlite_conn_id = 'sqlite_default'

@pytest.fixture
def mock_source_data():
    return "context\nSome job context data"

def test_extract(mock_source_data):
    with patch("pandas.read_csv") as mock_read_csv, patch("os.makedirs") as mock_makedirs, patch("builtins.open", mock_open()) as mock_file:
        mock_read_csv.return_value = pd.DataFrame({"context": [mock_source_data]})
        extract()
        mock_makedirs.assert_called_once_with(extracted_path, exist_ok=True)
        mock_file.assert_called_once_with(f"{extracted_path}/0.txt", "w")


@pytest.fixture
def mock_extracted_data():
    return {"context": "Some job context data"}

@pytest.fixture
def mock_transformed_data():
    return {"job": {"title": "Job Title", "industry": "IT", "description": "Job Description", "employment_type": "Full-time", "date_posted": "2024-01-02"},
            "company": {"name": "Company Name", "link": "https://www.linkedin.com/company"},
            "education": {"required_credential": "Bachelor's Degree"},
            "experience": {"months_of_experience": 24, "seniority_level": "Mid"},
            "salary": {"currency": "USD", "min_value": 50000, "max_value": 80000, "unit": "per year"},
            "location": {"country": "United States", "locality": "New York", "region": "Northeast", "postal_code": "10001", "street_address": "123 Main St", "latitude": 40.7128, "longitude": -74.0060}}


def test_transform(mock_extracted_data, mock_transformed_data):
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_extracted_data))), \
         patch("json.loads", return_value=mock_transformed_data) as mock_json_loads, \
         patch("os.makedirs") as mock_makedirs, \
         patch("builtins.open", mock_open()) as mock_file:
        transform()
        mock_makedirs.assert_called_once_with(transformed_path, exist_ok=True)
        mock_file.assert_called_once_with(f"{transformed_path}/0.json", "w")

def test_load(mock_transformed_data):
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_transformed_data))), \
         patch("airflow.providers.sqlite.hooks.sqlite.SqliteHook") as mock_sqlite_hook:
        load()

        mock_sqlite_hook.return_value.get_conn.return_value.cursor.return_value.execute.assert_called_once()
        mock_sqlite_hook.return_value.get_conn.return_value.commit.assert_called_once()

        expected_query = "SELECT * FROM your_table WHERE some_condition"
        mock_sqlite_hook.return_value.get_conn.return_value.cursor.return_value.execute.assert_called_once_with(expected_query)

        mock_sqlite_hook.return_value.get_conn.return_value.cursor.return_value.fetchall.assert_called_once()

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

test_etl_dag_instance = etl_dag(
    dag_id="test_etl_dag",
    description="Test ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def test_etl_dag():
    with patch("airflow.providers.sqlite.hooks.sqlite.SqliteHook") as mock_sqlite_hook, patch("pandas.read_csv") as mock_read_csv, patch("os.makedirs") as mock_makedirs, patch("builtins.open", mock_open()) as mock_file:
        dag_run = test_etl_dag_instance.create_dagrun(
            run_id="test_run",
            state="success",
            execution_date=datetime(2024, 1, 2),
        )

        assert dag_run is not None

