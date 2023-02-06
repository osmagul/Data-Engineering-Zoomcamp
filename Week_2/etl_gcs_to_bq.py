from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(month: int, year: int, color: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    
    path = Path(f"data/{color}/{gcs_path}")
    df = pd.read_parquet(path)

    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-cred")

    df.to_gbq(
        destination_table="dezoomcamp.yellow_rides",
        project_id="dtc-de-376110",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
    ):
    """Main ETL flow to load data into Big Query"""
    for month in months:
        df = extract_from_gcs(month, year, color)
        write_bq(df)


if __name__ == "__main__":
    color = "yellow", 
    year = 2019
    months = [2, 3]
    etl_gcs_to_bq(months, year, color)