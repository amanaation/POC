import logging
import os
from db_connectors.BigQuery import BigQuery
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

class TLogger:
    def __init__(self) -> None:

        logging_table_details = {
            "gcp_project_id": os.getenv('LOGGING_GCP_PROJECT_ID'),
            "gcp_bq_dataset_name": os.getenv('LOGGING_GCP_PROJECT_DATASET_NAME'),
            "target_table_name": os.getenv('LOGGING_TABLE'),
        }

        self.bq_client = BigQuery(**logging_table_details).client
        self.table_id = self.bq_client.get_table("{}.{}.{}".format(
            os.getenv('LOGGING_GCP_PROJECT_ID'),
            os.getenv('LOGGING_GCP_PROJECT_DATASET_NAME'),
            os.getenv('LOGGING_TABLE')))


    def get_last_successfull_extract(self, table):
        query_job = self.bq_client.query(
                f"""
                SELECT 
                    extraction_start_time, 
                    incremental_column, 
                    last_fetched_value
                FROM 
                    `{self.table_id}`
                WHERE 
                    table_name = "{table}"
                    and extraction_status="Success"
                    and last_fetched_value is Not Null
                order by extraction_start_time desc limit 1;
                """
            )
        results = query_job.result()
        if results._total_rows:
            for result in results:
                last_extract = {"extraction_start_time":result["extraction_start_time"],
                                "incremental_column": result["incremental_column"],
                                "last_fetched_value": result["last_fetched_value"]}
                return last_extract

        return None

    def log(self, **kwargs):
        errors = self.bq_client.insert_rows_json(self.table_id, [kwargs])
        if errors:
            logger.error(f"Errors : {errors}")

