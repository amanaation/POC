import logging
import os
import sys

sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from google.cloud import bigquery as bq
from google.api_core.exceptions import Conflict
from pprint import pprint
from datatypes import ORACLE2BQ
import pandas as pd
from db_connectors.connectors import Connectors
load_dotenv()


class BigQuery(Connectors):
    def __init__(self, **kwargs) -> None:
        self.project_id = kwargs['gcp_project_id']
        self.dataset_name = kwargs['gcp_bq_dataset_name']
        self.table_name = kwargs['target_table_name']

        self.table_id = f"{self.project_id}.{self.dataset_name}.{self.table_name}"

        self.client = bq.Client()
        self.job_config = bq.LoadJobConfig()

    def create_dataset(self):

        dataset_id = f"{self.client.project}.{self.dataset_name}"
        dataset = bq.Dataset(dataset_id)
        dataset.location = "US"
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Successfully created dataset : {self.dataset_name}")
        except Conflict:
            logger.info("Dataset already exists")

    def create_schema(self, schema_df: pd.DataFrame, source: str):
        logger.info(f"Creating DataSet : {self.dataset_name}")
        self.create_dataset()
        logger.info(f"Creating Schema : {self.table_name}")
        try:
            schema = []
            for index, row in schema_df.iterrows():
                column_name = row['COLUMN_NAME']
                source_data_type = row['DATA_TYPE']
                is_nullable = row['NULLABLE']

                if source.lower() == "oracle":
                    target_type_mapping = ORACLE2BQ
                else:
                    target_type_mapping = None

                if target_type_mapping:
                    target_data_type = target_type_mapping[source_data_type].value
                else:
                    target_data_type = "STRING"
                
                if is_nullable == "Y":
                    mode = "NULLABLE"
                else: 
                    mode = "REQUIRED"

                field = bq.SchemaField(column_name, target_data_type, mode=mode)
                schema.append(field)
            # pprint(schema)

            table = bq.Table(self.table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Successfully created schema : {self.table_id}")
        except Conflict:
            logger.info("Schema already exists")

    def save(self, df:pd.DataFrame):
        # print(df.dtypes)
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=self.job_config
        )
        job.result()


# if __name__ == "__main__":
#     args = {"gcp_project_id": "turing-nature-374608",
#             "gcp_bq_dataset_name": "test_dataset",
#             "target_table_name": "test_climate_bq"}
#     b = BigQuery(**args)

#     df_details = {'COLUMN_NAME': {0: 'TDATE', 1: 'MEANTEMP', 2: 'HUMIDITY', 3: 'WIND_SPEED', 4: 'MEANPRESSURE'}, 'DATA_TYPE': {0: 'DATE', 1: 'VARCHAR2', 2: 'VARCHAR2', 3: 'VARCHAR2', 4: 'VARCHAR2'}, 'NULLABLE': {0: 'Y', 1: 'Y', 2: 'Y', 3: 'Y', 4: 'Y'}, 'IDENTITY_COLUMN': {0: 'NO', 1: 'NO', 2: 'NO', 3: 'NO', 4: 'NO'}}
#     df = pd.DataFrame(df_details)

#     b.create_schema(df, "oracle")


