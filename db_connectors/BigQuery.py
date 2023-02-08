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
    """
    BigQuery connection class to connect, read, get schema details and write to Bigquery

    Parameters
    ----------
        kwargs: dict
            Required keys:
                gcp_project_id : GCP project ID to use
                gcp_bq_dataset_name : GCP dataset name to connect to
                target_table_name : GCP table name to write/read data from and to        

    """

    def __init__(self, **kwargs):
        """
        Constructs all the necessary attributes for the Bigquery class

        Parameters
        ----------
            kwargs: dict
                Required keys:
                    gcp_project_id : GCP project ID to use
                    gcp_bq_dataset_name : GCP dataset name to connect to
                    target_table_name : GCP table name to write/read data from and to        
        """
        self.project_id = kwargs['target_project_id']
        self.dataset_name = kwargs['target_bq_dataset_name']
        self.table_name = kwargs['target_table_name']

        self.table_id = f"{self.project_id}.{self.dataset_name}.{self.table_name}"

        # Creating BigQuery client
        self.client = bq.Client()
        self.job_config = bq.LoadJobConfig()

    def create_dataset(self) -> None:
        """
            Create dataset in bigquery if not exists

            Returns
            ----------
            None
        """
        dataset_id = f"{self.client.project}.{self.dataset_name}"
        dataset = bq.Dataset(dataset_id)
        dataset.location = "US"
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Successfully created dataset : {self.dataset_name}")
        except Conflict:
            logger.info("Dataset already exists")

    def create_schema(self, schema_df: pd.DataFrame, source: str) -> None:
        """
            Create schema in bigquery if not exists
            Parameters
            ----------
                schema_df : Source schema details in a dataframe
                source: Name of the source e.g. oracle/bq

            Returns
            ----------
            None
        """
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

                field = bq.SchemaField(column_name, target_data_type)
                schema.append(field)

            table = bq.Table(self.table_id, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Successfully created schema : {self.table_id}")
        except Conflict:
            logger.info("Schema already exists")

    def get_schema(self, **kwargs)  -> None:
        pass

    def extract(self, **kwargs)  -> None:
        pass

    def save(self, df:pd.DataFrame) -> None:
        """
            This function writes the dataframe to bigquery

            Parameters
            ----------
                df: dataframe to write to bigquery

            Returns
            --------
            None
        """
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=self.job_config
        )
        job.result()

