import logging
import os

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from transaction_logger import TLogger
from connection_mapping import Connectors

load_dotenv()

class Extraction:

    """
        Extraction wrapper class to connect to various databases and extract data from
    """

    def __init__(self, table_details: dict) -> None:
        """
            Parameters
            ------------
            table_details: dict
                Required keys
                - source : Name of the source 
                - name: Name of the source table
                - user:
                - password:
                - host:
                - port:
                - DB: 
        """
        source = Connectors[table_details["source"]].value # Connectors["oracle"]
        self.connection = source(**self.get_connection_details())
        self.table_details = table_details

    def get_connection_details(self):
        conn_details = {"user": os.getenv("DBUSER"),
                "password": os.getenv("PASSWORD"),
                "host": os.getenv("HOST"),
                "port": os.getenv("PORT"),
                "DB": os.getenv("DB")
                }

        return conn_details

    def get_schema(self, table_name):
        return self.connection.get_schema(table_name)

    def extract(self):
        logger.info("Fetching last successful extract")
        last_successfull_extract = TLogger().get_last_successfull_extract(self.table_details["name"])
        logger.info(f"Last successful extract : {last_successfull_extract}")

        result_df = self.connection.extract(
                                    last_successfull_extract,
                                    **self.table_details)

        return result_df

