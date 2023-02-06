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

    def __init__(self, table_details) -> None:
        source = Connectors[table_details["source"]].value # Source["oracle"]
        self.connection = source(**self.get_connection_details())


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

    def extract(self, table_details):
        # Alternate way implemented in informa
        # if table_details["source"] == "oracle":
        #     connection = OracleDatabaseConnection(**self.get_connection_details())
        # elif table_details["source"] == "redshift":
        #     connection = Redshift(**self.get_connection_details())

        source = Source[table_details["source"]].value 
        connection = source(**self.get_connection_details())

        logger.info("Fetching last successful extract")
        last_successfull_extract = TLogger().get_last_successfull_extract(table_details["name"])
        logger.info(f"Last successful extract : {last_successfull_extract}")


        result_df = connection.extract(
                                    last_successfull_extract,
                                    **table_details)

        return result_df

