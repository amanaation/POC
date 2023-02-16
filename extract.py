import logging
import json

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
        source = Connectors[table_details["source"]].value  # Connectors["api"]
        self.connection = source(**table_details)
        self.table_details = table_details

    def get_schema(self, *args):
        return self.connection.get_schema(*args)

    def handle_extract_error(self, args):
        return self.connection.handle_extract_error(args)

    def update_last_successful_extract(self):
        last_successful_extract = self.connection.last_successful_extract
        if last_successful_extract:
            last_successful_extract = {column: str(last_successful_extract[column]) for column in
                                       last_successful_extract}
            last_successful_extract = json.dumps(last_successful_extract)
        else:
            last_successful_extract = None

        print("last_successful_extract : ", last_successful_extract)
        return last_successful_extract

    def get_last_successful_extract(self):
        last_successful_extract = self.connection.last_successful_extract
        last_successful_extract = json.dumps(last_successful_extract)

        return last_successful_extract

    def extract(self):
        logger.info("Fetching last successful extract")
        last_successful_extract = TLogger().get_last_successful_extract(self.table_details)
        if last_successful_extract:
            last_successful_extract = json.loads(last_successful_extract["last_fetched_value"])
        logger.info(f"Last successful extract : {last_successful_extract}")

        connection_extract_function = self.connection.extract(
            last_successful_extract,
            **self.table_details)
        try:
            while True:
                yield next(connection_extract_function)
        except StopIteration:
            print("Stopping extraction")
            pass
