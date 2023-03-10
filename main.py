import os
import datetime;
import logging
import warnings

from config import Config
from extract import Extraction
from dotenv import load_dotenv
from load import Loader
from transaction_logger import TLogger

warnings.filterwarnings("ignore")

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class Main:
    def __init__(self) -> None:
        pass

    def run(self):

        logger.info(f"Reading config files from path {os.getenv('CONFIG_FILES_PATH')}")

        configs = Config(os.getenv("CONFIG_FILES_PATH")).get_config()
        for config in configs:
            for table in config["tables"]:

                if table["extract"]:
                    additional_info = ""
                    extraction_start_time = datetime.datetime.now()
                    logger.info(f"Started extraction for : {table['name']} at {extraction_start_time}")
                    extraction_obj = Extraction(table)

                    result_df = extraction_obj.extract(table)
                    logging.info(f"Completed extraction for : {table['name']} at {datetime.datetime.now()}")

                    logging.info(f"Getting schema details of table `{table['name']}` from {table['source']}")
                    source_schema = extraction_obj.get_schema(table["name"])
                    logging.info(f"Successfully fetched schema details of table `{table['name']}` from {table['source']}")

                    try:
                        logging.info(f"Starting loading into {table['target_table_name']} at {table['destination']}")

                        loader_obj = Loader(table)
                        loader_obj.create_schema(source_schema, table["source"])

                        loader_obj.load(result_df)

                        number_of_records = len(result_df)
                        last_fetched_value = str(result_df[table["incremental_column"].upper()].max())
                        load_status = "Success"
                        logging.info(f"Successfully loaded {number_of_records} records into {table['target_table_name']} at {table['destination']}")

                    except Exception as e:
                        logging.error(e)

                        additional_info = e
                        load_status = "Failed"
                        number_of_records = 0

                    finally:
                        logging.info(f"Logging transaction history in the reporting table")
                        extraction_end_time = datetime.datetime.now()

                        try:
                            final_status = {"table_name": table["name"],
                                            "extraction_status": load_status,
                                            "number_of_records": number_of_records,
                                            "extraction_start_time": str(extraction_start_time), 
                                            "extraction_end_time": str(extraction_end_time),
                                            "additional_info": str(additional_info),
                                            "incremental_column":table["incremental_column"],
                                            "last_fetched_value": last_fetched_value}
                            TLogger().log(**final_status)
                        except:
                            logging.error("Failed to log status in the reporting table")

            # break


if __name__ == "__main__":
    Main().run()



