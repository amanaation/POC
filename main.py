import os
import datetime;
import logging
import warnings
import json
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)
import pandas as pd
from config import Config
from column_matching import ColumnMM
from extract import Extraction
from dotenv import load_dotenv
from load import Loader
from transaction_logger import TLogger
from transformation import Transformation
warnings.filterwarnings("ignore")

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
pd.set_option('display.max_columns', None)

class Main:
    def __init__(self) -> None:
        pass

    def match_columns(self, result_df, table_config_details, source_schema_definition):
        cmm = ColumnMM(table_config_details, source_schema_definition)
        cmm.match_columns(result_df)

    def run(self):
        """
            This function is the main function to call and start the extract
        """

        logger.info(f"Reading config files from path {os.getenv('CONFIG_FILES_PATH')}")

        # Reading configs
        configs = Config(os.getenv("CONFIG_FILES_PATH")).get_config()
        for config in configs:
            for table in config["tables"]:

                if table["extract"]:
                    # try:
                    additional_info = ""
                    extraction_start_time = datetime.datetime.now()
                    number_of_records_from_source = 0
                    number_of_records_after_transformation = 0
                    last_fetched_values = {}
                    incremental_columns = []
                    if "incremental_column" in table:
                        incremental_columns = list(table["incremental_column"].keys())

                    # ------------------------------ start extract ------------------------------ 
                    logger.info(f"Started extraction for : {table['name']} at {extraction_start_time}")
                    extraction_obj = Extraction(table)

                    extraction_func = extraction_obj.extract()

                    try:

                        destination_schema_created = False
                        while True:
                            result_df = next(extraction_func)

                            logging.info(f"Getting schema details of source table `{table['name']}` from {table['source']}")
                            source_schema = extraction_obj.get_schema(*[table["name"], result_df])
                            logging.info(f"Following is the source schema details `{table['name']}` from {table['source']}")
                            logger.info(f"\n{source_schema}")

                            number_of_records_from_source += len(result_df)

                            logging.info(f"Extracted {number_of_records_from_source} rows from: {table['name']}")
                            # ------------------------------ End extract ------------------------------ 

                            # ------------------------------ Start Transformation ------------------------------ 
                            logging.info(f"starting transformation {number_of_records_from_source} rows from: {table['name']}")
                            transform = Transformation()
                            result_df = transform.transform(result_df)
                            # print("result_df :", result_df.head())
                            number_of_records_after_transformation += len(result_df)
                            # ------------------------------ End Transformation ------------------------------ 
                            
                            # ------------------------------ Start Load ------------------------------ 

                            if number_of_records_after_transformation:
                                # check columns discrepancy
                                
                                logging.info(f"Starting loading into {table['target_table_name']} at {table['destination']}")
                                loader_obj = Loader(table)
                                if not destination_schema_created:
                                    self.match_columns(result_df.head(), table, source_schema)

                                    loader_obj.create_schema(source_schema, table["source"])
                                    destination_schema_created = True
                                loader_obj.load(result_df)
                    # ------------------------------ End Load ------------------------------ 

                    # ------------------------------ Start Transaction Logging ------------------------------ 
                            if number_of_records_after_transformation:
                                for incremental_column in incremental_columns:
                                    incremental_column_last_batch_fetched_value = result_df[incremental_column.upper()].max()
                                    if incremental_column in last_fetched_values:
                                        # print("incremental_column : ", incremental_column, last_fetched_values[incremental_column])
                                        last_fetched_values[incremental_column] = max([last_fetched_values[incremental_column], incremental_column_last_batch_fetched_value])
                                    else:
                                        last_fetched_values[incremental_column] = incremental_column_last_batch_fetched_value

                                    logger.info(f"Last fetched values : {last_fetched_values}")

                                # logger.info(f"Last fetched values : {last_fetched_values}")

                    except StopIteration:
                        pass

                    if last_fetched_values:
                        last_fetched_values = {column: str(last_fetched_values[column]) for column in last_fetched_values}
                        last_fetched_values = json.dumps(last_fetched_values)
                    else:
                        last_fetched_values = None

                    load_status = "Success"
                    logging.info(f"Successfully loaded {number_of_records_from_source} records into {table['target_table_name']} at {table['destination']}")
                    
                    try:
                        pass
                    except Exception as e:
                        logging.error(e)
                        additional_info = e
                        load_status = "Failed"

                    finally:
                        logging.info(f"Logging transaction history in the reporting table")
                        extraction_end_time = datetime.datetime.now()

                        # try:
                        # Log transaction history
                        final_status = {"source_table_name": table["name"],
                                        "source": table["source"],
                                        "source_type": table["source_type"],

                                        "destination_table_name": table["target_table_name"],

                                        "extraction_status": load_status,

                                        "number_of_records_from_source": number_of_records_from_source,
                                        "number_of_records_pushed_to_destination": number_of_records_after_transformation,

                                        "extraction_start_time": str(extraction_start_time), 
                                        "extraction_end_time": str(extraction_end_time),

                                        "additional_info": str(additional_info),

                                        "incremental_columns": str(incremental_columns),
                                        "last_fetched_values": last_fetched_values}
                        TLogger().log(**final_status)
                        # except Exception as e:
                        #     logging.error("Failed to log status in the reporting table")
                    # ------------------------------ Start Transaction Logging ------------------------------ 

            # break


if __name__ == "__main__":
    Main().run()
    # df = pd.read_csv("DailyDelhiClimateTest.csv")
    # Main().macth_columns("test_climate_bq", "test_climate_bq", df.head())



