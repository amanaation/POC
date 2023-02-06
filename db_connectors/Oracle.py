import logging
import oracledb
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine
from db_connectors.connectors import Connectors
from transaction_logger import TLogger

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class OracleDatabaseConnection(Connectors):

    def __init__(self, **kwargs) -> None:
        
        logger.info("Creating connection")

        self.engine = create_engine(f'''oracle+cx_oracle://
                                        {kwargs["user"]}:{kwargs["password"]}
                                        @{kwargs['host']}
                                        /{kwargs['DB']}''')
        
        self.connection = oracledb.connect(
            user=kwargs["user"],
            password=kwargs["password"],
            dsn=f"{kwargs['host']}:{kwargs['port']}/{kwargs['DB']}")

        self.cursor = self.connection.cursor()
        logger.info("Connection created successfully with source")

    def get_schema(self, table_name: str) -> pd.DataFrame:
        """
            Connects to the source database, and gets the source schema
            Parameters
            -----------
                table_name: Name of the source table
            Returns
            ---------
                pd.DataFrame: Schema details of source table

        """
        schema_details_query = f"""SELECT column_name, data_type, NULLABLE, IDENTITY_COLUMN
                                  FROM USER_TAB_COLUMNS
                                  WHERE table_name = '{table_name.upper()}' """

        schema_details = pd.read_sql(schema_details_query, self.connection)
        return schema_details

    def extract(self, last_successfull_extract: dict, **table: dict):
        """
            Main Oracle extraction function
            Parameters
            ------------
                last_successfull_extract: dict
                    - Required Keys:
                        . last_fetched_value
                table: dict
                    - Required Keys:
                        . batch_size
                        . query
                        . incremental_type
                        . incremental_column
                        . incremental_column_format
            Return
            --------
                pd.DataFrame : Extracted dataframe from source table
        """
        batch_size = table["batch_size"]
        query = table["query"]

        if last_successfull_extract:
            logger.info("Adding incremental clause to query")
            if table["incremental_type"] == "timestamp":

                incremental_clause = f"""{table['incremental_column']} >
                                    TO_DATE('{last_successfull_extract['last_fetched_value']}', 
                                    '{table["incremental_column_format"]}')"""

            elif table["incremental_type"] == "id":
                incremental_clause = f""" {table['incremental_column']} > {last_successfull_extract['last_fetched_value']}"""
            
            if "where" in query.lower():
                query += f"and   {incremental_clause}"
            else:
                query += f" where {incremental_clause}"
        result_df = pd.DataFrame()

        logger.info(f"Running query : {query}")

        logger.info("Executing query...")
        try:
            if not table["batch_size"]:
                result_df = pd.read_sql_query(query, self.connection)
            else:
                offset = 0
                while True:
                    logger.info(f"Fetching rows between {offset} and {offset + batch_size}")
                    updated_query = query + f" order by {table['incremental_column']} " + f" OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"                                 

                    pd_result = pd.read_sql_query(updated_query, self.connection)
                    result = pd.DataFrame(pd_result)

                    result_df = pd.concat([result_df, result], ignore_index=True)
                    if len(result) < batch_size:
                        break
                    offset += batch_size
        except Exception as e:
            logger.error(e)
        finally:
            return result_df

