import logging
import oracledb
import pandas as pd
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from db_connectors.connectors import Connectors
# from connectors import Connectors
from pprint import pprint

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class OracleDatabaseConnection(Connectors):

    def __init__(self, **kwargs) -> None:
        
        logger.info("Creating connection")
        connection_details = self.get_connection_details()

        self.engine = create_engine(f'''oracle+cx_oracle://
                                        {connection_details["user"]}:{connection_details["password"]}
                                        @{connection_details['host']}
                                        /{connection_details['DB']}''')
        
        self.connection = oracledb.connect(
            user=connection_details["user"],
            password=connection_details["password"],
            dsn=f"{connection_details['host']}:{connection_details['port']}/{connection_details['DB']}")

        self.cursor = self.connection.cursor()
        logger.info("Connection created successfully with source")

    def get_connection_details(self):
        conn_details = {"user": os.getenv("DBUSER"),
                "password": os.getenv("PASSWORD"),
                "host": os.getenv("HOST"),
                "port": os.getenv("PORT"),
                "DB": os.getenv("DB")
                }

        return conn_details

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

    def get_incremental_clause(self, incremental_columns: dict, last_successfull_extract: dict) -> str:
        """
        Creates the inmcremental clause to be added to where clause to fetch latest data

        Parameters
        ------------
            incremental_columns : dict
                Required Keys:
                    - column_name : [column_details]
            last_successfull_extract: dict
                Example value : 
                    - {
                        last_fetch_incremental_column1: last_fetch_incremental_value1,
                        last_fetch_incremental_column2: last_fetch_incremental_value2,

                    }
        Returns
        ----------
        str : incremental_clause
        """
        incremental_clause = ""
        for incremental_column_name in incremental_columns:

            incremental_column_name = incremental_column_name.lower()
            incremental_column = incremental_columns[incremental_column_name]
            if incremental_column_name in last_successfull_extract:
                logger.info("Adding incremental clause to query")

                if incremental_column["column_type"] == "timestamp":
                    incremental_clause += f"""  {incremental_column_name} > 
                                        TO_DATE('{last_successfull_extract[incremental_column_name]}', 
                                        '{incremental_column["column_format"]}') """

                elif incremental_column["column_type"] == "id":
                    incremental_clause += f""" {incremental_column_name} > {last_successfull_extract[incremental_column_name]}"""

                incremental_clause += " and"

        if incremental_clause:
            incremental_clause = incremental_clause[:-4]
        return incremental_clause

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
            Executes given query and returns the dataframe
            Parameters
            -------------
                query: str
                    Query to be execute
            Returns
            ----------
            DataFrame: Query result
        """
        result = pd.read_sql(sql, self.connection)
        return result

    def execute_batch(self, query: str, batch_size: int, incremental_columns: dict) -> pd.DataFrame :
        """
        Execute the query and fetch the data in batches using the given batch size

        Parameters
        ------------
            query: str
                Query to be executed
            batch_size: int
                size of batch in which the data should be fetched
            incremental_columns: dict
                Incremental column details

        Returns
        -----------
        pd.DataFrame: Result Dataframe
        """
        offset = 0
        result_df = pd.DataFrame()
        incremental_columns = list(incremental_columns.keys())
        incremental_columns = ', '.join(incremental_columns)

        while True:
            logger.info(f"Fetching rows between {offset} and {offset + batch_size}")
            updated_query = query + f" order by {incremental_columns} " + f" OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            pd_result = self.execute_query(updated_query)
            yield pd_result

            result_df = pd.concat([result_df, pd_result], ignore_index=True)
            offset += batch_size
            if len(pd_result) < batch_size:
                break
        
        return 

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
        incremental_columns = table["incremental_column"]

        # If there is a last successfull extract then add incremental clause
        if last_successfull_extract:
            incremental_clause = self.get_incremental_clause(incremental_columns, last_successfull_extract)

            if "where" in query.lower():
                query += f"and   {incremental_clause}"
            else:
                query += f" where {incremental_clause}"

        logger.info(f"Running query : {query}")
        if not table["use_offset"]:
            yield self.execute_query(query)
        else:
            try:
                func = self.execute_batch(query, batch_size, incremental_columns)
                while True:
                    yield next(func)
            except StopIteration:
                pass



# if __name__ == "__main__":
#     import os
#     conn_details = {"user": os.getenv("DBUSER"),
#                     "password": os.getenv("PASSWORD"),
#                     "host": os.getenv("HOST"),
#                     "port": os.getenv("PORT"),
#                     "DB": os.getenv("DB")
#                     }

#     pprint(conn_details)
#     last_extract_value = {"tdate": "2022-04-24 00:00:00", "meantemp": "135"}
#     last_extract_value = None

#     table = {'name': 'climate', 'extract': True, 'source': 'oracle', 'source_type': 'db', 'destination': 'bq', 'timestamp_column': 'tdate', 'timestamp_format': 'YYYY-MM-DD HH24:MI:SS', 'incremental_column': {'tdate': {'column_type': 'timestamp', 'column_format': 'YYYY-MM-DD HH24:MI:SS'}, 'meantemp': {'column_type': 'id'}}, 'incremental_type': 'timestamp', 'incremental_column_format': 'YYYY-MM-DD HH24:MI:SS', 'frequency': 'daily', 'query': 'select * from climate', 'batch_size': 300, 'use_offset': True, 'gcp_project_id': 'turing-nature-374608', 'gcp_bq_dataset_name': 'test_dataset2', 'target_project_id': 'turing-nature-374608', 'target_bq_dataset_name': 'test_dataset2', 'target_table_name': 'test_climate_bq2', 'target_operation': 'a'}

#     odb = OracleDatabaseConnection(**conn_details)
#     func = odb.extract(last_extract_value, **table)

#     try:
#         while True:

#             res = next(func)
#             print(" -------  ", res, "-------")
#     except StopIteration:
#         pass
        

