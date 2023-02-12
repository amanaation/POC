import logging
import oracledb
import pandas as pd
import os
import json

from dotenv import load_dotenv
from sqlalchemy import create_engine
# from db_connectors.connectors import Connectors
from connectors import Connectors
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
        self.last_successfull_extract = {}
        logger.info("Connection created successfully with source")

    def get_connection_details(self):
        conn_details = {"user": os.getenv("DBUSER"),
                "password": os.getenv("PASSWORD"),
                "host": os.getenv("HOST"),
                "port": os.getenv("PORT"),
                "DB": os.getenv("DB")
                }

        return conn_details

    def get_schema(self, *args) -> pd.DataFrame:
        """
            Connects to the source database, and gets the source schema
            Parameters
            -----------
                table_name: Name of the source table
            Returns
            ---------
                pd.DataFrame: Schema details of source table
        """

        table_name = args[0]
        schema_details_query = f"""SELECT column_name, data_type, nullable
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

    def add_dynamic_limit(self, 
                          incremental_clause, 
                          table_name, 
                          batch_size, 
                          group_by_column, 
                          group_by_format):

        dynamic_limit = self.get_dynamic_limit(incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        print(dynamic_limit)

    def create_dynamic_limit_query(self, incremental_clause, table_name, batch_size, group_by_column, group_by_format):
        if incremental_clause:
            dynamic_limit_query = """select TRUNC({}, '{}') as group_by_timestamp, 
                                    count(*) as row_count from {} where {} group by TRUNC({}, '{}')  
                                    order by group_by_timestamp desc; """
            dynamic_limit_query = dynamic_limit_query.format(group_by_column, group_by_format, table_name, incremental_clause, group_by_column,  group_by_format)

        else:
            dynamic_limit_query = """select TRUNC({}, '{}') as group_by_timestamp, 
                                    count(*) as row_count from {} group by TRUNC({}, '{}')  
                                    order by group_by_timestamp desc; """

            dynamic_limit_query = dynamic_limit_query.format(group_by_column, group_by_format, table_name, group_by_column,  group_by_format)

        return dynamic_limit_query

    def get_dynamic_limit(self, incremental_clause, table_name, batch_size, group_by_column, group_by_format):
        group_by_formats = ["hh", "dd", "mm", "yy"]

        for i in range(len(group_by_formats)):
            group_by_format = group_by_formats[i]
            dynamic_limit_query = self.create_dynamic_limit_query( incremental_clause, table_name, batch_size, group_by_column, group_by_format)

            result = self.execute_query(dynamic_limit_query)
            if result["group_by_timestamp"][0] >= batch_size:
                if i:
                    i = i-1
                break
        group_by_format = group_by_formats[i]
        final_dynamic_limit_query = self.create_dynamic_limit_query( incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        result = self.execute_query(final_dynamic_limit_query)

        return result

    def execute_batches(self, 
                        query: str, 
                        incremental_clause, 
                        table_name,
                        batch_size: int, 
                        incremental_columns: dict, 
                        group_by_column, 
                        group_by_format) -> pd.DataFrame :

        offset = 0
        result_df = pd.DataFrame()
        incremental_columns = list(incremental_columns.keys())
        incremental_columns = ', '.join(incremental_columns)
        print("incremental_columns : ", incremental_columns)

        yield self.get_dynamic_limit(incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        # while True:
        #     logger.info(f"Fetching rows between {offset} and {offset + batch_size}")
        #     updated_query = query + f" order by {incremental_columns} " + f" OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        #     pd_result = self.execute_query(updated_query)
        #     yield pd_result

        #     result_df = pd.concat([result_df, pd_result], ignore_index=True)
        #     offset += batch_size
        #     if len(pd_result) < batch_size:
        #         break
        
        # return 

    def handle_extract_error(self):
        pass

    def update_last_successfull_extract(self, incremental_columns, result_df):
        print("Updating values")
        for incremental_column in incremental_columns:
            incremental_column_last_batch_fetched_value = result_df[incremental_column.upper()].max()
            if incremental_column in self.last_successfull_extract:
                # print("incremental_column : ", incremental_column, last_fetched_values[incremental_column])
                self.last_successfull_extract[incremental_column] = max([self.last_successfull_extract[incremental_column], incremental_column_last_batch_fetched_value])
            else:
                self.last_successfull_extract[incremental_column] = incremental_column_last_batch_fetched_value

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

        if last_successfull_extract:
            self.last_successfull_extract = last_successfull_extract

        batch_size = table["batch_size"]
        query = table["query"]
        incremental_columns = table["incremental_column"]

        # If there is a last successfull extract then add incremental clause
        incremental_clause = ""
        if last_successfull_extract:
            incremental_clause = self.get_incremental_clause(incremental_columns, last_successfull_extract)

            if "where" in query.lower():
                query += f"and   {incremental_clause}"
            else:
                query += f" where {incremental_clause}"

        logger.info(f"Running query : {query}")
        return_args = {"extraction_status": False}
        
        if not table["use_offset"]:
            yield self.execute_query(query)
        else:
            # try:
                print("batch execution")
                func = self.execute_batches(query, incremental_clause, table["name"], batch_size, incremental_columns, table["grouby_column"], table["grouby_format"])
                while True:
                    return_args["extraction_status"] = True
                    print("batch execution------")

                    result_df = next(func)

                    self.update_last_successfull_extract(incremental_columns, result_df)
                    yield result_df, return_args 
            # except StopIteration:
            #     pass
            # except Exception as e:
            #     yield pd.DataFrame(), return_args 


# """
if __name__ == "__main__":
    import os
    conn_details = {"user": os.getenv("DBUSER"),
                    "password": os.getenv("PASSWORD"),
                    "host": os.getenv("HOST"),
                    "port": os.getenv("PORT"),
                    "DB": os.getenv("DB")
                    }

    pprint(conn_details)
    # last_extract_value = {"tdate": "2022-04-24 00:00:00", "meantemp": "135"}
    last_extract_value = None

    table = {'name': 'climate', 
            'extract': True, 
            'source': 'oracle', 
            'source_type': 'db', 
            'destination': 'bq', 
            'timestamp_column': 'tdate', 
            'timestamp_format': 'YYYY-MM-DD HH24:MI:SS', 

            'grouby_column': 'tdate', 
            'grouby_format': 'YY', 

            'incremental_column': {'tdate': {'column_type': 'timestamp', 'column_format': 'YYYY-MM-DD HH24:MI:SS'}, 'meantemp': {'column_type': 'id'}}, 
            'incremental_type': 'timestamp', 
            'incremental_column_format': 'YYYY-MM-DD HH24:MI:SS', 
            'frequency': 'daily', 
            'query': 'select * from climate', 
            'batch_size': 300, 
            'use_offset': True, 
            'gcp_project_id': 'turing-nature-374608', 
            'gcp_bq_dataset_name': 'test_dataset2', 
            'target_project_id': 'turing-nature-374608', 
            'target_bq_dataset_name': 'test_dataset2', 
            'target_table_name': 'test_climate_bq2', 
            'target_operation': 'a'}

    odb = OracleDatabaseConnection(**conn_details)

    # odb.execute_batch("select * from climate", 500, [{"tdate": {"column_type":"timestamp", "column_format": 'YYYY-MM-DD HH24:MI:SS'}}], "tdate", "DD")
    # odb.test()
    # odb.execute_batch("select * from climate", 500, [{"tdate": {"column_type":"timestamp", "column_format": 'YYYY-MM-DD HH24:MI:SS'}}], "tdate", "DD")
    next(odb.extract(last_extract_value, **table))
    print("-----------")

#     func = odb.extract(last_extract_value, **table)

#     try:
#         while True:

#             res = next(func)
#             print(" -------  ", res, "-------")
#     except StopIteration:
#         pass
        
# """
