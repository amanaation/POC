# import required libraries and notebook
import logging
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

import os
import uuid
import pandas as pd
import numpy as np
from connection_mapping import Connectors
from datetime import datetime
from datatypes import *
from dotenv import load_dotenv
from pandas.io import sql

load_dotenv()
class ColumnMM():
    """
    This class is to do adaptive framework by adding new column and 
    filling empty string in deleting columns
    """
    def __init__(self, target_project_id, target_dataset_name, target_table_name, source, source_schema):
        """
        This function is to create global variables
        Parameters:
            cnx_config (obj) : configuration connection object
            cnx_target (obj) : target connection object
            object_name (str) : table name
        """
        self.source_schema = source_schema
        self.configuration_project_id = os.getenv("CONFIGURATION_PROJECT_ID")
        self.configuration_dataset_name = os.getenv("CONFIGURATION_GCP_PROJECT_DATASET_NAME")
        self.configuration_table = os.getenv("CONFIGURATION_TABLE")
        self.configuration_table_id = f"""{self.configuration_project_id}.{self.configuration_dataset_name}.{self.configuration_table}"""

        self.target_project_id = target_project_id
        self.target_dataset_name = target_dataset_name
        self.target_table_name = target_table_name

        self.target_table_id = f"""{self.target_project_id}.{self.target_dataset_name}.{self.target_table_name}"""
        self.source = source

    def execute(self, sql , project_id):
        """
        This function is to return dataframe out of query result
        parameters:
            sql (str) : query string to return result
            project_id (str) : GCP project ID
        returns:
            df (core.frame.DataFrame) : dataframe with source data
        """
        return pd.read_gbq(sql, project_id=project_id)

    def find_types(self,dataframes,col):
        """
        This function is to return nested and unnested columns for exploded fields
        parameters:
            dataframes (core.frame.DataFrame) : dataframe with source data
            col (list) : list of columns
        returns:
            nested (list) : nested column list
            unnested (list) : unnested column list"""
        nested=[]
        unnested=[]
        for i in col:
            dataframe=pd.DataFrame(dataframes[i])
            dataframe.replace("", np.nan, inplace=True)
            dataframe.dropna(how='all', axis=1, inplace=True)
            col_dict = dataframe.stack().apply(type).reset_index(0,drop=True).to_dict()
            unnested_columns = [k for (k,v) in col_dict.items() if v not in (dict,set,list,tuple)]
            unnested.extend(unnested_columns)
            nested_columns = list(set(col_dict.keys()) - set(unnested_columns))
            nested.extend(nested_columns)
        return nested,unnested

    # def match_columns(self,_table):
    #     """
    #     This function is to do the adaptie framework function 
    #     to add new columns and adding empty string to the deleted columns
    #     parameters:
    #         _table (core.frame.DataFrame) : dataframe with source data
    #     returns:
    #         _table (core.frame.DataFrame) : dataframe with source data
    #     """
    #     columns = set(list(_table.keys()))
    #     columns_dict = {x:x.lower() for x in columns}

    #     _sql_column=f"""SELECT column_name,`index`,data_type FROM {self.configuration_dataset_name}.{self.configuration_table}
    #     where object_name = '{self.target_table_name}' """
    #     configuration_column_mapping_df=self.execute(_sql_column, self.configuration_project_id)
    #     column_df=configuration_column_mapping_df.iloc[:, 0]
    #     val=column_df.values.tolist()
    #     index_df=configuration_column_mapping_df.iloc[:, 1]        
    #     index=0
    #     try:
    #         index=int(max(index_df.values.tolist()))
    #     except Exception as e:
    #         logging.error("initialising index value, error is %s",e)   

    #     if len(val)>0:
    #         cols_lower = set([v for (k,v) in columns_dict.items()])
    #         cols_added= cols_lower - set(val)
    #         columns_added = set([k for k in columns_dict if columns_dict[k] in cols_added])
    #         logger.info("columns_added %s",columns_added)
    #         columns_deleted=set(val)-cols_lower
    #         logger.info("columns_deleted %s",columns_deleted)
            
    #         if len(columns_added)>0:
    # #             #check if the new column added in nested or string
    #             identified_columns=[]
    #             nested,unested = self.find_types(_table,columns_added)
    #             new_list2=set(nested)-set(identified_columns)
                
    #             new_columns_added=set(unested)-set(identified_columns)
    #             new_list=set(new_columns_added).union(new_list2)
    #             logger.info("columns to be added %s",new_columns_added)
                  
    #             if len(new_columns_added)>0 : 
                    
    #                 for new_col in new_columns_added:
    #             #Added this check to validate if there are dtypes defined in the config file or gettting created in the code itself 
                        
    #                     column_data_type=str(_table.dtypes[new_col])

    #                     if column_data_type in ['object']:
    #                         datatype='VARCHAR(20)'
    #                         data_cast='string'
    #                     if column_data_type in ["int64","int32","int8"]:
    #                         datatype='BIGINT'
    #                         data_cast='int' 
    #                     elif column_data_type in ['timestamp','datetime64[ns]','datetime','datetime64','timedelta[ns]','datetime64[ns, UTC]']:
    #                         datatype='timestamp'
    #                         data_cast='timestamp'
    #                     elif column_data_type in ['date']:
    #                         datatype='date'
    #                         data_cast='date'
    #                     elif column_data_type in ['float64','double', 'currency', 'float', 'decimal','float_','float16','float32','numeric','real']:
    #                         datatype='REAL'
    #                         data_cast='float'
    #                     elif column_data_type in ['boolean','Boolean','bool','bool_']:
    #                         datatype='bool'
    #                         data_cast='boolean'
    #                     elif column_data_type in ['smallint','int', 'bigint','int_','int16','uint8','uint16','uint32','Int64','uint64''integer']:
    #                         datatype='BIGINT'
    #                         data_cast='int'                       
    #                     else:
    #                         datatype='VARCHAR(20)'
    #                         data_cast='object' 

    #                     index+=1

    #                     try:
    #                         # this has to be change based on the use case like target database syntax changes
    #                         _sql=f'alter table {self.target_project_id}.{self.target_dataset_name}.{self.target_table_name} add column {new_col} {datatype} ;'
    #                         self.execute(_sql,self.target_project_id)
    #                         logger.info("executed alter table for adding column %s", new_col)
                            
    #                     except Exception as e:
    #                         logging.error("error occured in altering table is %s", e)

    #                     _insert_dict = {"column_id": [str(uuid.uuid4())],
    #                                     "object_name": [self.target_table_name], 
    #                                     "column_name": [new_col], 
    #                                     "index": [index], 
    #                                     "inserted_by": ['adpative framework'], 
    #                                     "created_at": [datetime.now()]}
                                                   
    #                     try:
    #                         insert_df = pd.DataFrame(_insert_dict)
    #                         insert_df.to_gbq(f"{self.target_dataset_name}.{self.target_table_name}", self.target_project_id)

    #                         logger.info("successfully executed insert statement for column %s", new_col)
    #                     except Exception as e:
    #                         logging.error("error occured in insert query - %s",e)

    #         if len(columns_deleted)>0:
    #             #logging.info("columns_deleted {}".format(columns_deleted))    
    #             col_mapping={x[0]: x[2] for x in configuration_column_mapping_df.itertuples(index=False)}
    #             cast_columns = {key: value for key, value in col_mapping.items() if key in columns_deleted}
    #             _table=self.align_columns(_table,cast_columns)

    #     else:
    #         print("changed columns")

    #     return _table
    def get_source_data_type(self, fields):
        source_schema = self.source_schema
        new_fields_details = source_schema[source_schema["COLUMN_NAME"].isin(map(str.upper, fields))]
        new_fields_details["COLUMN_NAME"] = list(map(str.lower, new_fields_details["COLUMN_NAME"]))
        return new_fields_details

    def get_destination_field_type(self, source, source_data_type):
        if source == "oracle":
            datatype_mapping_obj = ORACLE2BQ

        destination_datatype = datatype_mapping_obj[source_data_type].value
        return destination_datatype

    def match_columns(self, _table):
        field_mappings_df = self.get_field_mappings()
        logger.info("Starting columns mapping")
        if field_mappings_df.empty:
            self.save_field_mappings(_table)
        else:
            existing_fields = set(field_mappings_df["column_name"].to_list())
            data_columns = set(_table.columns.to_list())

            new_fields = list(data_columns.difference(existing_fields))
            deleted_fields = list(existing_fields.difference(data_columns))

            if new_fields:
                logger.info(f"Following are the new fields added in the dataset: {new_fields}")
                self.add_new_fields(self.target_table_id, new_fields)
                logger.info(f"Adding fields {new_fields} to configuration table")
                self.save_field_mappings(_table[new_fields], field_mappings_df["object_id"][0])
                logger.info(f"Successfully added fields {new_fields} to configuration table")
            else:
                logger.info(f"No new fields to be added")

            if deleted_fields:
                logger.info(f"Following are the deleted fields in the dataset: {deleted_fields}")
                logger.info(f"Dropping deleted columns details")
                deleted_fields_mapping_details = field_mappings_df[field_mappings_df["column_name"].isin(deleted_fields)]
                self.delete_fields(deleted_fields, deleted_fields_mapping_details)
                logger.info(f"Successfully deleted all fields")

            else:
                logger.info(f"No fields to be deleted")


    def delete_fields(self, deleted_fields, deleted_fields_mapping_details):
        for field in deleted_fields:
            deleted_fields_column_id = deleted_fields_mapping_details[deleted_fields_mapping_details["column_name"] == field]["column_id"].to_list()[0]
            delete_column_in_destination_table_query = f"""alter table {self.target_table_id} drop column {field}"""
            
            try:
                logger.info(f"Dropping field {field} from destination table")
                logger.info(delete_column_in_destination_table_query)
                self.execute(delete_column_in_destination_table_query, self.target_project_id)
                logger.info(f"Successfully dropped {field}")
            except Exception as e:
                logger.info("Column does not exists")

            update_column_mapping_query = f"update {self.configuration_table_id} set deleted = True where column_id='{deleted_fields_column_id}'"
            logger.info(f"Updating field {field} in configuration_mapping table")
            logger.info(update_column_mapping_query)
            try:
                self.execute(update_column_mapping_query, self.configuration_project_id)
                logger.info(f"Successfully updated {field} in the configuration table")
            except Exception as e:
                logger.error(e)


    def add_new_fields(self, table_name, fields):
        source_field_types = self.get_source_data_type(fields)
        print("source_field_types : ", source_field_types)
        print(" fields : ", fields)

        for field in fields:
            source_field_type = source_field_types[source_field_types["COLUMN_NAME"] == field]["DATA_TYPE"].to_list()[0]

            destination_field_type = self.get_destination_field_type(self.source, source_field_type)
            logger.info(f"Adding field {field} of type {destination_field_type}")
            try:
                alter_query = f"""alter table {table_name} add column {field} {destination_field_type};"""
                self.execute(alter_query, self.configuration_project_id)
                logger.info(f"Successfully added field {field} of type {destination_field_type} to table")

            except Exception as e:
                logger.error(f"{e}")


    def get_field_mappings(self):
        """
        This function is to check the column metadata present in the config table or not
        returns:
            df (core.frame.DataFrame) : dataframe with source data
        """
        _sql_column=f"SELECT * FROM {self.configuration_table_id} where object_name = '{self.target_table_name}'"
        df = self.execute(_sql_column,self.configuration_project_id)
        return df

    def save_field_mappings(self,df, object_id=None):
        """
        This function will insert column metadata into config table if it is a first load
        parameters:
            df (core.frame.DataFrame) : dataframe with source data
        returns:
            None
        """

        info_df = pd.DataFrame()
        number_of_rows = len(df.columns)
        info_df["data_type"] = [str(dtype) for dtype in df.dtypes]
        info_df['column_name'] = df.columns
        info_df = info_df.reset_index()
        info_df["object_name"] = [self.target_table_name] * number_of_rows
        info_df["inserted_by"] = ['core_framework'] * number_of_rows
        info_df["column_id"] = [str(uuid.uuid4()) for i in range(number_of_rows)]
        if not object_id:
            info_df["object_id"] = [str(uuid.uuid4())] * number_of_rows
        else:
            info_df["object_id"] = [object_id] * number_of_rows

        info_df["index"] = [str(uuid.uuid4())] * number_of_rows

        info_df.to_gbq(f"{self.configuration_dataset_name}.{self.configuration_table}", self.configuration_project_id, if_exists='append')
        
        logger.info("configuration column mapping table updated")
            
    def align_columns(self, df, deleted_column):
        """
        This function is to align column order based on the target table
        parameters:
            original_column (list) : 
        """
        _df = df.copy()
        
        original_columns_sql = f"""SELECT `index`, column_name from {self.configuration_dataset_name}.{self.configuration_table}
            WHERE object_name = '{self.target_table_name}' order by `index`"""
        # Need to remove the "-" and substitute with "_"
        original_columns = self.execute(original_columns_sql, self.configuration_project_id)
        
        original_columns = list(original_columns['column_name'])
        _df.columns = _df.columns.str.replace('-','_', regex=True)

        _missing_columns = (list(set(original_columns) - set(_df.columns)))

        # Create empty columns in case the current recordset does not contain records with that column
        for field in _missing_columns:
            _df[field] = np.NaN

        # Reoder the table columns as per the original table definition
        _df = _df[original_columns]
        
        return _df

