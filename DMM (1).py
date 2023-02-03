# Databricks notebook source
# MAGIC %run "/Users/krishna.r@tredence.com/ATOM_ideathon/DMM_common"

# COMMAND ----------

# import required libraries and notebook
import uuid
import pandas as pd
from pandas.io import sql
import numpy as np
import json
from datetime import datetime


# COMMAND ----------


class DynamicMM():
    """
    This class is to do adaptive framework by adding new column and 
    filling empty string in deleting columns
    """
    def __init__(self, cnx_config, cnx_target, object_name, schema_name):
        """
        This function is to create global variables
        Parameters:
            cnx_config (obj) : configuration connection object
            cnx_target (obj) : target connection object
            object_name (str) : table name
        """
        self.config_connection = cnx_config
        self.target_connection = cnx_target
        self.object_name = object_name
        self.schema_name = schema_name

        
    def get_df(self, sql , cnx):
        """
        This function is to return dataframe out of query result
        parameters:
            sql (str) : query string to return result
            cnx (obj) : connection object
        returns:
            df (core.frame.DataFrame) : dataframe with source data
        """
        return pd.read_sql(sql,con = cnx)    
    
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

    def match_columns(self,_table):
        """
        This function is to do the adaptie framework function 
        to add new columns and adding empty string to the deleted columns
        parameters:
            _table (core.frame.DataFrame) : dataframe with source data
        returns:
            _table (core.frame.DataFrame) : dataframe with source data
        """
        columns = set(list(_table.keys()))
        columns_dict = {x:x.lower() for x in columns}

        _sql_column=f"""SELECT column_name,`index`,data_type FROM `idmm-tmt-uat-config`.column_mapping
        where object_name = '{self.object_name}' """
        column_mapping_df=self.get_df(_sql_column, self.config_connection)
        column_df=column_mapping_df.iloc[:, 0]
        val=column_df.values.tolist()   
        index_df=column_mapping_df.iloc[:, 1]        
        index=0
        try:
            index=int(max(index_df.values.tolist()))
        except Exception as e:
            logger.error("initialising index value, error is %s",error)   

        if len(val)>0:           
            cols_lower = set([v for (k,v) in columns_dict.items()])
            cols_added= cols_lower - set(val)
            columns_added = set([k for k in columns_dict if columns_dict[k] in cols_added])
            logger.info("columns_added %s",columns_added)
            columns_deleted=set(val)-cols_lower
            logger.info("columns_deleted %s",columns_deleted)
            
            if len(columns_added)>0:
    #             #check if the new column added in nested or string
                identified_columns=[]
                nested,unested = self.find_types(_table,columns_added)
                new_list2=set(nested)-set(identified_columns)
                
                new_columns_added=set(unested)-set(identified_columns)
                new_list=set(new_columns_added).union(new_list2)
                logger.info("columns to be added %s",new_columns_added)
                  
                if len(new_columns_added)>0 : 
                    
                    for new_col in new_columns_added:
                #Added this check to validate if there are dtypes defined in the config file or gettting created in the code itself 
                        
                        column_data_type=str(_table.dtypes[new_col])

                        if column_data_type in ['object']:
                            datatype='VARCHAR(20)'
                            data_cast='string'
                        if column_data_type in ["int64","int32","int8"]:
                            datatype='BIGINT'
                            data_cast='int' 
                        elif column_data_type in ['timestamp','datetime64[ns]','datetime','datetime64','timedelta[ns]','datetime64[ns, UTC]']:
                            datatype='timestamp'
                            data_cast='timestamp'
                        elif column_data_type in ['date']:
                            datatype='date'
                            data_cast='date'
                        elif column_data_type in ['float64','double', 'currency', 'float', 'decimal','float_','float16','float32','numeric','real']:
                            datatype='REAL'
                            data_cast='float'
                        elif column_data_type in ['boolean','Boolean','bool','bool_']:
                            datatype='bool'
                            data_cast='boolean'
                        elif column_data_type in ['smallint','int', 'bigint','int_','int16','uint8','uint16','uint32','Int64','uint64''integer']:
                            datatype='BIGINT'
                            data_cast='int'                       
                        else:
                            datatype='VARCHAR(20)'
                            data_cast='object' 

                        index+=1

                        try:
                            # this has to be change based on the use case like target database syntax changes
                            _sql='alter table {object_name} add column {column_name} {datatype} ;'.format(column_name=new_col,datatype=datatype,object_name = self.object_name)
                            self.execute_sql_script(_sql,self.target_connection)
                            logger.info("executed alter table for adding column %s", new_col)
                            
                        except Exception as e:
                            logger.error("error occured in altering table is %s", e)
                        _sql_insert="""INSERT INTO `idmm-tmt-uat-config`.`column_mapping`
                                    (`column_id`,
                                    `object_name`,
                                    `column_name`,
                                    `data_type`,
                                    `index`,
                                    `inserted_by`,
                                    `created_at`)
                                    VALUES
                                    ('{column_id}',
                                    '{object_name}',
                                    '{column_name}',
                                    '{data_type}',
                                    '{index}',
                                    '{inserted_by}',
                                    '{timestamp}');"""   
                        _id = str(uuid.uuid4())
                        object_name = self.object_name
                        column_name = new_col
                        data_type = data_cast
                        #str(df[new_col].dtype)
                        inserted_by = 'adpative framework'
                        
                           
                        try:
                            self.execute_sql_script(_sql_insert.format(column_id = _id, object_name = object_name, column_name = column_name, data_type = data_type, index = index, inserted_by = inserted_by, timestamp = datetime.now()), self.config_connection)
                            
                            logger.info("successfully executed insert statement for column %s", new_col)
                        except Exception as e:
                            logger.error("error occured in insert query - %s",e)

            if len(columns_deleted)>0:
                #logging.info("columns_deleted {}".format(columns_deleted))    
                col_mapping={x[0]: x[2] for x in column_mapping_df.itertuples(index=False)}
                cast_columns = {key: value for key, value in col_mapping.items() if key in columns_deleted}
                _table=self.align_columns(_table,cast_columns)

        return _table
    
    def execute_sql_script(self,sql,connection):
        """
        this function is to execute sql script
        parameters:
            sql (str) : sql query string
            connection (obj) : connection object
        returns:
            None"""
        connection.execute(sql)
        return None
    
    def get_field_mappings(self):
        """
        This function is to check the column metadata present in the config table or not
        returns:
            df (core.frame.DataFrame) : dataframe with source data
        """
        _sql_column=f"SELECT * FROM `idmm-tmt-uat-config`.column_mapping where object_name = '{self.object_name}'"
        df = self.get_df(_sql_column,self.config_connection)
        return df

    def save_field_mappings(self,df):
        """
        This function will insert column metadata into config table if it is a first load
        parameters:
            df (core.frame.DataFrame) : dataframe with source data
        returns:
            None
        """
        cols = "`,`".join([str(i) for i in df.columns.tolist()])
        sql = """INSERT INTO `idmm-tmt-uat-config`.`column_mapping`
            (`column_id`,
            `object_name`,
            `column_name`,
            `data_type`,
            `index`,
            `inserted_by`,
            `created_at`)
            VALUES
            ('{column_id}',
            '{object_name}',
            '{column_name}',
            '{data_type}',
            '{index}',
            '{inserted_by}',
            '{timestamp}');"""
        for index, column in enumerate(df.columns):
            _id = str(uuid.uuid4())
            object_name = self.object_name
            column_name = column
            data_type = str(df[column].dtype)
            inserted_by = 'core_framework'
            self.config_connection.execute(sql.format(column_id = _id, object_name = object_name, column_name = column_name, data_type = data_type, index = index, inserted_by = inserted_by, timestamp = datetime.now()))
        logger.info("configuration column mapping table updated")
            
    def align_columns(self, df, deleted_column):
        """
        This function is to align column order based on the target table
        parameters:
            original_column (list) : 
        """
        _df = df.copy()
        
        original_columns_sql = f"""SELECT `index`, column_name from column_mapping
            WHERE object_name = '{self.object_name}' order by `index`"""
        # Need to remove the "-" and substitute with "_"
        original_columns = self.get_df(original_columns_sql, self.config_connection)
        
        original_columns = list(original_columns['column_name'])
        _df.columns = _df.columns.str.replace('-','_', regex=True)

        _missing_columns = (list(set(original_columns) - set(_df.columns)))

        # Create empty columns in case the current recordset does not contain records with that column
        for field in _missing_columns:
            _df[field] = np.NaN

        # Reoder the table columns as per the original table definition
        _df = _df[original_columns]
        
        return _df



# COMMAND ----------


