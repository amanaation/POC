import ciso8601
import csv
import logging
import pandas as pd
import re
import sys
import os
import google.api_core
sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

# from db_connectors.connectors import Connectors
from connectors import Connectors
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from io import StringIO
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

class GCS(Connectors):
    def __init__(self, **kwargs) -> None:

        self.client = storage.Client()
        self.bucket_name = kwargs['source_gcs_bucket_name']
        self.bucket_path = f"gs://{self.bucket_name}"
        self.bucket = Bucket.from_string(f"gs://{kwargs['source_gcs_bucket_name']}", client=self.client)
        if "source_gcs_file_path" in kwargs:
            self.file_path = kwargs['source_gcs_file_path']
        else:
            self.file_path = None

        self.last_successfull_extract = {}
        
    def get_schema(self, *args):
        table_sample_data = args[1]

        schema_details = {"COLUMN_NAME": [], "DATA_TYPE": []}
        for column in table_sample_data.columns:
            schema_details["COLUMN_NAME"].append(column)
            column_type = re.findall("\'(.*?)\'",str(type(table_sample_data[column].to_list()[0])))[0]

            schema_details["DATA_TYPE"].append(column_type)

        schema_details = pd.DataFrame(schema_details)
        schema_details["NULLABLE"] = ['Y'] * len(schema_details)
        return schema_details

    def create_schema(self, *args):
        pass

    def get_files(self, path=None):
        pass

    def rectify_column_names(self, df):
        columns = list(df.columns)

        for i in range(len(columns)):
            column = columns[i]
            column = column.strip()
            if type(column[0]) is not str:
                column = "" + column[0]

            columns[i] = column
        df.columns = columns
        return df

    def read_file(self, blob):
        logger.info(f"Reasing file {blob.name} from bucket {self.bucket}")
        content = blob.download_as_string()
        content = content.decode('utf-8')

        content = StringIO(content)  #tranform bytes to string here

        datas = csv.reader(content)
        df = pd.DataFrame(datas)
        df.columns = df.iloc[0]
        df = df[1:]

        return df

    def create_bucket(self, storage_client, bucket_name):
        try:
            storage_client.create_bucket(bucket_name)
            logger.info("Created target bucket")
        except google.api_core.exceptions.Conflict:
            logger.info("Target bucket exists")

    def move_blob(self, bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """Moves a blob from one bucket to another with a new name.
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The ID of your GCS object
        # blob_name = "your-object-name"
        # The ID of the bucket to move the object to
        # destination_bucket_name = "destination-bucket-name"
        # The ID of your new GCS object (optional)
        # destination_blob_name = "destination-object-name"
        """

        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        logger.info(f"Creating target bucket {destination_bucket_name}")
        self.create_bucket(storage_client, destination_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        """
        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        # There is also an `if_source_generation_match` parameter, which is not used in this example.
        """
        destination_generation_match_precondition = 0
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
        )
        source_bucket.delete_blob(blob_name)

        print(
            "File {} in bucket {} moved to following path {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

    def handle_extract_error(self, args):
        self.move_blob(self.bucket_name, args["file_name"], os.getenv("GCS_ERROR_BUCKET_NAME"), args["file_name"])

    def update_last_successfull_extract(self, max_timestamp):
        new_timestamp = ciso8601.parse_datetime(new_timestamp)

        if "max_timestamp" not in self.last_successfull_extract:
            self.last_successfull_extract["max_timestamp"] = max_timestamp

        else:
            last_max_timestamp = ciso8601.parse_datetime(self.last_successfull_extract["max_timestamp"])
            self.last_successfull_extract["max_timestamp"] = max(new_timestamp, last_max_timestamp)

    def extract(self, last_successfull_extract, **kwargs):

        return_args = {"extraction_status": False, "file_name": blob.name}

        if last_successfull_extract:
            self.last_successfull_extract = last_successfull_extract

        blobs = self.bucket.list_blobs(prefix=self.file_path)
        for blob in blobs:

            blob_time_created = ciso8601.parse_datetime(blob.timeCreated)
            if "max_timestamp" in self.last_successfull_extract:
                last_max_timestamp = ciso8601.parse_datetime(self.last_successfull_extract["max_timestamp"])
            else:
                last_max_timestamp = blob_time_created

            # try:
            if not blob.name.endswith("/") and blob_time_created >= last_max_timestamp:
                file_data = self.read_file(blob)
                return_args["extraction_status"] = True
                self.update_last_successfull_extract(blob.timeCreated)
                yield file_data, return_args
            # except Exception as e:
            #     logger.info(f"Error occured while reading file {blob.name}")
            #     logger.info(f"Moving file to error bucket ")
            #     yield pd.DataFrame({"":[], "": []}), return_args

    def save(self, df: pd.DataFrame) -> None:
        pass


if __name__ == "__main__":
    bucket_details = {"source_gcs_bucket_name": "activision-dev",
        "source_gcs_file_path": "2023-02-08/"}

    df = pd.read_csv("/Users/amanmishra/Desktop/tredence/Python framework/DailyDelhiClimateTest.csv")    

    from pprint import pprint
    gcs = GCS(**bucket_details)
    next(gcs.extract({}))
    # print(gcs.get_schema("climate", df.head()))