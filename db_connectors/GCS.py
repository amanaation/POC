import csv
import logging
import pandas as pd
import sys

sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)
logger = logging.getLogger(__name__)

from db_connectors.connectors import Connectors
# from connectors import Connectors
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from io import StringIO

class GCS(Connectors):
    def __init__(self, **kwargs) -> None:

        from pprint import pprint
        pprint(kwargs)
        self.client = storage.Client()
        self.bucket_path = f"gs://{kwargs['source_gcs_bucket_name']}"
        self.bucket = Bucket.from_string(f"gs://{kwargs['source_gcs_bucket_name']}", client=self.client)
        if "source_gcs_file_path" in kwargs:
            self.file_path = kwargs['source_gcs_file_path']
        else:
            self.file_path = None

    def get_schema(self, table_name):
        pass

    def create_schema(self, table):
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
        content = blob.download_as_string()
        content = content.decode('utf-8')

        content = StringIO(content)  #tranform bytes to string here

        datas = csv.reader(content)
        df = pd.DataFrame(datas)
        df.columns = df.iloc[0]
        df = df[1:]

        return blob.name


    def extract(self, last_successfull_extract, **kwargs):
        blobs = self.bucket.list_blobs(prefix=self.file_path)
        for blob in blobs:
            if not blob.name.endswith("/"):
                names = self.read_file(blob)
                yield names

    def save(self, df: pd.DataFrame) -> None:
        pass

# if __name__ == "__main__":
#     bucket_details = {"source_gcs_file_path": "2023-02-08/"}
#     gcs = GCS("activision-dev")
#     func = gcs.extract()
#     try:
#         while True:

#             res = next(func)
#             print(" -------  ", res, "-------")
#     except StopIteration:
#         pass
#     # print(next(func))
#     # print(next(func))


    # df = gcs.extract(**bucket_details)
    # df = pd.read_csv("/Users/amanmishra/Desktop/tredence/Python framework/DailyDelhiClimateTest.csv")    
    # df = gcs.rectify_column_names(df)
    # gcs.save(df)



