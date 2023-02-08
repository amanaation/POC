from enum import Enum
from db_connectors.Oracle import OracleDatabaseConnection
from db_connectors.BigQuery import BigQuery
from db_connectors.GCS import GCS

class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery
    gcs = GCS

