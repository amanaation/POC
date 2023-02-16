from enum import Enum
from connectors.Oracle import OracleDatabaseConnection
from connectors.BigQuery import BigQuery
from connectors.GCS import GCS


class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery
    gcs = GCS
