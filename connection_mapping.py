from enum import Enum
from db_connectors.Oracle import OracleDatabaseConnection
from db_connectors.BigQuery import BigQuery

class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery

