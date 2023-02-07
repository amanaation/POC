from enum import Enum
from db_connectors.Oracle import OracleDatabaseConnection
from db_connectors.BigQuery import BigQuery
from db_connectors.api import API

class Connectors(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery
    api = API

