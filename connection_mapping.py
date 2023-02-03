from enum import Enum
from source.Oracle import OracleDatabaseConnection
from source.BigQuery import BigQuery

from targets.BigQuery import BigQuery as TargetBq
from targets.Redshift import Redshift as TargetRedshift
class Source(Enum):
    oracle = OracleDatabaseConnection
    bq = BigQuery


class Target(Enum):
    bq = TargetBq
    redshift: TargetRedshift
