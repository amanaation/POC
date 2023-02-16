from enum import Enum


class ORACLE2BQ(Enum):
    """
        Enum class with all the dataype mappings between oracle to Bigquery
    """
    VARCHAR2 = "STRING"
    NVARCHAR2 = "STRING"
    CHAR = "STRING"
    NCHAR = "STRING"
    CLOB = "STRING"
    NCLOB = "STRING"
    INTEGER = "INT64"
    SHORTINTEGER = "INT64"
    LONGINTEGER = "INT64"
    NUMBER = "NUMERIC"
    FLOAT = "FLOAT64"
    BINARY_DOUBLE = "FLOAT64"
    BINARY_FLOAT = "FLOAT64"
    LONG = "BYTES"
    BLOB = "BYTES"
    BFILE = "STRING"
    DATE = "DATETIME"
    TIMESTAMP = "TIMESTAMP"


class GCS2BQ(Enum):
    str = "STRING"
    float = "FLOAT64"
    int = "INT64"
    date = "DATETIME"
    timestamp = "TIMESTAMP"
    boolean = "BOOL"


class SourceDestinationTypeMapping(Enum):
    oracle = ORACLE2BQ
    gcs = GCS2BQ
