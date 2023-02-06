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

class SourceDestinationMapping(Enum):
    oracle = ORACLE2BQ


