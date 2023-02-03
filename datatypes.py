from enum import Enum


class ORACLE2BQ(Enum):
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


class REDSHIFT2BQ(Enum):
    VARCHAR2 = "STRING"
    NVARCHAR2 = "STRING"

class AWS2BQ(Enum):
    pass




