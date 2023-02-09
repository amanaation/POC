import pandas as pd

class Transformation:

    def __init__(self) -> None:
        pass

    def rectify_column_names(self, table:pd.DataFrame) -> pd.DataFrame:
        columns = []

        for column in table.columns:
            column = column.strip()

            columns.append(column)
        table.columns = columns

        return table

    def transform(self, _table):
        _table = self.rectify_column_names(_table)
        return _table

        