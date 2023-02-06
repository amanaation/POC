from connection_mapping import Connectors

class Loader:
    def __init__(self, table) -> None:
        target = Connectors[table["destination"]].value
        self.target_obj = target(**table)

    def create_schema(self, schema_df, source):
        self.target_obj.create_schema(schema_df, source)

    def load(self, df):
        self.target_obj.save(df)



