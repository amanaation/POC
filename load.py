from connection_mapping import Target

class Loader:
    def __init__(self, table) -> None:
        target = Target[table["destination"]].value
        self.target_obj = target(**table)

    def create_schema(self, schema_df, source):
        self.target_obj.create_schema(schema_df, source)

    def load(self, df):
        self.target_obj.save(df)



