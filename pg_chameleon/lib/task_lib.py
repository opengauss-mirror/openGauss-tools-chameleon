class copy_data_task:
    def __init__(self, csv_file, count_rows, table, schema, select_columns, slice=0):
        self.csv_file = csv_file
        self.count_rows = count_rows
        self.table = table
        self.schema = schema
        self.select_columns = select_columns
        self.slice = slice

class create_index_task:
    def __init__(self, table, schema, indices, destination_schema, master_status):
        self.table = table
        self.schema = schema
        self.indices = indices
        self.destination_schema = destination_schema
        self.master_status = master_status

class read_data_task:
    def __init__(self, destination_schema, loading_schema, schema, table):
        self.destination_schema = destination_schema
        self.loading_schema = loading_schema
        self.schema = schema
        self.table = table
