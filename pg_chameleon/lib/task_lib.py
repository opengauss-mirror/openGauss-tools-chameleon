class Pair:
    def __init__(self, first, second):
        self.first = first
        self.second = second

class CopyDataTask:
    def __init__(self, csv_file, count_rows, table, schema, select_columns, rows, task_slice=0,
                 contain_columns=False, column_split=',', idx_pair=Pair('','')):
        self.csv_file = csv_file
        self.count_rows = count_rows
        self.table = table
        self.schema = schema
        self.select_columns = select_columns
        self.rows = rows
        self.slice = task_slice
        self.contain_columns = contain_columns
        self.column_split = column_split
        self.idx_pair = idx_pair

class CreateIndexTask:
    def __init__(self, table, schema, indices, destination_schema, master_status, is_parallel_create_index,
                 auto_increment_column):
        self.table = table
        self.schema = schema
        self.indices = indices
        self.destination_schema = destination_schema
        self.master_status = master_status
        self.is_parallel_create_index = is_parallel_create_index
        self.auto_increment_column = auto_increment_column


class ReadDataTask:
    def __init__(self, destination_schema, loading_schema, schema, table):
        self.destination_schema = destination_schema
        self.loading_schema = loading_schema
        self.schema = schema
        self.table = table


class TableMetadataTask:
    def __init__(self, schema, table, count, contain_primary_key):
        self.schema = schema
        self.table = table
        self.count = count
        self.contain_primary_key = contain_primary_key


class ColumnMetadataTask:
    def __init__(self, schema, table, column_name, column_index, column_data_type, column_key):
        self.schema = schema
        self.table = table
        self.column_name = column_name
        self.column_index = column_index
        self.column_data_type = column_data_type
        self.column_key = column_key


class KeyWords:
    keyword_set = {"user", "for", "check", "all"}
