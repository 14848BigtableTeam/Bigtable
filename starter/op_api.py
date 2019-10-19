class MemTable:

    def __init__(self):
        self.max_entries = 100
        self.length = 0
        self.table = []

    def insert(self, table_name, payload, wal_path): 
        pass

    def retrieve(self, table_name, payload):
        row = payload["row"]
        column_family = payload["column_family"]
        column = payload["column"]
        Memfind = find_row_index(self.table, row)
        if self.table[Memfind]["row"] !=  


    def spill(self, ssindex_path, wal_path):
        pass


def find_row_index(table, row_key):
    left = 0
    right = len(table) - 1
    while left <= right:
        mid = left + (right-left) // 2
        if table[mid]['row'] < row_key:
            left = mid + 1
        else:
            right = mid - 1
    return left

def mem_find_row_index(table, row_key, table_name):
    left = 0
    right = len(table) - 1
    while left <= right:
        mid = left + (right-left) // 2
        if table[mid]['row'] < row_key:
            left = mid + 1
        else:
            right = mid - 1
    if left == len(table) or table[left]['row'] != row_key:
        return left
    else: 
        tmpindex = left
        while tmpindex < len(table) and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] != table_name:
            tmpindex += 1
        if tmpindex < len(table) and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] == table_name:
            return tmpindex
        tmpindex = left
        while tmpindex >= 0 and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] != table_name:
            tmpindex -= 1
        if tmpindex >= 0 and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] != table_name:
            return tmpindex
        return left


def insert_single_cell(payload, memtable, memindex):
    pass


def retrieve_single_cell(payload, memtable, memindex):
    pass