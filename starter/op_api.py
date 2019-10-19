class MemTable:

    def __init__(self):
        self.max_entries = 100
        self.length = 0
        self.table = []

    def insert(self, table_name, payload, wal_path):
        pass

    def retrieve(self, table_name, payload):
        pass

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



def insert_single_cell(payload, memtable, memindex):
    pass


def retrieve_single_cell(payload, memtable, memindex):
    pass