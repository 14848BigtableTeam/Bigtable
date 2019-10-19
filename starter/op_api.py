import json
import os.path as osp
import global_v as Global

class MemTable:

    def __init__(self):
        self.max_entries = 100
        self.length = 0
        self.table = []

    def insert(self, table_name, payload, wal_path):
        pass

    def retrieve(self, table_name, payload, mem_index):
        row = payload["row"]
        column_family = payload["column_families"]
        column = payload["column"]
        mem_find = mem_find_row_index(self.table, row)
        mem_data = []
        sstable_data = []
        if self.table[mem_find]["row"] ==  row_key and self.table[mem_find]["table_name"] == table_name:
            mem_data = self.table[mem_find]["column_families"][column_family][column]
        if row in mem_index:
            if table_name in mem_index[row]:
                with open(mem_index[row][table_name], 'r') as f:
                    sstable = json.load(f)
                sstable_date = sstable[find_row_index(sstable, row)]["column_families"][column_family][column]
        retrieve_data = sstable_data + mem_data
        if len(retrieve_data) > 5:
            del retrieve_data[0: len(retrieve_data) - 5]
        return res = { "row": row, "data": retrieve_data }




    def spill(self, mem_index, ssindex_path, wal_path, metadata_path):
        with open(metadata_path, 'f') as f:
            metadata = json.load(f)
        row_table = {}
        for row in self.table:
            if row["table_name"][]

        for row in self.table:
            row_key = row["row"]
            table_name = row["table_name"]
            if row_key in mem_index:
                if table_name in mem_index[row_key]:
                    sstable_path = osp.join(Global.get_sstable_folder(), mem_index[row_key][table_name])
                    with open(sstable_path, 'r') as f:
                        sstable = json.load(f)
                    row_index = find_row_index(sstable, row_key)
                    for column_family in row["column_families"]:
                        for column in row["column_families"][column_family]:
                            sstable_list = sstable[row_index]["column_families"][column_family][column]
                            sstable_list += row["column_families"][column_family][column]
                            if len(sstable_list) > 5:
                                del sstable_list[0 : len(sstable_list) - 5]
                    


            if metadata[table_name]["row_num"][-1] != 1000:
                metadata[table_name]["row_num"][-1] += 1
                sstable_path = osp.join(Global.get_sstable_folder(), metadata[table_name]["filenames"][-1]
            else:
                metadata[table_name]["row_num"].append(1)
                filetable = metadata[table_name]["filenames"][-1].split('_')
                filenum = str(int(filetable[-1]) + 1)
                filetable.pop()
                filefront = '_'.join(filetable)
                filename = filefront + "_" + filenum + ".json"
                sstable_path = osp.join(Global.get_sstable_folder(), filename)
                metadata[table_name]["filenames"].append(filename)
                with open(sstable_path, 'w+') as f:
                    f.write('[]')
            with open(sstable_path, 'r') as f:
                sstable = json.load(f)





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