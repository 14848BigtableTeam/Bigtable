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
                

    def spill(self, start, mem_index, ssindex_path, wal_path, metadata_path):
        with open(metadata_path, 'f') as f:
            metadata = json.load(f)

        c_table = self.table[start :]
        row_table = classify(c_table)
        for table_name in row_table:
            for subtable_name in row_table[table_name]:
                if subtable_name != "Not":
                    if len(row_table[table_name][subtable_name]):
                        subtable_path = osp.join(Global.get_sstable_folder(), subtable_name)
                        with open(subtable_path, 'r') as f:
                            subtable = json.load(f)
                        for row in row_table[table_name][subtable_name]:
                            merge_row(subtable, row)
                        with open(subtable_path, "w") as f:
                            json.dump(subtable, f)
                else:
                    if len(row_table[table_name]["Not"]):
                        subtable_path = osp.join(Global.get_sstable_folder(), metadata[table_name]["filenames"][-1])
                        with open(subtable_path, 'r') as f:
                            subtable = json.load(f)
                        for row in row_table[table_name]["Not"]:
                            if metadata[table_name]["row_num"][-1] == 1000:
                                with open(subtable_path, 'w') as f:
                                    json.dump(subtable, f)
                                last_file = metadata[table_name]["filenames"][-1][0 : len(metadata[table_name]["filenames"][-1]) - 5]
                                last_file = last_file.split('_')
                                filenum = str(int(last_file[-1]) + 1)
                                last_file.pop()
                                filefront = '_'.join(last_file)
                                filename = filefront + "_" + filenum + ".json"
                                sstable_path = osp.join(Global.get_sstable_folder(), filename)
                                metadata[table_name]["filenames"].append(filename)
                                metadata[table_name]["row_num"].append(0)
                                with open(sstable_path, 'w+') as f:
                                    f.write('[]')
                                with open(sstable_path, 'r') as f:
                                    subtable = json.load(f)
                            add_row(subtable, row)
                            metadata[table_name]["row_num"][-1] += 1
                            mem_index[row["row"]][table_name] = filename
                        with open(sstable_path, 'r') as f:
                            json.dump(subtable, f)
        self.table = self.table[0: start]
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)
        with open(ssindex_path, 'w') as f:
            json.dump(mem_index, f) 
        with open(wal_path, 'w') as f:
            pass

def classify (c_table, mem_index):
        row_table = {}
        for row in c_table:
            row_key = row["row"]
            table_name = row["table_name"]
            column_family = row["column_families"]
            if table_name not in row_table:
                row_table[table_name] = {"Not":[]}
            if row_key in mem_index and table_name in mem_index[row_key]:
                if mem_index[row_key][table_name] not in row_table[table_name]:
                    row_table[table_name][mem_index[row_key][table_name]] = []
                row_table[table_name][mem_index[row_key][table_name]].append(row)
            else:
                row_table[table_name]["Not"].append(row)
        return row_table

def merge_row(self, subtable, row):
        row_key = row["row"]
        row_index = find_row_index(subtable, row_key)
        for column_family in subtable[row_index]["column_families"]:
            for column in subtable[row_index]["column_families"][column_family]
                subtable_list = subtable[row_index]["column_families"][column_family][column]
                subtable_list = subtable + row["column_families"][column_family][column]
                if len(subtable_list) > 5:
                    del subtable_list[0 : len(subtable_list) - 5]

def add_row(subtable, row):
    row.pop("table_name")
    subtable.append(row)


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