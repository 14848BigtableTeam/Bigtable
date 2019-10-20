import json

import os.path as osp
import global_v as Global


class MemTable:

    def __init__(self):
        self.max_entries = 100
        self.table = []
        self.cell_data_max_num = 5

    def insert(self, table_name, payload, metadata, ssindex_path, wal_path):
        column_family_key, column_key, row_key, cell_data = payload['column_family'], \
                                                            payload['column'], \
                                                            payload['row'], \
                                                            payload['data']
        # find index to insert a cell
        row_insert_index = mem_find_row_index(table=self.table, row_key=row_key, table_name=table_name)
        if row_insert_index == len(self.table) or self.table[row_insert_index]['row'] != row_key or \
                self.table[row_insert_index]['table_name'] != table_name:
            # size of memtable has reached to the maximum value
            if len(self.table) == self.max_entries:
                self.spill(ssindex_path=ssindex_path, wal_path=wal_path, metadata=metadata)
            # cannot find a specific row, insert a new one
            new_row = {
                'row': row_key,
                'table_name': table_name,
                'column_families': {}
            }
            # construct empty column families and columns container
            for column_family_info in metadata[table_name]['column_families']:
                new_row['column_families'][column_family_info['column_family_key']] = {}
                for metadata_column_key in column_family_info['columns']:
                    new_row['column_families'][column_family_info['column_family_key']][metadata_column_key] = []
            # insert new row to table
            self.table.insert(row_insert_index, new_row)

        # write change into wal
        with open(wal_path, 'a') as fp:
            new_wal = {'table_name': table_name}
            new_wal.update(payload)
            new_wal_line = '{}\n'.format(json.dumps(new_wal))
            fp.write(new_wal_line)

        # inject cell data
        cell_data_l = self.table[row_insert_index]['column_families'][column_family_key][column_key]
        if len(cell_data_l) == self.cell_data_max_num:
            cell_data_l.pop(0)
        cell_data_l += cell_data

    def retrieve(self, table_name, payload, mem_index):
        row = payload["row"]
        column_family = payload["column_family"]
        column = payload["column"]

        mem_find = mem_find_row_index(self.table, row, table_name)
        mem_data = []
        sstable_data = []
        if mem_find < len(self.table) and self.table[mem_find]["row"] == row and self.table[mem_find][
            "table_name"] == table_name:
            mem_data = self.table[mem_find]["column_families"][column_family][column]
        if row in mem_index:
            if table_name in mem_index[row]:
                with open(mem_index[row][table_name], 'r') as f:
                    sstable = json.load(f)
                sstable_data = sstable[find_row_index(sstable, row)]["column_families"][column_family][column]
        retrieve_data = sstable_data + mem_data
        if len(retrieve_data) > 5:
            del retrieve_data[0: len(retrieve_data) - 5]
        return {"row": row, "data": retrieve_data}

    # def spill(self, mem_index, ssindex_path, wal_path, metadata_path):
    #     with open(metadata_path, 'f') as f:
    #         metadata = json.load(f)
    #     row_table = {}
    #     for row in self.table:
    #         if row["table_name"][]
    #
    #     for row in self.table:
    #         row_key = row["row"]
    #         table_name = row["table_name"]
    #         if row_key in mem_index:
    #             if table_name in mem_index[row_key]:
    #                 sstable_path = osp.join(Global.get_sstable_folder(), mem_index[row_key][table_name])
    #                 with open(sstable_path, 'r') as f:
    #                     sstable = json.load(f)
    #                 row_index = find_row_index(sstable, row_key)
    #                 for column_family in row["column_families"]:
    #                     for column in row["column_families"][column_family]:
    #                         sstable_list = sstable[row_index]["column_families"][column_family][column]
    #                         sstable_list += row["column_families"][column_family][column]
    #                         if len(sstable_list) > 5:
    #                             del sstable_list[0 : len(sstable_list) - 5]
    #
    #
    #
    #         if metadata[table_name]["row_num"][-1] != 1000:
    #             metadata[table_name]["row_num"][-1] += 1
    #             sstable_path = osp.join(Global.get_sstable_folder(), metadata[table_name]["filenames"][-1]
    #         else:
    #             metadata[table_name]["row_num"].append(1)
    #             filetable = metadata[table_name]["filenames"][-1].split('_')
    #             filenum = str(int(filetable[-1]) + 1)
    #             filetable.pop()
    #             filefront = '_'.join(filetable)
    #             filename = filefront + "_" + filenum + ".json"
    #             sstable_path = osp.join(Global.get_sstable_folder(), filename)
    #             metadata[table_name]["filenames"].append(filename)
    #             with open(sstable_path, 'w+') as f:
    #                 f.write('[]')
    #         with open(sstable_path, 'r') as f:
    #             sstable = json.load(f)


def find_row_index(table, row_key):
    left = 0
    right = len(table) - 1
    while left <= right:
        mid = left + (right - left) // 2
        if table[mid]['row'] < row_key:
            left = mid + 1
        else:
            right = mid - 1
    return left


def mem_find_row_index(table, row_key, table_name):
    left = 0
    right = len(table) - 1
    while left <= right:
        mid = left + (right - left) // 2
        if table[mid]['row'] < row_key:
            left = mid + 1
        else:
            right = mid - 1
    if left == len(table) or table[left]['row'] != row_key:
        return left
    else:
        tmpindex = left
        while tmpindex < len(table) and table[tmpindex]['row'] == row_key and table[tmpindex][
            'table_name'] != table_name:
            tmpindex += 1
        if tmpindex < len(table) and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] == table_name:
            return tmpindex
        tmpindex = left
        while tmpindex >= 0 and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] != table_name:
            tmpindex -= 1
        if tmpindex >= 0 and table[tmpindex]['row'] == row_key and table[tmpindex]['table_name'] != table_name:
            return tmpindex
        return left


def retrieve_single_cell(payload, memtable, memindex):
    pass
