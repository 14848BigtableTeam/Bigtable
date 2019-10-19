class Global:
    WAL_PATH = None
    SSTABLE_FOLDER = None
    METADATA_PATH = None


def get_wal_path():
    return Global.WAL_PATH


def get_sstable_folder():
    return Global.SSTABLE_FOLDER


def get_metadata_path():
    return Global.METADATA_PATH


def set_wal_path(wal_path):
    Global.WAL_PATH = wal_path


def set_sstable_folder(sstable_folder):
    Global.SSTABLE_FOLDER = sstable_folder


def set_metadata_path(metadata_path):
    Global.METADATA_PATH = metadata_path