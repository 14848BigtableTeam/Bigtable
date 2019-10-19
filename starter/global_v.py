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


def set_wal_path(WAL_PATH):
    Global.WAL_PATH = WAL_PATH


def set_sstable_folder(SSTABLE_FOLDER):
    Global.SSTABLE_FOLDER = SSTABLE_FOLDER


def set_metadata_path(METADATA_PATH):
    Global.METADATA_PATH = METADATA_PATH