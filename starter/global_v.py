"""

global_v.py

Defined some global variables used across modules.

"""


class Global:
    WAL_PATH = None
    SSTABLE_FOLDER = None
    METADATA_PATH = None
    MASTER_HOSTNAME = None
    MASTER_PORT = None
    TABLET_HOSTNAME = None
    TABLET_PORT = None


def get_wal_path():
    return Global.WAL_PATH


def get_sstable_folder():
    return Global.SSTABLE_FOLDER


def get_master_hostname():
    return Global.MASTER_HOSTNAME


def get_master_port():
    return Global.MASTER_PORT


def get_metadata_path():
    return Global.METADATA_PATH


def get_tablet_hostname():
    return Global.TABLET_HOSTNAME


def get_tablet_port():
    return Global.TABLET_PORT


def set_wal_path(wal_path):
    Global.WAL_PATH = wal_path


def set_sstable_folder(sstable_folder):
    Global.SSTABLE_FOLDER = sstable_folder


def set_metadata_path(metadata_path):
    Global.METADATA_PATH = metadata_path


def set_master_hostname(master_hostname):
    Global.MASTER_HOSTNAME = master_hostname


def set_master_port(master_port):
    Global.MASTER_PORT = master_port


def set_tablet_hostname(tablet_hostname):
    Global.TABLET_HOSTNAME = tablet_hostname


def set_tablet_port(tablet_port):
    Global.TABLET_PORT = tablet_port
