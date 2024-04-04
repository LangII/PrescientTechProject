

"""
utilities module

Miscellaneous tools.
"""

import logging
import json
from typing import NewType, Union
from datetime import datetime, UTC


################################################################################


LOG_FILE = '/stuff/PrescientTechProject/logs.log'
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

JsonType = NewType('JsonType', Union[dict, list[dict]])


################################################################################


class Log:
    """
    :attr job:      designates value inserted into 'job' column of 'logs' table.
                    set with setJob()
    :attr to_table: designates if calls to info() or error() will have data also
                    sent to database table.  set with setToTable()
    :notes:         - the current setup has the timezone for DB logs set to UTC,
                    while logs to file and console are local
    """
    def __init__(self):
        logging.basicConfig(
            filename=LOG_FILE, level='INFO', format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )
        self.log = logging.getLogger(__name__)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(logging.Formatter(
            fmt=LOG_FORMAT, datefmt=DATE_FORMAT
        ))
        self.log.addHandler(self.stream_handler)
        self.job = None
        self.to_table = True

    def setLevel(self, level:str) -> None:
        self.log.setLevel(level)

    def setToTable(self, to_table:bool) -> None:
        self.to_table = to_table

    def setJob(self, job:str) -> None:
        self.job = job

    def info(self, msg:str, to_table:bool=None) -> None:
        """
        sort of a wrapper for standard log.info() to additionally upload info
        data to database 'logs' table
        :param msg:         info message
        :param to_table:    designates if data will also be sent to database
                            'logs' table.  if not provided, default to
                            self.to_table
        """
        to_table = self.to_table if to_table is None else to_table
        self.log.info(msg)
        self.logToTable('info', msg) if to_table else None

    def error(self, msg:str, to_table:bool=None) -> None:
        """
        sort of a wrapper for standard log.error() to additionally upload error
        data to database 'logs' table
        :param msg:         error message
        :param to_table:    designates if data will also be sent to database
                            'logs' table.  if not provided, default to
                            self.to_table
        """
        to_table = self.to_table if to_table is None else to_table
        self.log.error(msg)
        self.logToTable('error', msg) if to_table else None

    def logToTable(self, level:str, msg:str) -> None:
        """ upload log to db """
        log_data = [{'dt': getNowDtStr(), 'job': self.job, 'level': level, 'msg': msg}]
        log_data = db.convInsertValuesDataTypes(log_data, db.LOG_TABLE_DATA_TYPE_MAP)
        insert_query = db.buildInsertQuery('logs', log_data)
        db.executeQuery(insert_query, is_select=False, is_log=True)

log = Log()

def convDtToStr(dt:datetime) -> str:
    """ convert datetime object to string """
    return dt.strftime(DATE_FORMAT)

def getNowDtStr() -> str:
    """ get 'now' datetime as string """
    return convDtToStr(datetime.now(UTC))

def convEpochToDtStr(epoch:int) -> str:
    """ convert datetime as epoch to string """
    return convDtToStr(datetime.fromtimestamp(epoch, UTC))

def getFileAsStr(file_name:str) -> str:
    with open(file_name, 'r') as file:
        return file.read()

def prettyPrintJson(_json:JsonType) -> None:
    print(json.dumps(_json, default=str, indent=4))


################################################################################


# to avoid circular import
import db

