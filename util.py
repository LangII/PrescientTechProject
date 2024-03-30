

import logging
import json
from typing import NewType, Union
from datetime import datetime, UTC


################################################################################


LOG_FORMAT = '[%(asctime)s] [%(levelname)s] %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

JsonType = NewType("JsonType", Union[dict, list[dict]])


################################################################################


class Log:
    """
    :notes: - the current setup has the timezone for DB logs set to UTC, while
            logs to file and console are local
    """
    def __init__(self):
        logging.basicConfig(
            filename='logs.log',
            level=logging.INFO,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
        )
        self.log = logging.getLogger(__name__)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(logging.Formatter(
            fmt=LOG_FORMAT, datefmt=DATE_FORMAT
        ))
        self.log.addHandler(self.stream_handler)
        self.job = None

    def setJob(self, job:str) -> None:
        self.job = job

    def info(self, msg:str) -> None:
        self.log.info(msg)
        self.logToTable('info', msg)

    def error(self, msg:str) -> None:
        self.log.error(msg)
        self.logToTable('error', msg)

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

