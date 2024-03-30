

import os
import traceback
import psycopg2, psycopg2.extras

import util


################################################################################


POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']

LOG_TABLE_DATA_TYPE_MAP = {
    'dt': str,
    'job': str,
    'level': str,
    'msg': str
}
WEATHER_TABLE_DATA_TYPE_MAP = {
    'type': str,
    'import_dt': str,
    'dt': str,
    'lat': float,
    'lon': float,
    'temp': float,
    'feels_like': float,
    'humidity': float,
    'description': str,
    'clouds': float,
    'wind_speed': float,
    'wind_deg': float
}


################################################################################


def getConn() -> psycopg2.extensions.connection:
    """ get postgres connection """
    return psycopg2.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password=POSTGRES_PASSWORD
    )

def executeQuery(query:str, is_select:bool=True, is_log:bool=False) -> util.JsonType:
    """
    Execute query.
    :param query:       postgres SQL query
    :param is_select:   designates whether function returns from fetch or commits
    :param is_log:      designates whether the execution is for the 'logs' table
    :return:            returns results of a SELECT query in list of dicts format
    :notes:             - is_log is required to avoid the infinite loop of func1
                        that calls func2 that calls func1 etc
    """
    with getConn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            util.log.info(f"executing query:\n{query}") if not is_log else None
            try:
                cur.execute(query)
            except Exception:
                util.log.error(traceback.format_exc())
                exit()
            util.log.info("success! query executed") if not is_log else None
            response = [dict(row) for row in cur.fetchall()] if is_select else conn.commit()
    return response

def convInsertValuesDataTypes(insert_json:util.JsonType, data_type_map: dict) -> util.JsonType:
    """
    Used as a preperation step before buildInsertQuery() to ensure that all
    values are in their correct postgres format.
    :param insert_json:     data to be converted
    :param data_type_map:   a data type map of the table that the insert_json is
                            to be inserted into
    :return:                a converted version of insert_json
    """
    converted_insert_json = []
    for row in insert_json:
        converted_row = {}
        for k, v in row.items():
            # allow for quotation marks in insert values
            v = v.replace("'", "''") if type(v) is str else v
            if v is None:
                converted_row[k] = 'NULL'
            elif data_type_map[k] == str:
                converted_row[k] = f"'{v}'"
            elif data_type_map[k] in [int, float]:
                converted_row[k] = str(v)
        converted_insert_json += [converted_row]
    return converted_insert_json

def buildInsertQuery(table:str, insert_json:util.JsonType) -> str:
    """
    Build insert query.
    :param table:       name of table
    :param insert_json: list of dicts, where each dict in the list represents a
                        row and each key in a dict represents a column
    :return:            a complete sql insert query as string
    :notes:             - this function assumes user accuracy...
                        - assumes that dict values of insert_json are in proper
                        format.  string and timestamp types should be prefixed
                        and suffixed with single quotations
                        - assumes that all dict keys (columns) are identical
                        from one dict to the next
    """
    insert_format = "INSERT INTO {table} ({columns}) VALUES {values}"
    columns = ', '.join(insert_json[0].keys())
    values = ', '.join([f"({', '.join(row.values())})" for row in insert_json])
    return insert_format.format(table=table, columns=columns, values=values)

