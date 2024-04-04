

"""
Import Weather Job

Utilizing the api module for extraction and the db module for load, extract
'current' weather and 'forecast' weather from the Open Weather API and load it
into the 'weather' postgres table.
"""

import traceback
from pathlib import Path

import util
import api
import db


################################################################################


NAME = Path(__file__).stem

# the min/max the api accepts is -90/90, reducing for brevity
LAT_MIN = -60
LAT_MAX = 60
LON_MIN = -180
LON_MAX = 180
LAT_LON_STEP = 20

INSERT_TABLE = 'weather'


################################################################################


def main() -> None:

    util.log.setJob(NAME)

    util.log.info(f"starting '{NAME}' job")

    weather_api = api.OpenWeatherApi()

    for lat, lon in getParamSets():

        util.log.info(f"starting gets with lat, lon = {lat}, {lon}")

        import_dt = util.getNowDtStr()

        # get lat, lon current data
        current = weather_api.getCurrent(lat, lon)                              # EXTRACT
        # get lat, lon forecasts data
        forecasts = weather_api.getForecasts(lat, lon)

        # transform lat, lon current data to flat dict
        current_flat_dict = getFlatDictFromApiJson(                             # TRANSFORM
            'current', import_dt, lat, lon, current
        )
        # transform lat, lon forecasts data to flat dict
        forecasts_flat_dicts = []
        for f in forecasts:
            forecasts_flat_dicts += [
                getFlatDictFromApiJson('forecast', import_dt, lat, lon, f)
            ]
        # clean and prep raw data from api for db insert
        insert_data = db.convInsertValuesDataTypes(
            [current_flat_dict] + forecasts_flat_dicts,
            db.WEATHER_TABLE_DATA_TYPE_MAP
        )
        insert_query = db.buildInsertQuery(INSERT_TABLE, insert_data)

        db.executeQuery(insert_query, is_select=False)                          # LOAD

    util.log.info(f"finished '{NAME}' job")


################################################################################


def getParamSets() -> list[tuple[int, int]]:
    """ get sets of lat/lon params for requests """
    param_sets = []
    for lat in range(LAT_MIN, LAT_MAX + 1, LAT_LON_STEP):
        for lon in range(LON_MIN, LON_MAX + 1, LAT_LON_STEP):
            param_sets += [(lat, lon)]
    return param_sets

def getFlatDictFromApiJson(
        _type:str, import_dt:str, lat:int, lon:int, from_api:util.JsonType
) -> dict:
    """
    first step of transforming raw api json data (and other data) into single
    flat dict
    :param _type:       due to the nature of the 2 imported sources, that being
                        they're identical.  on import, _type designates source
    :param import_dt:   datetime of import
    :param lat:         latitude (from parent scope)
    :param lon:         longitude (from parent scope)
    :param from_api:    the json from the api
    :return:            flat dict
    """
    # get type specific dt value
    match _type:
        case 'current':
            dt = util.convEpochToDtStr(from_api.get('dt'))
        case 'forecast':
            dt = from_api.get('dt_txt')
    # all usage of dict.get() is to maintain handling of converting missing data
    # from api into None.  w(eather)_list is just a helper, specifically for
    # 'description' value
    w_list = from_api.get('weather', [])
    return {
        'type': _type,
        'import_dt': import_dt,
        'dt': dt,
        'lat': lat,
        'lon': lon,
        'temp': from_api.get('main', {}).get('temp'),
        'feels_like': from_api.get('main', {}).get('feels_like'),
        'humidity': from_api.get('main', {}).get('humidity'),
        'description': None if len(w_list) == 0 else w_list[0].get('description'),
        'clouds': from_api.get('clouds', {}).get('all'),
        'wind_speed': from_api.get('wind', {}).get('speed'),
        'wind_deg': from_api.get('wind', {}).get('deg')
    }


################################################################################


if __name__ == '__main__':
    try:
        main()
    except Exception:
        util.log.error(traceback.format_exc())

