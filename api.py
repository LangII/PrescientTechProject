

"""
API module

The meat n' potatoes is the _get() method.  And most of the _get() method's
heavy lifting is done with the backoff decorator.  It handles the majority of
all request issues.

DOCS = {
    'home': 'https://openweathermap.org/',
    'current': 'https://openweathermap.org/current',
    'forecast': 'https://openweathermap.org/forecast5'
}
"""

import os
import requests
import backoff
import traceback
from requests.exceptions import RequestException, ConnectionError, SSLError

import util


################################################################################


BASE_URL = 'https://api.openweathermap.org/'
FORECAST_URL_EXT = 'data/2.5/forecast'
CURRENT_URL_EXT = 'data/2.5/weather'
KEY = os.environ['OPEN_WEATHER_API_KEY']


################################################################################


class OpenWeatherApi:
    def __init__(
        self, key:str=KEY, base_url:str=BASE_URL,
        forecast_url_ext:str=FORECAST_URL_EXT, current_url_ext:str=CURRENT_URL_EXT
    ):
        self._key = key
        self.base_url = base_url
        self.forecast_url_ext = forecast_url_ext
        self.current_url_ext = current_url_ext

    @backoff.on_exception(
        backoff.expo, (RequestException, ConnectionError, SSLError), max_time=300,
        giveup=lambda e: 400 <= e.response.status_code < 500
    )
    def _get(self, url:str, params:dict=None) -> util.JsonType:
        """
        Internal method for handling get requests.
        :param url:     request url
        :param params:  request params
        :return:        response of request in json format
        """
        util.log.info(f"making get request to url '{url}' with params {params}")
        params['appid'] = self._key
        try:
            response = requests.get(url=url, params=params)
        except Exception:
            util.log.error(traceback.format_exc())
            exit()
        if response.status_code != 200:
            util.log.error({
                'response_status_code': response.status_code,
                'response_json': response.json()
            })
            response.raise_for_status()
        util.log.info("success! response received")
        return response.json()

    def getForecasts(self, lat:float, lon:float, units:str='imperial') -> util.JsonType:
        """
        Method for getting weather forecasts for given latitude and longitude.
        :param lat:     latitude
        :param lon:     longitude
        :param units:   unit of measure
        :docs:          https://openweathermap.org/forecast5
        :return:        json of weather forecasts
        """
        url = self.base_url + self.forecast_url_ext
        params = {'lat': lat, 'lon': lon, 'units': units}
        return self._get(url, params).get('list')

    def getCurrent(self, lat:float, lon:float, units:str='imperial') -> util.JsonType:
        """
        Method for getting current weather for given latitude and longitude.
        :param lat:     latitude
        :param lon:     longitude
        :param units:   unit of measure
        :docs:          https://openweathermap.org/current
        :return:        json of current weather
        """
        url = self.base_url + self.current_url_ext
        params = {'lat': lat, 'lon': lon, 'units': units}
        return self._get(url, params)

