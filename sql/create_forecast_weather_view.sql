
/*
cardinality = dt, lat, lon, forecast_hours
*/
CREATE VIEW forecast_weather AS
    SELECT
        import_dt,
        DATE_TRUNC('HOUR', import_dt) AS import_dt_trunc,
        dt,
        lat,
        lon,
        CAST((
            EXTRACT(
                EPOCH FROM dt
            ) - EXTRACT(
                EPOCH FROM DATE_TRUNC('HOUR', import_dt)
            )
        ) / 3600 AS INTEGER) AS forecast_hours,
        temp,
        feels_like,
        humidity,
        clouds,
        wind_speed,
        wind_deg,
        description
    FROM
        weather
    WHERE
        type = 'forecast'
