
/*
cardinality = dt, lat, lon
*/
CREATE VIEW current_weather AS
    SELECT
        import_dt,
        -- converting datetime now for joins later
        DATE_TRUNC('HOUR', dt) AS dt,
        lat,
        lon,
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
        type = 'current'
