
/*
cardinality = dt, lat, lon, forecast_hours
*/
CREATE VIEW forecast_accuracy AS
    SELECT
        f.dt AS dt,
        f.lat AS lat,
        f.lon AS lon,
        f.forecast_hours AS forecast_hours,
        c.temp AS current_temp,
        f.temp AS forecast_temp,
        CAST(
            COALESCE(
                1.0 - (ABS(c.temp - f.temp) / NULLIF(c.temp, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS temp_acc,
        c.feels_like AS current_feels_like,
        f.feels_like AS forecast_feels_like,
        CAST(
            COALESCE(
                1.0 - (ABS(c.feels_like - f.feels_like) / NULLIF(c.feels_like, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS feels_like_acc,
        c.humidity AS current_humidity,
        f.humidity AS forecast_humidity,
        CAST(
            COALESCE(
                1.0 - (ABS(c.humidity - f.humidity) / NULLIF(c.humidity, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS humidity_acc,
        c.clouds AS current_clouds,
        f.clouds AS forecast_clouds,
        CAST(
            COALESCE(
                1.0 - (ABS(c.clouds - f.clouds) / NULLIF(c.clouds, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS clouds_acc,
        c.wind_speed AS current_wind_speed,
        f.wind_speed AS forecast_wind_speed,
        CAST(
            COALESCE(
                1.0 - (ABS(c.wind_speed - f.wind_speed) / NULLIF(c.wind_speed, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS wind_speed_acc,
        c.wind_deg AS current_wind_deg,
        f.wind_deg AS forecast_wind_deg,
        CAST(
            COALESCE(
                1.0 - (ABS(c.wind_deg - f.wind_deg) / NULLIF(c.wind_deg, 0.0)),
                0.0
            )
            AS NUMERIC(8, 4)
        ) AS wind_deg_acc,
        c.description AS current_description,
        f.description AS forecast_description,
        CAST(
            CASE WHEN c.description = f.description THEN 1.0 ELSE 0.0 END
            AS NUMERIC(8, 4)
        ) AS description_acc
    FROM
        forecast_weather AS f
        LEFT JOIN current_weather AS c
            ON (f.dt, f.lat, f.lon) = (c.dt, c.lat, c.lon)
    WHERE
        -- these dt conditions are to get the "goldie locks zone" of data
        -- where there is 100% overlap of all forecast_hours
        f.dt > (SELECT MIN(import_dt) + INTERVAL '3 DAYS' FROM weather)
        AND f.dt < CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
        AND f.forecast_hours <= 72
        AND f.forecast_hours != 0
