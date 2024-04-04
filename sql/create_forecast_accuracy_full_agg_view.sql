
/*
cardinality = forecast_hours
*/
CREATE VIEW forecast_accuracy_full_agg AS
    SELECT
        forecast_hours,
        -- the use of ABS here is a temporary fix for the normalization issue upstream
        CAST(ABS(AVG(temp_acc)) AS NUMERIC(8, 4)) AS temp_acc_avg,
        CAST(ABS(AVG(feels_like_acc)) AS NUMERIC(8, 4)) AS feels_like_acc_avg,
        CAST(ABS(AVG(humidity_acc)) AS NUMERIC(8, 4)) AS humidity_acc_avg,
        CAST(ABS(AVG(clouds_acc)) AS NUMERIC(8, 4)) AS clouds_acc_avg,
        CAST(ABS(AVG(wind_speed_acc)) AS NUMERIC(8, 4)) AS wind_speed_acc_avg,
        CAST(ABS(AVG(wind_deg_acc)) AS NUMERIC(8, 4)) AS wind_deg_acc_avg,
        CAST(ABS(AVG(description_acc)) AS NUMERIC(8, 4)) AS description_acc_avg
    FROM
        forecast_accuracy
    GROUP BY
        forecast_hours
    ORDER BY
        forecast_hours
