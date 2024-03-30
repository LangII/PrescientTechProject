
CREATE TABLE weather (
    type        VARCHAR(255),
    import_dt   TIMESTAMP,
    dt          TIMESTAMP,
    lat         NUMERIC(8, 4),
    lon         NUMERIC(8, 4),
    temp        NUMERIC(8, 4),
    feels_like  NUMERIC(8, 4),
    humidity    NUMERIC(8, 4),
    clouds      NUMERIC(8, 4),
    wind_speed  NUMERIC(8, 4),
    wind_deg    NUMERIC(8, 4),
    description VARCHAR(255),
    PRIMARY KEY (type, import_dt, dt, lat, lon)
)
