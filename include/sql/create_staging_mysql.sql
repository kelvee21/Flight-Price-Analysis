-- Staging table in MySQL: mirrors the raw CSV structure.
CREATE TABLE IF NOT EXISTS flight_staging.stg_flight_prices (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    airline           VARCHAR(100),
    source            VARCHAR(10),
    source_name       VARCHAR(150),
    destination       VARCHAR(10),
    destination_name  VARCHAR(150),
    departure_datetime VARCHAR(50),
    arrival_datetime   VARCHAR(50),
    duration_hrs      DOUBLE,
    stopovers         VARCHAR(20),
    aircraft_type     VARCHAR(50),
    class             VARCHAR(20),
    booking_source    VARCHAR(50),
    base_fare_bdt     DOUBLE,
    tax_surcharge_bdt DOUBLE,
    total_fare_bdt    DOUBLE,
    seasonality       VARCHAR(20),
    days_before_departure INT,
    loaded_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_airline (airline),
    INDEX idx_route (source, destination)
) ENGINE=InnoDB;