-- Analytics warehouse, kept in its own schema apart from Airflow's metadata tables.
-- Grain: one fact row per valid source price observation. Each row has exactly one
-- airline, route and season, so no bridge tables are needed.
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_airline (
    airline_key BIGINT PRIMARY KEY,
    airline     VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS analytics.dim_route (
    route_key        BIGINT PRIMARY KEY,
    source           CHAR(3) NOT NULL,
    source_name      VARCHAR(150),
    destination      CHAR(3) NOT NULL,
    destination_name VARCHAR(150),
    UNIQUE (source, destination)
);

CREATE TABLE IF NOT EXISTS analytics.dim_season (
    season_key  BIGINT PRIMARY KEY,
    seasonality VARCHAR(30) NOT NULL UNIQUE,
    is_peak     BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_flight_prices (
    flight_price_key      BIGSERIAL PRIMARY KEY,
    airline_key           BIGINT NOT NULL REFERENCES analytics.dim_airline (airline_key),
    route_key             BIGINT NOT NULL REFERENCES analytics.dim_route (route_key),
    season_key            BIGINT NOT NULL REFERENCES analytics.dim_season (season_key),
    base_fare_bdt         NUMERIC(12, 2) NOT NULL CHECK (base_fare_bdt >= 0),
    tax_surcharge_bdt     NUMERIC(12, 2) NOT NULL CHECK (tax_surcharge_bdt >= 0),
    total_fare_bdt        NUMERIC(12, 2) NOT NULL CHECK (total_fare_bdt = base_fare_bdt + tax_surcharge_bdt),
    departure_datetime    TIMESTAMP,
    arrival_datetime      TIMESTAMP,
    duration_hrs          DOUBLE PRECISION,
    stopovers             VARCHAR(20),
    aircraft_type         VARCHAR(50),
    class                 VARCHAR(20),
    booking_source        VARCHAR(50),
    days_before_departure INTEGER
);

CREATE INDEX IF NOT EXISTS idx_fact_airline ON analytics.fact_flight_prices (airline_key);
CREATE INDEX IF NOT EXISTS idx_fact_route ON analytics.fact_flight_prices (route_key);
CREATE INDEX IF NOT EXISTS idx_fact_season ON analytics.fact_flight_prices (season_key);

-- One row per successful load, for auditing.
CREATE TABLE IF NOT EXISTS analytics.etl_run_log (
    run_log_key               BIGSERIAL PRIMARY KEY,
    run_id                    VARCHAR(250) NOT NULL,
    input_rows                INTEGER NOT NULL,
    loaded_rows               INTEGER NOT NULL,
    rejected_rows             INTEGER NOT NULL,
    total_fare_recomputed_rows INTEGER NOT NULL,
    loaded_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- KPI views. "booking_count" counts price observations: the source has no booking ID.
CREATE OR REPLACE VIEW analytics.vw_avg_fare_by_airline AS
SELECT a.airline,
       ROUND(AVG(f.total_fare_bdt), 2) AS avg_total_fare,
       COUNT(*) AS booking_count
FROM analytics.fact_flight_prices AS f
JOIN analytics.dim_airline AS a USING (airline_key)
GROUP BY a.airline;

CREATE OR REPLACE VIEW analytics.vw_seasonal_fare_variation AS
SELECT s.seasonality,
       s.is_peak,
       ROUND(AVG(f.total_fare_bdt), 2) AS avg_total_fare,
       COUNT(*) AS booking_count
FROM analytics.fact_flight_prices AS f
JOIN analytics.dim_season AS s USING (season_key)
GROUP BY s.seasonality, s.is_peak;

CREATE OR REPLACE VIEW analytics.vw_peak_vs_non_peak_fare AS
SELECT CASE WHEN s.is_peak THEN 'Peak' ELSE 'Non-Peak' END AS season_period,
       ROUND(AVG(f.total_fare_bdt), 2) AS avg_total_fare,
       COUNT(*) AS booking_count
FROM analytics.fact_flight_prices AS f
JOIN analytics.dim_season AS s USING (season_key)
GROUP BY s.is_peak;

CREATE OR REPLACE VIEW analytics.vw_booking_count_by_airline AS
SELECT a.airline, COUNT(*) AS booking_count
FROM analytics.fact_flight_prices AS f
JOIN analytics.dim_airline AS a USING (airline_key)
GROUP BY a.airline;

CREATE OR REPLACE VIEW analytics.vw_most_popular_routes AS
SELECT r.source,
       r.source_name,
       r.destination,
       r.destination_name,
       COUNT(*) AS booking_count,
       ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, r.source, r.destination) AS route_rank
FROM analytics.fact_flight_prices AS f
JOIN analytics.dim_route AS r USING (route_key)
GROUP BY r.source, r.source_name, r.destination, r.destination_name;
