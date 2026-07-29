-- Test-only mirror of the schema owned by nem-ingestion — keep in sync.
--
-- nem-ingestion applies this DDL for real (it's the sole writer against the
-- shared NEM Postgres). This copy exists ONLY so the pytest fixtures in this
-- repo have tables to query against; it is never applied to a real deployment.
-- Extracted verbatim from the source NEMDatabase.initialize() (pre-split).

CREATE TABLE IF NOT EXISTS dispatch_data (
    id BIGSERIAL PRIMARY KEY,
    settlementdate TIMESTAMP NOT NULL,
    duid TEXT NOT NULL,
    scadavalue REAL,
    uigf REAL,
    totalcleared REAL,
    ramprate REAL,
    availability REAL,
    raise1sec REAL,
    lower1sec REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlementdate, duid)
);

CREATE TABLE IF NOT EXISTS generator_info (
    duid TEXT PRIMARY KEY,
    station_name TEXT,
    region TEXT,
    fuel_source TEXT,
    technology_type TEXT,
    capacity_mw REAL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_data (
    id BIGSERIAL PRIMARY KEY,
    settlementdate TIMESTAMP NOT NULL,
    region TEXT NOT NULL,
    price REAL,
    totaldemand REAL,
    price_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlementdate, region, price_type)
);

CREATE TABLE IF NOT EXISTS pdpasa_data (
    id BIGSERIAL PRIMARY KEY,
    run_datetime TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    regionid TEXT NOT NULL,
    demand10 REAL,
    demand50 REAL,
    demand90 REAL,
    reservereq REAL,
    capacityreq REAL,
    aggregatecapacityavailable REAL,
    aggregatepasaavailability REAL,
    surplusreserve REAL,
    lorcondition INTEGER,
    calculatedlor1level REAL,
    calculatedlor2level REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_datetime, interval_datetime, regionid)
);

CREATE TABLE IF NOT EXISTS stpasa_data (
    id BIGSERIAL PRIMARY KEY,
    run_datetime TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    regionid TEXT NOT NULL,
    demand10 REAL,
    demand50 REAL,
    demand90 REAL,
    reservereq REAL,
    capacityreq REAL,
    aggregatecapacityavailable REAL,
    aggregatepasaavailability REAL,
    surplusreserve REAL,
    lorcondition INTEGER,
    calculatedlor1level REAL,
    calculatedlor2level REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_datetime, interval_datetime, regionid)
);

-- PREDISPATCH region price forecast (RRP), one row per run x interval x region
CREATE TABLE IF NOT EXISTS predispatch_price (
    id BIGSERIAL PRIMARY KEY,
    run_datetime TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    regionid TEXT NOT NULL,
    rrp REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_datetime, interval_datetime, regionid)
);

-- PD interconnector solution (flows vs limits), one row per run x interval x interconnector
CREATE TABLE IF NOT EXISTS predispatch_interconnector (
    id BIGSERIAL PRIMARY KEY,
    run_datetime TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    interconnectorid TEXT NOT NULL,
    mwflow REAL,
    exportlimit REAL,
    importlimit REAL,
    marginalvalue REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_datetime, interval_datetime, interconnectorid)
);

-- PD constraint solution, binding-or-violated rows only (filtered on write)
CREATE TABLE IF NOT EXISTS predispatch_constraint (
    id BIGSERIAL PRIMARY KEY,
    run_datetime TIMESTAMP NOT NULL,
    interval_datetime TIMESTAMP NOT NULL,
    constraintid TEXT NOT NULL,
    rhs REAL,
    marginalvalue REAL,
    violationdegree REAL,
    lhs REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_datetime, interval_datetime, constraintid)
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    region TEXT NOT NULL,
    capture_solar REAL,
    capture_wind REAL,
    capture_battery REAL,
    capture_gas REAL,
    capture_coal REAL,
    capture_hydro REAL,
    capture_price_solar REAL,
    capture_price_wind REAL,
    capture_price_battery REAL,
    capture_price_gas REAL,
    capture_price_coal REAL,
    capture_price_hydro REAL,
    baseload_price REAL,
    tb2_spread REAL,
    tb4_spread REAL,
    tb8_spread REAL,
    intervals_count INTEGER,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ps_freq_solar REAL, ps_freq_wind REAL, ps_freq_battery REAL,
    ps_freq_gas REAL, ps_freq_coal REAL, ps_freq_hydro REAL,
    ps_price_solar REAL, ps_price_wind REAL, ps_price_battery REAL,
    ps_price_gas REAL, ps_price_coal REAL, ps_price_hydro REAL,
    ps_count_solar REAL, ps_count_wind REAL, ps_count_battery REAL,
    ps_count_gas REAL, ps_count_coal REAL, ps_count_hydro REAL,
    UNIQUE(metric_date, region)
);

-- Price setter data from NEMDE archive
CREATE TABLE IF NOT EXISTS price_setter_data (
    id BIGSERIAL PRIMARY KEY,
    period_id TIMESTAMP NOT NULL,
    region TEXT NOT NULL,
    price REAL NOT NULL,
    duid TEXT NOT NULL,
    increase REAL,
    band_price REAL,
    band_no INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(period_id, region, duid)
);

-- Bid data tables
CREATE TABLE IF NOT EXISTS bid_day_offer (
    id BIGSERIAL PRIMARY KEY,
    settlementdate DATE NOT NULL,
    duid TEXT NOT NULL,
    offerdate TIMESTAMP NOT NULL,
    priceband1 REAL, priceband2 REAL, priceband3 REAL, priceband4 REAL, priceband5 REAL,
    priceband6 REAL, priceband7 REAL, priceband8 REAL, priceband9 REAL, priceband10 REAL,
    minimumload REAL,
    t1 REAL, t2 REAL, t3 REAL, t4 REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlementdate, duid, offerdate)
);

CREATE TABLE IF NOT EXISTS bid_per_offer (
    id BIGSERIAL PRIMARY KEY,
    settlementdate TIMESTAMP NOT NULL,
    duid TEXT NOT NULL,
    offerdate TIMESTAMP NOT NULL,
    bandavail1 REAL, bandavail2 REAL, bandavail3 REAL, bandavail4 REAL, bandavail5 REAL,
    bandavail6 REAL, bandavail7 REAL, bandavail8 REAL, bandavail9 REAL, bandavail10 REAL,
    maxavail REAL,
    fixedload REAL,
    rocup REAL,
    rocdown REAL,
    pasaavailability REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(settlementdate, duid, offerdate)
);

-- Hourly rollup of dispatch_data. Stores sum+count (not avg) so
-- coarser aggregations compose exactly; serves all >=60min display bands.
CREATE TABLE IF NOT EXISTS dispatch_data_hourly (
    hour TIMESTAMP NOT NULL,
    duid TEXT NOT NULL,
    sum_scada DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    PRIMARY KEY (hour, duid)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dispatch_settlement ON dispatch_data(settlementdate);
CREATE INDEX IF NOT EXISTS idx_dispatch_duid ON dispatch_data(duid);
CREATE INDEX IF NOT EXISTS idx_dispatch_settlement_duid ON dispatch_data(settlementdate, duid);
CREATE INDEX IF NOT EXISTS idx_price_settlement ON price_data(settlementdate);
CREATE INDEX IF NOT EXISTS idx_price_region ON price_data(region);
CREATE INDEX IF NOT EXISTS idx_price_settlement_region ON price_data(settlementdate, region);
CREATE INDEX IF NOT EXISTS idx_price_region_settlement ON price_data(region, settlementdate);
CREATE INDEX IF NOT EXISTS idx_price_region_type_settlement ON price_data(region, price_type, settlementdate);

CREATE INDEX IF NOT EXISTS idx_pdpasa_run ON pdpasa_data(run_datetime);
CREATE INDEX IF NOT EXISTS idx_pdpasa_region ON pdpasa_data(regionid);
CREATE INDEX IF NOT EXISTS idx_pdpasa_region_run ON pdpasa_data(regionid, run_datetime);
CREATE INDEX IF NOT EXISTS idx_stpasa_run ON stpasa_data(run_datetime);
CREATE INDEX IF NOT EXISTS idx_stpasa_region ON stpasa_data(regionid);
CREATE INDEX IF NOT EXISTS idx_stpasa_region_run ON stpasa_data(regionid, run_datetime);
CREATE INDEX IF NOT EXISTS idx_predispatch_region ON predispatch_price(regionid);
CREATE INDEX IF NOT EXISTS idx_predispatch_region_run ON predispatch_price(regionid, run_datetime);
CREATE INDEX IF NOT EXISTS idx_predispatch_ic_id ON predispatch_interconnector(interconnectorid);
CREATE INDEX IF NOT EXISTS idx_predispatch_ic_id_run ON predispatch_interconnector(interconnectorid, run_datetime);
CREATE INDEX IF NOT EXISTS idx_predispatch_constraint_id ON predispatch_constraint(constraintid);
CREATE INDEX IF NOT EXISTS idx_predispatch_constraint_id_run ON predispatch_constraint(constraintid, run_datetime);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_region_date ON daily_metrics(region, metric_date);

CREATE INDEX IF NOT EXISTS idx_price_setter_period ON price_setter_data(period_id);
CREATE INDEX IF NOT EXISTS idx_price_setter_region_period ON price_setter_data(region, period_id);
CREATE INDEX IF NOT EXISTS idx_price_setter_increase ON price_setter_data(increase);

CREATE INDEX IF NOT EXISTS idx_biddayoffer_duid_date ON bid_day_offer(duid, settlementdate);
CREATE INDEX IF NOT EXISTS idx_bidperoffer_duid_date ON bid_per_offer(duid, settlementdate);
CREATE INDEX IF NOT EXISTS idx_bidperoffer_settlement ON bid_per_offer(settlementdate);
