-- ============================================================
-- HDX Ukraine Pipeline — Database Schema
-- Run once to initialize. Pipeline will upsert into these tables.
-- ============================================================

-- Drop and recreate (safe for dev; remove DROP lines for prod)
IF OBJECT_ID('dbo.food_prices',   'U') IS NOT NULL DROP TABLE dbo.food_prices;
IF OBJECT_ID('dbo.five_w',        'U') IS NOT NULL DROP TABLE dbo.five_w;
IF OBJECT_ID('dbo.fts_funding',   'U') IS NOT NULL DROP TABLE dbo.fts_funding;
IF OBJECT_ID('dbo.hunger_map',    'U') IS NOT NULL DROP TABLE dbo.hunger_map;
IF OBJECT_ID('dbo.pipeline_runs', 'U') IS NOT NULL DROP TABLE dbo.pipeline_runs;


-- ── WFP Food Prices ─────────────────────────────────────────
CREATE TABLE dbo.food_prices (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    report_date     DATE            NOT NULL,
    admin1          NVARCHAR(100),
    admin2          NVARCHAR(100),
    market          NVARCHAR(200),
    market_id       INT,
    latitude        FLOAT,
    longitude       FLOAT,
    category        NVARCHAR(100),
    commodity       NVARCHAR(200),
    commodity_id    INT,
    unit            NVARCHAR(50),
    price_flag      NVARCHAR(50),
    price_type      NVARCHAR(50),
    currency        NVARCHAR(10),
    price           FLOAT,
    usd_price       FLOAT,
    loaded_at       DATETIME2       DEFAULT GETDATE()
);

CREATE INDEX ix_food_prices_date     ON dbo.food_prices (report_date);
CREATE INDEX ix_food_prices_market   ON dbo.food_prices (market_id);
CREATE INDEX ix_food_prices_geo      ON dbo.food_prices (latitude, longitude);


-- ── 5W People Reached ───────────────────────────────────────
CREATE TABLE dbo.five_w (
    id                          INT IDENTITY(1,1) PRIMARY KEY,
    oblast                      NVARCHAR(100)   NOT NULL,
    pcode                       NVARCHAR(10),
    camp_management             FLOAT,
    education                   FLOAT,
    food_security_livelihoods   FLOAT,
    health                      FLOAT,
    protection_child            FLOAT,
    protection_gbv              FLOAT,
    protection_mine_action      FLOAT,
    protection_general          FLOAT,
    shelter_nfi                 FLOAT,
    wash                        FLOAT,
    cash_assistance             FLOAT,
    inter_cluster               FLOAT,
    report_year                 INT,
    loaded_at                   DATETIME2       DEFAULT GETDATE()
);

CREATE INDEX ix_five_w_pcode  ON dbo.five_w (pcode);
CREATE INDEX ix_five_w_year   ON dbo.five_w (report_year);


-- ── FTS Cluster Funding ──────────────────────────────────────
CREATE TABLE dbo.fts_funding (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    country_code    NVARCHAR(5),
    plan_id         INT,
    plan_name       NVARCHAR(500),
    plan_code       NVARCHAR(50),
    start_date      DATE,
    end_date        DATE,
    report_year     INT,
    cluster_code    NVARCHAR(50),
    cluster         NVARCHAR(300),
    requirements    FLOAT,
    funding         FLOAT,
    pct_funded      FLOAT,
    loaded_at       DATETIME2       DEFAULT GETDATE()
);

CREATE INDEX ix_fts_year     ON dbo.fts_funding (report_year);
CREATE INDEX ix_fts_cluster  ON dbo.fts_funding (cluster_code);


-- ── WFP Hunger Map ───────────────────────────────────────────
CREATE TABLE dbo.hunger_map (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    country_code    NVARCHAR(5),
    country_name    NVARCHAR(100),
    admin_one       NVARCHAR(100),
    admin_level     NVARCHAR(50),
    report_date     DATE,
    data_type       NVARCHAR(50),
    indicator       NVARCHAR(100),
    population      BIGINT,
    prevalence      FLOAT,
    loaded_at       DATETIME2       DEFAULT GETDATE()
);

CREATE INDEX ix_hunger_date       ON dbo.hunger_map (report_date);
CREATE INDEX ix_hunger_indicator  ON dbo.hunger_map (indicator);


-- ── Pipeline Run Log ─────────────────────────────────────────
CREATE TABLE dbo.pipeline_runs (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    run_at          DATETIME2       DEFAULT GETDATE(),
    source          NVARCHAR(100),
    target_table    NVARCHAR(100),
    rows_loaded     INT,
    status          NVARCHAR(20),   -- SUCCESS | FAILED
    error_msg       NVARCHAR(MAX)
);
