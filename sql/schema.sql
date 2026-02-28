IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart')
    EXEC('CREATE SCHEMA mart');
GO

IF OBJECT_ID('raw.comtrade_trade', 'U') IS NULL
BEGIN
    CREATE TABLE raw.comtrade_trade (
        ingest_id NVARCHAR(64) NOT NULL,
        ingested_at DATETIME2 NOT NULL,
        reporter_code NVARCHAR(16) NOT NULL,
        flow_code NVARCHAR(8) NOT NULL,
        ref_year INT NULL,
        partner_code NVARCHAR(16) NULL,
        partner_text NVARCHAR(255) NULL,
        cmd_code NVARCHAR(16) NULL,
        trade_value_usd FLOAT NULL,
        net_weight_kg FLOAT NULL,
        source_period NVARCHAR(16) NULL,
        source_system NVARCHAR(64) NULL
    );
END;
GO

IF OBJECT_ID('raw.logistics_signals', 'U') IS NULL
BEGIN
    CREATE TABLE raw.logistics_signals (
        ingest_id NVARCHAR(64) NOT NULL,
        ingested_at DATETIME2 NOT NULL,
        reporter_code NVARCHAR(16) NOT NULL,
        supplier_country_code NVARCHAR(16) NOT NULL,
        supplier_country NVARCHAR(255) NULL,
        route_name NVARCHAR(255) NULL,
        lead_time_days FLOAT NULL,
        lead_time_stddev_days FLOAT NULL,
        freight_index FLOAT NULL,
        disruption_index FLOAT NULL,
        risk_score FLOAT NULL,
        source_system NVARCHAR(64) NULL
    );
END;
GO

IF OBJECT_ID('raw.policy_signals', 'U') IS NULL
BEGIN
    CREATE TABLE raw.policy_signals (
        ingest_id NVARCHAR(64) NOT NULL,
        ingested_at DATETIME2 NOT NULL,
        reporter_code NVARCHAR(16) NOT NULL,
        supplier_country_code NVARCHAR(16) NOT NULL,
        supplier_country NVARCHAR(255) NULL,
        hs_code NVARCHAR(16) NULL,
        tariff_pct FLOAT NULL,
        sanctions_flag BIT NULL,
        export_control_flag BIT NULL,
        policy_volatility FLOAT NULL,
        risk_score FLOAT NULL,
        source_system NVARCHAR(64) NULL
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_comtrade_reporter_flow_year'
      AND object_id = OBJECT_ID('raw.comtrade_trade')
)
CREATE INDEX IX_comtrade_reporter_flow_year
ON raw.comtrade_trade (reporter_code, flow_code, ref_year);
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_logistics_reporter_supplier'
      AND object_id = OBJECT_ID('raw.logistics_signals')
)
CREATE INDEX IX_logistics_reporter_supplier
ON raw.logistics_signals (reporter_code, supplier_country_code);
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_policy_reporter_supplier'
      AND object_id = OBJECT_ID('raw.policy_signals')
)
CREATE INDEX IX_policy_reporter_supplier
ON raw.policy_signals (reporter_code, supplier_country_code);
GO
