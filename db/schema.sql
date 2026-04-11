-- DSE V3 — PostgreSQL (Neon) schema
-- Pure DDL; order matters for foreign keys.

CREATE TABLE IF NOT EXISTS stocks_master (
    symbol          VARCHAR(20) PRIMARY KEY,
    is_dsex         BOOLEAN DEFAULT FALSE,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS price_history (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    date            DATE NOT NULL,
    open            NUMERIC(10,2),
    high            NUMERIC(10,2),
    low             NUMERIC(10,2),
    close           NUMERIC(10,2),
    ltp             NUMERIC(10,2),
    ycp             NUMERIC(10,2),
    trade           INTEGER,
    value           NUMERIC(18,4),
    volume          BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date
    ON price_history(symbol, date DESC);

CREATE TABLE IF NOT EXISTS live_ticks (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    date            DATE NOT NULL,
    session_no      SMALLINT NOT NULL CHECK (session_no BETWEEN 1 AND 10),
    fetched_at      TIMESTAMPTZ NOT NULL,
    ltp             NUMERIC(10,2),
    high            NUMERIC(10,2),
    low             NUMERIC(10,2),
    close           NUMERIC(10,2),
    ycp             NUMERIC(10,2),
    change_val      NUMERIC(10,2),
    trade           INTEGER,
    value           NUMERIC(18,4),
    volume          BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date, session_no)
);

CREATE INDEX IF NOT EXISTS idx_live_ticks_symbol_date
    ON live_ticks(symbol, date DESC);

CREATE TABLE IF NOT EXISTS live_ticks_archive (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL,
    date            DATE NOT NULL,
    session_no      SMALLINT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    ltp             NUMERIC(10,2),
    high            NUMERIC(10,2),
    low             NUMERIC(10,2),
    close           NUMERIC(10,2),
    ycp             NUMERIC(10,2),
    change_val      NUMERIC(10,2),
    trade           INTEGER,
    value           NUMERIC(18,4),
    volume          BIGINT,
    archived_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_ticks_archive_symbol_date
    ON live_ticks_archive(symbol, date DESC);

CREATE TABLE IF NOT EXISTS market_summary (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE NOT NULL UNIQUE,
    total_trade     INTEGER,
    total_volume    BIGINT,
    total_value_mn  NUMERIC(18,4),
    total_mcap_mn   NUMERIC(18,4),
    dsex_index      NUMERIC(10,2),
    dses_index      NUMERIC(10,2),
    ds30_index      NUMERIC(10,2),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stock_fundamentals (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    pe_ratio        NUMERIC(10,2),
    eps             NUMERIC(10,2),
    fetched_at      DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_stock_fundamentals_symbol
    ON stock_fundamentals(symbol, fetched_at DESC);

CREATE TABLE IF NOT EXISTS signals (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    signal_type     VARCHAR(20) NOT NULL CHECK (signal_type IN ('BUY', 'WATCH', 'EXIT', 'HOLD')),
    signal_date     DATE NOT NULL,
    price_at_signal NUMERIC(10,2),
    reason          TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_date
    ON signals(symbol, signal_date DESC);

CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    quantity        INTEGER NOT NULL,
    avg_buy_price   NUMERIC(10,2) NOT NULL,
    bought_at       DATE NOT NULL,
    is_open         BOOLEAN DEFAULT TRUE,
    closed_at       DATE,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pulse_log (
    id              BIGSERIAL PRIMARY KEY,
    pulse_date      DATE NOT NULL,
    session_no      SMALLINT,
    deepseek_input  JSONB,
    deepseek_output TEXT,
    telegram_sent   BOOLEAN DEFAULT FALSE,
    sent_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analysis_results (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    analysis_date       DATE NOT NULL,
    session_no          SMALLINT,
    support_levels      JSONB,
    resistance_levels   JSONB,
    breakout_signal     VARCHAR(20),
    fib_levels          JSONB,
    rsi                 NUMERIC(6,2),
    macd                JSONB,
    volume_signal       VARCHAR(20),
    stock_class         VARCHAR(20) CHECK (stock_class IN ('INVESTMENT', 'TRADING', 'GAMBLING')),
    overall_signal      VARCHAR(20) CHECK (overall_signal IN ('BUY', 'WATCH', 'HOLD', 'EXIT')),
    confidence_score    NUMERIC(4,2),
    raw_output          JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, analysis_date, session_no)
);

CREATE INDEX IF NOT EXISTS idx_analysis_results_symbol_date
    ON analysis_results(symbol, analysis_date DESC);

CREATE TABLE IF NOT EXISTS traders (
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(100),
    telegram_chat_id VARCHAR(50) UNIQUE,
    is_active       BOOLEAN DEFAULT TRUE,
    timezone        VARCHAR(50) DEFAULT 'Asia/Dhaka',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trader_watchlist (
    id              BIGSERIAL PRIMARY KEY,
    trader_id       BIGINT NOT NULL REFERENCES traders(id),
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    added_at        DATE NOT NULL DEFAULT CURRENT_DATE,
    target_price    NUMERIC(10,2),
    notes           TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trader_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_trader_watchlist_trader
    ON trader_watchlist(trader_id);

CREATE TABLE IF NOT EXISTS trader_performance (
    id                  BIGSERIAL PRIMARY KEY,
    trader_id           BIGINT NOT NULL REFERENCES traders(id),
    date                DATE NOT NULL,
    total_invested      NUMERIC(18,2),
    current_value       NUMERIC(18,2),
    total_pnl           NUMERIC(18,2),
    total_pnl_pct       NUMERIC(8,4),
    day_pnl             NUMERIC(18,2),
    day_pnl_pct         NUMERIC(8,4),
    open_positions      INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trader_id, date)
);

CREATE INDEX IF NOT EXISTS idx_trader_performance_trader_date
    ON trader_performance(trader_id, date DESC);


-- One row per signal_type per symbol per trading day (allows BUY + WATCH same date)
CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_symbol_signal_date_signal_type
    ON signals (symbol, signal_date, signal_type);

CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id              BIGSERIAL PRIMARY KEY,
    trader_id       BIGINT NOT NULL REFERENCES traders(id),
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    quantity        INTEGER NOT NULL DEFAULT 0,
    avg_buy_price   NUMERIC(10,2) NOT NULL,
    total_invested  NUMERIC(18,2) NOT NULL,
    first_bought_at DATE NOT NULL,
    last_updated    DATE NOT NULL,
    is_open         BOOLEAN DEFAULT TRUE,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trader_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_holdings_trader
    ON portfolio_holdings(trader_id);

CREATE TABLE IF NOT EXISTS trade_transactions (
    id              BIGSERIAL PRIMARY KEY,
    trader_id       BIGINT NOT NULL REFERENCES traders(id),
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    transaction_type VARCHAR(4) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
    quantity        INTEGER NOT NULL,
    price           NUMERIC(10,2) NOT NULL,
    total_value     NUMERIC(18,2) NOT NULL,
    transaction_date DATE NOT NULL,
    brokerage_fee   NUMERIC(10,2) DEFAULT 0,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_transactions_trader_symbol
    ON trade_transactions(trader_id, symbol, transaction_date DESC);

-- Optional: tie pulse runs to a trader (used by pulse/deepseek.py when column exists)
ALTER TABLE pulse_log ADD COLUMN IF NOT EXISTS trader_id BIGINT REFERENCES traders(id);

-- Remove corrupt future-dated market_summary rows
DELETE FROM market_summary WHERE trade_date > CURRENT_DATE;

CREATE TABLE IF NOT EXISTS signal_outcomes (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES stocks_master(symbol),
    signal_type     VARCHAR(20) NOT NULL,
    signal_date     DATE NOT NULL,
    price_at_signal NUMERIC(10,2),
    eval_date       DATE,
    price_at_eval   NUMERIC(10,2),
    pnl_pct         NUMERIC(8,4),
    outcome         VARCHAR(10) CHECK (outcome IN ('WIN', 'LOSS', 'NEUTRAL', 'PENDING')),
    days_held       INTEGER,
    signal_reason   TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, signal_date, signal_type)
);

CREATE INDEX IF NOT EXISTS idx_signal_outcomes_date
    ON signal_outcomes(signal_date DESC);

CREATE TABLE IF NOT EXISTS accuracy_scores (
    id              BIGSERIAL PRIMARY KEY,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    signal_type     VARCHAR(20),
    total_signals   INTEGER,
    wins            INTEGER,
    losses          INTEGER,
    neutrals        INTEGER,
    win_rate        NUMERIC(5,2),
    avg_pnl_pct     NUMERIC(8,4),
    best_signal     VARCHAR(20),
    worst_signal    VARCHAR(20),
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(period_start, period_end, signal_type)
);

-- Trader onboarding fields (traders table has no updated_at)
ALTER TABLE traders ADD COLUMN IF NOT EXISTS onboarding_complete BOOLEAN DEFAULT FALSE;
ALTER TABLE traders ADD COLUMN IF NOT EXISTS trading_style VARCHAR(50);
ALTER TABLE traders ADD COLUMN IF NOT EXISTS risk_tolerance VARCHAR(20);
ALTER TABLE traders ADD COLUMN IF NOT EXISTS strategy_notes TEXT;
ALTER TABLE traders ADD COLUMN IF NOT EXISTS holding_period VARCHAR(50);

CREATE TABLE IF NOT EXISTS trader_preferences (
    trader_id        BIGINT PRIMARY KEY REFERENCES traders(id),
    trading_style    VARCHAR(50),
    holding_period   VARCHAR(50),
    risk_tolerance   VARCHAR(20),
    preferred_signals TEXT,
    avoid_signals    TEXT,
    notes            TEXT,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trader_stock_intents (
    id               BIGSERIAL PRIMARY KEY,
    trader_id        BIGINT NOT NULL REFERENCES traders(id),
    symbol           VARCHAR(20),
    avg_buy_price    NUMERIC(10,2),
    intent           VARCHAR(20) CHECK (intent IN ('HOLD','EXIT','WATCH','AVERAGE')),
    target_price     NUMERIC(10,2),
    stop_price       NUMERIC(10,2),
    timeframe        VARCHAR(50),
    notes            TEXT,
    is_active        BOOLEAN DEFAULT TRUE,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trader_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_stock_intents_trader
    ON trader_stock_intents(trader_id);
