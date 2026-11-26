-- Decision ledger and consumer positions. See ARCHITECTURE.md section 4.

CREATE TABLE IF NOT EXISTS decisions (
    tx_id             CHAR(32)         PRIMARY KEY,
    card              VARCHAR(19)      NOT NULL,
    event_time        BIGINT           NOT NULL CHECK (event_time >= 0),
    amount_cents      BIGINT           NOT NULL CHECK (amount_cents > 0),
    fraud_probability DOUBLE PRECISION NOT NULL
        CHECK (fraud_probability >= 0 AND fraud_probability <= 1),
    decision          TEXT             NOT NULL
        CHECK (decision IN ('APPROVE', 'REVIEW', 'BLOCK')),
    model_version     TEXT             NOT NULL,
    feature_schema    TEXT             NOT NULL,
    features          JSONB            NOT NULL,
    topic_partition   INTEGER          NOT NULL CHECK (topic_partition >= 0),
    topic_offset      BIGINT           NOT NULL CHECK (topic_offset >= 0),
    recorded_at       TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS decisions_card_time ON decisions (card, event_time);

CREATE TABLE IF NOT EXISTS consumer_positions (
    consumer        TEXT    NOT NULL,
    topic           TEXT    NOT NULL,
    topic_partition INTEGER NOT NULL CHECK (topic_partition >= 0),
    last_offset     BIGINT  NOT NULL CHECK (last_offset >= 0),
    PRIMARY KEY (consumer, topic, topic_partition)
);
