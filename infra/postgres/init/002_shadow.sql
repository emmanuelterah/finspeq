-- Shadow model verdicts beside the live decision (M6). Idempotent; safe to run again.

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shadow_probability DOUBLE PRECISION
    CHECK (shadow_probability IS NULL OR (shadow_probability >= 0 AND shadow_probability <= 1));
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shadow_decision TEXT
    CHECK (shadow_decision IS NULL OR shadow_decision IN ('APPROVE', 'REVIEW', 'BLOCK'));
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shadow_model_version TEXT;
