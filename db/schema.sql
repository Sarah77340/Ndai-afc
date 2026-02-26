-- 1. Table des ventes (Source: Etape 5/6)
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    sale_date DATE,
    country VARCHAR(100),
    product VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2)
);

-- 2. Table des feedbacks enrichis (Source: Etape 4)
CREATE TABLE IF NOT EXISTS campaign_feedback_enriched (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    feedback_date DATE,
    campaign_id VARCHAR(50),
    comment TEXT,
    sentiment_score FLOAT, -- Pour le ML Engineer
    sentiment_label VARCHAR(20) -- Pour le ML Engineer
);

-- 3. Table de mapping (Source: README)
CREATE TABLE IF NOT EXISTS campaign_product_mapping (
    campaign_id VARCHAR(50) PRIMARY KEY,
    product VARCHAR(255)
);

-- 4. Indexation pour la performance (Source: Etape 3)
CREATE INDEX IF NOT EXISTS idx_feedback_date ON campaign_feedback_enriched(feedback_date);
CREATE INDEX IF NOT EXISTS idx_campaign_id ON campaign_feedback_enriched(campaign_id);
CREATE INDEX IF NOT EXISTS idx_sale_date ON sales(sale_date);
