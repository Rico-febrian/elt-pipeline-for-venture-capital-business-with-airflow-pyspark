CREATE TABLE IF NOT EXISTS data_profile_quality (
    profile_id SERIAL PRIMARY KEY,
    person_in_charge TEXT NOT NULL,
    source TEXT NOT NULL,
    schema TEXT NOT NULL,
    table_name TEXT NOT NULL,
    n_rows INT NOT NULL,
    n_cols INT NOT NULL,
    data_profile JSONB NOT NULL,
    data_quality JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);