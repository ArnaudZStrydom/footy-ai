-- === Teams ===
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    league VARCHAR(100),
    stadium VARCHAR(100),
    manager VARCHAR(100)
);

-- === Players ===
CREATE TABLE IF NOT EXISTS players (
    player_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    team_id INT REFERENCES teams(team_id),
    position VARCHAR(50),
    nationality VARCHAR(50),
    birth_date DATE,
    foot VARCHAR(10)
);

-- === Matches ===
CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    season VARCHAR(20),
    home_team_id INT REFERENCES teams(team_id),
    away_team_id INT REFERENCES teams(team_id),
    venue VARCHAR(100),
    home_goals INT,
    away_goals INT,
    result CHAR(1), -- H, A, D
    match_url TEXT
);

-- === Team Match Stats ===
CREATE TABLE IF NOT EXISTS team_stats (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id),
    team_id INT REFERENCES teams(team_id),
    shots INT,
    shots_on_target INT,
    corners INT,
    fouls INT,
    offsides INT,
    yellow_cards INT,
    red_cards INT,
    passes INT,
    pass_accuracy FLOAT
);

-- === Player Match Stats ===
CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id),
    player_id INT REFERENCES players(player_id),
    team_id INT REFERENCES teams(team_id),
    minutes_played INT,
    goals INT,
    assists INT,
    shots INT,
    shots_on_target INT,
    xg FLOAT,
    xa FLOAT,
    passes INT,
    pass_accuracy FLOAT,
    tackles INT,
    interceptions INT,
    dribbles INT,
    fouls_committed INT,
    yellow_card BOOLEAN,
    red_card BOOLEAN
);

-- === Injuries ===
CREATE TABLE IF NOT EXISTS injuries (
    id SERIAL PRIMARY KEY,
    player_id INT REFERENCES players(player_id),
    start_date DATE,
    end_date DATE,
    injury_type VARCHAR(100),
    description TEXT,
    source TEXT
);

-- === Model Predictions ===
CREATE TABLE IF NOT EXISTS model_predictions (
    id SERIAL PRIMARY KEY,
    match_id INT REFERENCES matches(match_id),
    prediction_label CHAR(1),
    prediction_score FLOAT,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
