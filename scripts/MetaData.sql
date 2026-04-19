-- ==============================================================================
-- 1. USERS TABLE
-- ==============================================================================
CREATE TABLE users (
    user_id VARCHAR2(36) PRIMARY KEY,
    username VARCHAR2(255) NOT NULL,
    email VARCHAR2(255) UNIQUE NOT NULL,
    is_admin NUMBER(1) DEFAULT 0 CHECK (is_admin IN (0, 1))
);

INSERT INTO users (user_id, username, email, is_admin) 
VALUES ('usr-1001', 'jdoe', 'jane.doe@company.com', 1);

-- ==============================================================================
-- 2. TEAMS TABLE
-- ==============================================================================
CREATE TABLE teams (
    team_id VARCHAR2(36) PRIMARY KEY,
    team_name VARCHAR2(255) UNIQUE NOT NULL
);

INSERT INTO teams (team_id, team_name) 
VALUES ('team-2001', 'Actuarial Sciences');

-- ==============================================================================
-- 3. USER_TEAMS (Junction)
-- ==============================================================================
CREATE TABLE user_teams (
    user_id VARCHAR2(36) REFERENCES users(user_id) ON DELETE CASCADE,
    team_id VARCHAR2(36) REFERENCES teams(team_id) ON DELETE CASCADE,
    user_role VARCHAR2(50) DEFAULT 'MEMBER' CHECK (user_role IN ('MEMBER', 'MANAGER', 'ADMIN')),
    PRIMARY KEY (user_id, team_id)
);

INSERT INTO user_teams (user_id, team_id, user_role) 
VALUES ('usr-1001', 'team-2001', 'MANAGER');

-- ==============================================================================
-- 4. CATALOG_REGISTRY (The Metadata Source of Truth)
-- ==============================================================================
CREATE TABLE catalog_registry (
    catalog_id VARCHAR2(36) PRIMARY KEY,
    name VARCHAR2(255) NOT NULL,
    namespace VARCHAR2(255),
    scope VARCHAR2(50) CHECK (scope IN ('SYSTEM', 'TEAM', 'USER')),
    
    -- Ownership constraints
    owner_user_id VARCHAR2(36) REFERENCES users(user_id) ON DELETE SET NULL,
    owner_team_id VARCHAR2(36) REFERENCES teams(team_id) ON DELETE SET NULL,
    
    -- The AST Payload (Enforces valid JSON in Oracle 12c+)
    catalog_json CLOB CONSTRAINT check_valid_json CHECK (catalog_json IS JSON),
    
    -- Materialization Flags
    is_materialized NUMBER(1) DEFAULT 0 CHECK (is_materialized IN (0, 1)),
    oracle_view_name VARCHAR2(255),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO catalog_registry (
    catalog_id, name, namespace, scope, owner_team_id, catalog_json, is_materialized, oracle_view_name
) VALUES (
    'cat-3001', 
    'Q3 Actuary Risk Summary', 
    'actuary_marts', 
    'TEAM', 
    'team-2001', 
    '{"name": "Q3 Actuary Risk Summary", "entities": [{"name": "risk_data", "locator": {"plugin": "oracle"}}]}', 
    1, 
    'MV_Q3_RISK_SUMMARY'
);

-- ==============================================================================
-- 5. CATALOG_SHARES (Granular ad-hoc sharing)
-- ==============================================================================
CREATE TABLE catalog_shares (
    share_id VARCHAR2(36) PRIMARY KEY,
    catalog_id VARCHAR2(36) REFERENCES catalog_registry(catalog_id) ON DELETE CASCADE,
    
    shared_with_user_id VARCHAR2(36) REFERENCES users(user_id) ON DELETE CASCADE,
    shared_with_team_id VARCHAR2(36) REFERENCES teams(team_id) ON DELETE CASCADE,
    
    permission_level VARCHAR2(50) CHECK (permission_level IN ('VIEWER', 'EDITOR'))
);

INSERT INTO catalog_shares (share_id, catalog_id, shared_with_user_id, permission_level) 
VALUES ('shr-4001', 'cat-3001', 'usr-1001', 'EDITOR');

-- Commit the transactions
COMMIT;