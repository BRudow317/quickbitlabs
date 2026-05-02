-- ==============================================================================
-- 1. USERS TABLE
-- ==============================================================================
CREATE TABLE users (
    user_id VARCHAR2(36) PRIMARY KEY,
    username VARCHAR2(255) NOT NULL,
    email VARCHAR2(255) UNIQUE NOT NULL,
    password_hash VARCHAR2(510) NOT NULL,
    is_admin NUMBER(1) DEFAULT 0 CHECK (is_admin IN (0, 1))
);

INSERT INTO users (user_id, username, email, password_hash, is_admin) 
VALUES ('usr-1001', 'jdoe', 'jane.doe@company.com', 'hashed_password', 1);

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
    alias VARCHAR2(255),
    namespace VARCHAR2(255),
    scope VARCHAR2(50) CHECK (scope IN ('SYSTEM', 'TEAM', 'USER')),
    source_type VARCHAR2(50), 
    
    -- Ownership constraints
    owner_user_id VARCHAR2(36) REFERENCES users(user_id) ON DELETE SET NULL,
    team_id VARCHAR2(36) REFERENCES teams(team_id) ON DELETE SET NULL,
    
    -- Scalars
    "LIMIT" NUMBER,
    
    -- Complex Pydantic Lists/Dicts mapped to native JSON CLOBs
    entities CLOB CONSTRAINT check_entities_json CHECK (entities IS JSON),
    filters CLOB CONSTRAINT check_filters_json CHECK (filters IS JSON),
    assignments CLOB CONSTRAINT check_assignments_json CHECK (assignments IS JSON),
    joins CLOB CONSTRAINT check_joins_json CHECK (joins IS JSON),
    sort_columns CLOB CONSTRAINT check_sorts_json CHECK (sort_columns IS JSON),
    properties CLOB CONSTRAINT check_props_json CHECK (properties IS JSON),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO catalog_registry (
    catalog_id, name, alias, namespace, scope, source_type, team_id, 
    "LIMIT", entities, filters, assignments, joins, sort_columns, properties
) VALUES (
    'cat-3001', 
    'Q3 Actuary Risk Summary', 
    'Risk_Summary_V1',
    'actuary_marts', 
    'TEAM', 
    'federation',
    'team-2001',
    5000,
    '[{"name": "risk_data", "columns": [{"name": "id", "locator": {"plugin": "oracle"}}]}]',
    '[]',
    '[]',
    '[]',
    '[]',
    '{}'
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
