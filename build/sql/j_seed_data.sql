

-- =====================================================
-- 1. USERS TABLE
-- =====================================================
INSERT INTO users (user_id, username, email, password_hash, is_admin) 
VALUES ('jdoe', 'jane.doe@company.com','hashed_password');
insert into users (username, email, hashed_password) values ('testuser', 'testuser@example.com', 'hashedpassword');
insert into users (username, email, hashed_password) values ('client111', 'brudow@example.com', 'clientpassword111');
insert into users (username, email, hashed_password) values ('admin', 'admin@example.com', 'administratum');

-- =====================================================
-- 2. QBL.USER_SETTINGS
-- =====================================================
INSERT INTO user_settings (user_id, setting_key, setting_value) 
VALUES ('jdoe', 'theme', 'dark');

-- =====================================================
-- 2. TEAMS TABLE
-- =====================================================
INSERT INTO teams (team_id, team_name) 
VALUES ('actuarial', 'Actuarial Sciences');
INSERT INTO teams (team_id, team_name) 
VALUES ('finance', 'Finance Department');
INSERT INTO teams (team_id, team_name) 
VALUES ('operations', 'Operations Department');
INSERT INTO teams (team_id, team_name) 
VALUES ('admin', 'Administrator');

-- =====================================================
-- 3. USER_TEAMS (Junction)
-- =====================================================
DECLARE
    v_user_id VARCHAR2(36);
    v_username VARCHAR2(50);
    CURSOR c_user_ids IS 
    SELECT user_id, username FROM users WHERE username IN ('jdoe', 'testuser', 'client111', 'admin');
    v_id   mq_table.message_id%TYPE;
BEGIN
    OPEN c_user_ids;
    LOOP
        FETCH c_user_ids INTO v_user_id, v_username;
        EXIT WHEN c_user_ids%NOTFOUND;
        
        IF v_username = 'testuser' THEN
            INSERT INTO user_teams (user_id, team_id, user_role) 
            VALUES (v_user_id, 'actuarial', 'USER');
        END IF;
        
        IF v_username = 'client111' THEN
            INSERT INTO user_teams (user_id, team_id, user_role) 
            VALUES (v_user_id, 'finance', 'USER');
        END IF;
        
        IF v_username = 'jdoe' THEN
            INSERT INTO user_teams (user_id, team_id, user_role) 
            VALUES (v_user_id, 'operations', 'USER');
        END IF;
        
        IF v_username = 'admin' THEN
            INSERT INTO user_teams (user_id, team_id, user_role) 
            VALUES (v_user_id, 'admin', 'ADMIN');
        END IF;
    END LOOP;
    CLOSE c_user_ids;
END;
/

/*--====================================================
4. CATALOG_REGISTRY
*/--====================================================
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

/*--====================================================
5. CATALOG_SHARES (Granular ad-hoc sharing)   
*/--====================================================
INSERT INTO catalog_shares (share_id, catalog_id, shared_with_user_id, permission_level) 
VALUES ('shr-4001', 'cat-3001', 'usr-1001', 'EDITOR');

-- Commit the transactions
-- COMMIT;