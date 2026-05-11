CREATE TABLE PERF.MQ_OUTBOUND (
    /* Shared global sequence */
    MQ_ID NUMBER DEFAULT global_mq_id_sequence.NEXTVAL PRIMARY KEY,
    CSM_MBR_DEMOGR_ID VARCHAR2(32 CHAR) NOT NULL,
    PID VARCHAR2(16 CHAR) NOT NULL,
    FIRST_NAME VARCHAR2(32 CHAR),
    MIDDLE_NAME VARCHAR2(32 CHAR),
    LAST_NAME VARCHAR2(32 CHAR),
    NAME_PREFIX_ID NUMBER,
    NAME_PREFIX VARCHAR2(32 CHAR),
    NAME_SUFFIX_ID NUMBER,
    NAME_SUFFIX VARCHAR2(32 CHAR),
    BIRTHDATE DATE,
    DT_OF_DEATH DATE,
    EMAIL_ADDR VARCHAR2(96 CHAR),
    GENDER_ID NUMBER,
    GENDER VARCHAR2(32 CHAR),
    MAR_STATUS_ID NUMBER,
    MAR_STATUS VARCHAR2(32 CHAR),
    SSN VARCHAR2(16 CHAR) NOT NULL,
    DEMOGR_CREATED_BY VARCHAR2(32 CHAR),
    DEMOGR_CREATE_DATE TIMESTAMP,
    DEMOGR_UPDATED_BY VARCHAR2(32 CHAR),
    DEMOGR_UPDATED_DATE TIMESTAMP,   

    /* PHONE */
    CSM_MBR_PHN_ID NUMBER,
    PHONE VARCHAR2(32 CHAR),
    PHONE_TYPE_ID NUMBER,
    PHONE_TYPE VARCHAR2(32 CHAR),
    PHONE_EXT VARCHAR2(8 CHAR),
    PHONE_CREATED_BY VARCHAR2(32 CHAR),
    PHONE_CREATE_DATE TIMESTAMP,
    PHONE_UPDATED_BY VARCHAR2(32 CHAR),
    PHONE_UPDATE_DATE TIMESTAMP,

    /* ADDRESS */
    CSM_MBR_ADDR_ID NUMBER,
    ADDRESS1        VARCHAR2(64 CHAR),
    ADDRESS2        VARCHAR2(64 CHAR),
    ADDRESS3        VARCHAR2(64 CHAR),
    ADDRESS4        VARCHAR2(64 CHAR),
    CITY            VARCHAR2(64 CHAR),
    COUNTY          VARCHAR2(64 CHAR),
    STATE           VARCHAR2(64 CHAR),
    POSTAL          VARCHAR2(16 CHAR),
    COUNTRY         VARCHAR2(16 CHAR),
    ADDRESS_CREATED_BY VARCHAR2(32 CHAR),
    ADDRESS_CREATE_DATE TIMESTAMP,
    ADDRESS_UPDATED_BY VARCHAR2(32 CHAR),
    ADDRESS_UPDATED_DATE TIMESTAMP,
    
    /* AUDIT FIELDS */
    MQ_STATUS       NUMBER DEFAULT NULL,
    updated_at      TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
    updated_by      VARCHAR2(32 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL,
    created_at      TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
    created_by      VARCHAR2(32 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL,

    -- FIX 1: Explicitly define data types for Virtual Columns
    -- FIX 2: Wrapped numeric IDs in TO_CHAR to match concatenated string types
    MQ_OB_DEMOGR_HASH RAW(16) GENERATED ALWAYS AS 
        (STANDARD_HASH(
            NVL(first_name,     '/|\')||
            NVL(middle_name,    '/|\')||
            NVL(last_name,      '/|\')||
            NVL(TO_CHAR(name_prefix_id), '/|\')||
            NVL(name_prefix,    '/|\')||
            NVL(TO_CHAR(name_suffix_id), '/|\')||
            NVL(name_suffix,    '/|\')||
            NVL(to_char(birthdate, 'YYYY-MM-DD'), '/|\')||
            NVL(to_char(dt_of_death, 'YYYY-MM-DD'), '/|\')||
            NVL(email_addr,     '/|\')||
            NVL(TO_CHAR(gender_id),      '/|\')||
            NVL(gender,         '/|\')||
            NVL(TO_CHAR(mar_status_id),  '/|\')||
            NVL(mar_status,     '/|\'),
            'MD5'
        )) VIRTUAL,

    MQ_OB_PHONE_HASH RAW(16) GENERATED ALWAYS AS
        (STANDARD_HASH(
            NVL(PHONE,         '/|\')||
            NVL(TO_CHAR(PHONE_TYPE_ID), '/|\')||
            NVL(PHONE_TYPE,    '/|\'), 
            'MD5'
        )) VIRTUAL,

    MQ_OB_ADDRESS_HASH RAW(16) GENERATED ALWAYS AS
        (STANDARD_HASH (
            NVL(ADDRESS1, '/|\')||
            NVL(ADDRESS2, '/|\')||
            NVL(ADDRESS3, '/|\')||
            NVL(ADDRESS4, '/|\')||
            NVL(CITY,     '/|\')||
            NVL(COUNTY,   '/|\')||
            NVL(STATE,    '/|\')||
            NVL(POSTAL,   '/|\')||
            NVL(COUNTRY,  '/|\'),
            'MD5'
        )) VIRTUAL,

    -- FIX 3: Moved constraint to separate line to avoid Virtual Column conflict
    CONSTRAINT fk_mqo_em_status_code FOREIGN KEY (MQ_STATUS) REFERENCES perf.http_codes(code)
)
PARTITION BY LIST (MQ_STATUS)(
    PARTITION P_NEW     VALUES (NULL),
    PARTITION P_DEFAULT VALUES (DEFAULT)
) ENABLE ROW MOVEMENT;
