CREATE SEQUENCE global_mq_id_sequence START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

CREATE TABLE DWH.MQ_OUTBOUND (
    /* Shared global sequence */
    MQ_ID NUMBER DEFAULT ON NULL global_mq_id_sequence.NEXTVAL PRIMARY KEY,
    DEMOGRAPHIC_ID NUMBER NOT NULL,
    EMPLOYEE_ID VARCHAR2(16 CHAR) NOT NULL,
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
    SSN VARCHAR2(9 CHAR) NOT NULL,
    DELETED_STATUS NUMBER,
    DEMOGR_CREATED_BY VARCHAR2(32 CHAR),
    DEMOGR_CREATED_AT TIMESTAMP,
    DEMOGR_UPDATED_BY VARCHAR2(32 CHAR),
    DEMOGR_UPDATED_AT TIMESTAMP,   

    /* PHONE */
    PHONE_ID NUMBER,
    PHONE VARCHAR2(32 CHAR),
    PHONE_TYPE_ID NUMBER,
    PHONE_TYPE VARCHAR2(32 CHAR),
    PHONE_EXT VARCHAR2(8 CHAR),
    PHONE_CREATED_BY VARCHAR2(32 CHAR),
    PHONE_CREATED_AT TIMESTAMP,
    PHONE_UPDATED_BY VARCHAR2(32 CHAR),
    PHONE_UPDATED_AT TIMESTAMP,

    /* ADDRESS */
    ADDRESS_ID      NUMBER,
    ADDRESS1        VARCHAR2(64 CHAR),
    ADDRESS2        VARCHAR2(64 CHAR),
    ADDRESS3        VARCHAR2(64 CHAR),
    ADDRESS4        VARCHAR2(64 CHAR),
    CITY            VARCHAR2(64 CHAR),
    COUNTY          VARCHAR2(64 CHAR),
    STATE           VARCHAR2(64 CHAR),
    POSTAL          VARCHAR2(16 CHAR),
    COUNTRY         VARCHAR2(16 CHAR),
    ADDRESS_TYPE_ID NUMBER,
    ADDRESS_TYPE    VARCHAR2(32 CHAR),
    ADDRESS_CREATED_BY VARCHAR2(32 CHAR),
    ADDRESS_CREATED_AT TIMESTAMP,
    ADDRESS_UPDATED_BY VARCHAR2(32 CHAR),
    ADDRESS_UPDATED_AT TIMESTAMP,
    
    /* AUDIT FIELDS */
    MQ_STATUS       NUMBER DEFAULT NULL CONSTRAINT FK_MQ_OUTBOUND_STATUS REFERENCES qbl.http_codes(code),
    UPDATED_AT      TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
    UPDATED_BY      VARCHAR2(32 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL,
    CREATED_AT      TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
    CREATED_BY      VARCHAR2(32 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL

)
PARTITION BY LIST (MQ_STATUS)(
    PARTITION P_NEW     VALUES (NULL),
    PARTITION P_DEFAULT VALUES (DEFAULT)
) ENABLE ROW MOVEMENT;

-- =====================================================
-- DWH.MQ_INBOUND (Staging Table)
-- Raw inbound data from external sources 
-- hash added during post processing.
-- =====================================================
CREATE TABLE dwh.mq_inbound (
  mq_id             NUMBER DEFAULT ON NULL global_mq_id_sequence.NEXTVAL PRIMARY KEY,
  demographic_id    NUMBER,
  employee_id       VARCHAR2(16 CHAR) NOT NULL,
  first_name        VARCHAR2(64 CHAR),
  middle_name       VARCHAR2(64 CHAR),
  last_name         VARCHAR2(64 CHAR),
  name_prefix_id    NUMBER,
  name_prefix       VARCHAR2(16 CHAR),
  name_suffix_id    NUMBER,
  name_suffix       VARCHAR2(16 CHAR),
  birthdate         DATE,
  dt_of_death       DATE,
  email_addr        VARCHAR2(120 CHAR),
  email_type        VARCHAR2(32 CHAR),
  gender_id         NUMBER,
  gender            VARCHAR2(32 CHAR),
  mar_status_id     NUMBER,
  mar_status        VARCHAR2(32 CHAR),
  ssn               VARCHAR2(9 CHAR),
  mq_demographic_hash VARCHAR2(32 CHAR),
  phone_id          NUMBER,
  phone             VARCHAR2(32 CHAR),
  phone_type_id     NUMBER,
  phone_type        VARCHAR2(32 CHAR),
  mq_phone_hash     VARCHAR2(32 CHAR),
  /* Address Fields */
  address_id        NUMBER,
  address1          VARCHAR2(64 CHAR),
  address2          VARCHAR2(64 CHAR),
  address3          VARCHAR2(64 CHAR),
  address4          VARCHAR2(64 CHAR),
  city              VARCHAR2(64 CHAR),
  county            VARCHAR2(64 CHAR),
  state             VARCHAR2(64 CHAR),
  postal            VARCHAR2(16 CHAR),
  country           VARCHAR2(16 CHAR),
  address_type_id   NUMBER,
  address_type      VARCHAR2(32 CHAR),
  mq_address_hash   VARCHAR2(32 CHAR),
  integration_id    VARCHAR2(64 CHAR),
  mq_status         NUMBER CONSTRAINT FK_MQ_INBOUND_STATUS REFERENCES qbl.http_codes(code) DEFAULT 202,
  -- Audit Fields
  created_at        TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
  created_by        VARCHAR2(100 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL,
  updated_at        TIMESTAMP DEFAULT ON NULL SYSTIMESTAMP NOT NULL,
  updated_by        VARCHAR2(100 CHAR) DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER) NOT NULL
)
PARTITION BY LIST (MQ_STATUS)(
    PARTITION P_NEW     VALUES (202),
    PARTITION P_DEFAULT VALUES (DEFAULT)
) ENABLE ROW MOVEMENT;


CREATE VIEW DWH.MQ_SOURCE_VW AS
SELECT
    d.demographic_id,
    e.employee_id,
    d.first_name,
    d.middle_name,
    d.last_name,
    d.prefix_id      AS name_prefix_id,
    prefix.lookup_desc AS name_prefix,
    d.suffix_id      AS name_suffix_id,
    suffix.lookup_desc AS name_suffix,
    d.birthdate,
    d.dt_of_death,
    d.email_addr,
    d.gender_id,
    gender.lookup_desc AS gender,
    d.mar_status_id,
    mar_status.lookup_desc AS mar_status,
    e.ssn,
    d.deleted_status,
    d.created_by AS demographic_created_by,
    d.created_at AS demographic_created_at,
    d.updated_by AS demographic_updated_by,
    d.updated_at AS demographic_updated_at,

    p.phone_id,
    p.phone,
    p.phone_type_id,
    phone_type.lookup_desc AS phone_type,
    p.created_by AS phone_created_by,
    p.created_at AS phone_created_at,
    p.updated_by AS phone_updated_by,
    p.updated_at AS phone_updated_at,

    a.address_id,
    a.address1,
    a.address2,
    a.address3,
    a.address4,
    a.city,
    a.county,
    a.state,
    a.postal,
    a.country,
    a.address_type_id,
    address_type.lookup_desc AS address_type,
    a.created_by AS address_created_by,
    a.created_at AS address_created_at,
    a.updated_by AS address_updated_by,
    a.updated_at AS address_updated_at,

    RAWTOHEX(STANDARD_HASH(
        NVL(d.first_name, CHR(0)) || CHR(30) ||
        NVL(d.middle_name, CHR(0)) || CHR(30) ||
        NVL(d.last_name, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.prefix_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.suffix_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.birthdate, 'YYYY-MM-DD'), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.dt_of_death, 'YYYY-MM-DD'), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.gender_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(d.mar_status_id), CHR(0)) || CHR(30) ||
        NVL(d.email_addr, CHR(0)),
        'MD5'
    )) AS mq_demographic_hash,

    RAWTOHEX(STANDARD_HASH(
        NVL(p.phone, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(p.phone_type_id), CHR(0)),
        'MD5'
    )) AS mq_phone_hash,

    RAWTOHEX(STANDARD_HASH(
        NVL(a.address1, CHR(0)) || CHR(30) ||
        NVL(a.address2, CHR(0)) || CHR(30) ||
        NVL(a.address3, CHR(0)) || CHR(30) ||
        NVL(a.address4, CHR(0)) || CHR(30) ||
        NVL(a.city, CHR(0)) || CHR(30) ||
        NVL(a.county, CHR(0)) || CHR(30) ||
        NVL(a.state, CHR(0)) || CHR(30) ||
        NVL(a.postal, CHR(0)) || CHR(30) ||
        NVL(a.country, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(a.address_type_id), CHR(0)),
        'MD5'
    )) AS mq_address_hash

FROM dwh.demographic d
JOIN dwh.employee e ON d.employee_id = e.employee_id
LEFT JOIN dwh.mq_lookup prefix      ON d.prefix_id      = prefix.lookup_id
LEFT JOIN dwh.mq_lookup suffix      ON d.suffix_id      = suffix.lookup_id
LEFT JOIN dwh.mq_lookup gender      ON d.gender_id      = gender.lookup_id
LEFT JOIN dwh.mq_lookup mar_status  ON d.mar_status_id  = mar_status.lookup_id
LEFT JOIN dwh.phone p ON d.demographic_id = p.demographic_id
    AND p.phone_type_id = (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'PHONE_TYPE' AND lookup_desc = 'MAIN')
LEFT JOIN dwh.mq_lookup phone_type   ON p.phone_type_id   = phone_type.lookup_id
LEFT JOIN dwh.address a ON d.demographic_id = a.demographic_id
    AND a.address_type_id = (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'ADDRESS_TYPE' AND lookup_desc = 'HOME')
LEFT JOIN dwh.mq_lookup address_type ON a.address_type_id = address_type.lookup_id
WHERE d.deleted_status = (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'DELETED_STATUS' AND lookup_desc = 'ACTIVE');
