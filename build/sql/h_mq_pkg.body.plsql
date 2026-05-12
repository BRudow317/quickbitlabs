/************************************************************************** Package: DWH.MQ_PKG Author: Blaine Rudow Last Edit: 2026-05-10 Description: This is a helper package for ingesting Master Data Management records in batch and incrementally from MuleSoft or another source. Helpful Links: https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/constraint.html https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/Oracle-object-types.html**************************************************************************/

CREATE OR REPLACE PACKAGE BODY dwh.mq_pkg AS

  /**************************************************************************
    Cursor Implementation
  **************************************************************************/
  CURSOR c_json_payload(i_json CLOB) IS
    SELECT * FROM JSON_TABLE(i_json, '$[*]'
      COLUMNS (
        mq_id                  NUMBER              PATH '$.mq_id',
        demographic_id         NUMBER              PATH '$.demographic_id',
        employee_id            VARCHAR2(16 CHAR)   PATH '$.employee_id',
        first_name             VARCHAR2(64 CHAR)   PATH '$.first_name',
        middle_name            VARCHAR2(64 CHAR)   PATH '$.middle_name',
        last_name              VARCHAR2(64 CHAR)   PATH '$.last_name',
        name_prefix_id         NUMBER              PATH '$.name_prefix_id',
        name_prefix            VARCHAR2(16 CHAR)   PATH '$.name_prefix',
        name_suffix_id         NUMBER              PATH '$.name_suffix_id',
        name_suffix            VARCHAR2(16 CHAR)   PATH '$.name_suffix',
        birthdate              DATE                PATH '$.birthdate',
        dt_of_death            DATE                PATH '$.dt_of_death',
        email_addr             VARCHAR2(120 CHAR)  PATH '$.email_addr',
        email_type             VARCHAR2(32 CHAR)   PATH '$.email_type',
        gender_id              NUMBER              PATH '$.gender_id',
        gender                 VARCHAR2(32 CHAR)   PATH '$.gender',
        mar_status_id          NUMBER              PATH '$.mar_status_id',
        mar_status             VARCHAR2(32 CHAR)   PATH '$.mar_status',
        ssn                    VARCHAR2(9 CHAR)    PATH '$.ssn',
        mq_demographic_hash    VARCHAR2(32 CHAR)   PATH '$.mq_demographic_hash',
        phone_id               NUMBER              PATH '$.phone_id',
        phone                  VARCHAR2(32 CHAR)   PATH '$.phone',
        phone_type_id          NUMBER              PATH '$.phone_type_id',
        phone_type             VARCHAR2(32 CHAR)   PATH '$.phone_type',
        mq_phone_hash          VARCHAR2(32 CHAR)   PATH '$.mq_phone_hash',
        address_id             NUMBER              PATH '$.address_id',
        address1               VARCHAR2(64 CHAR)   PATH '$.address1',
        address2               VARCHAR2(64 CHAR)   PATH '$.address2',
        address3               VARCHAR2(64 CHAR)   PATH '$.address3',
        address4               VARCHAR2(64 CHAR)   PATH '$.address4',
        city                   VARCHAR2(64 CHAR)   PATH '$.city',
        county                 VARCHAR2(64 CHAR)   PATH '$.county',
        state                  VARCHAR2(64 CHAR)   PATH '$.state',
        postal                 VARCHAR2(16 CHAR)   PATH '$.postal',
        country                VARCHAR2(16 CHAR)   PATH '$.country',
        address_type_id        NUMBER              PATH '$.address_type_id',
        address_type           VARCHAR2(32 CHAR)   PATH '$.address_type',
        mq_address_hash        VARCHAR2(32 CHAR)   PATH '$.mq_address_hash',
        integration_id         VARCHAR2(64 CHAR)   PATH '$.integration_id',
        mq_status              NUMBER              PATH '$.mq_status',
        created_at             TIMESTAMP           PATH '$.created_at',
        created_by             VARCHAR2(100 CHAR)  PATH '$.created_by',
        updated_at             TIMESTAMP           PATH '$.updated_at',
        updated_by             VARCHAR2(100 CHAR)  PATH '$.updated_by'
      )
    );

  /**************************************************************************
    Package Logging Procedure: DWH.MQ_PKG.MQ_LOGGER 
  **************************************************************************/
  PROCEDURE mq_logger (
    i_mq_id                 IN NUMBER   DEFAULT NULL,
    i_json_record           IN CLOB     DEFAULT NULL,
    i_error_code            IN NUMBER   DEFAULT NULL,
    i_error_message         IN VARCHAR2 DEFAULT NULL,
    i_error_location        IN VARCHAR2 DEFAULT NULL,
    i_mq_status_code        IN NUMBER   DEFAULT NULL,
    i_procedure_name        IN VARCHAR2 DEFAULT NULL,
    i_procedure_description IN VARCHAR2 DEFAULT NULL,
    i_package_name          IN VARCHAR2 DEFAULT g_package_name
  )
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO dwh.mq_pkg_log (
      mq_id,
      json_record,
      error_code,
      error_message,
      error_location,
      mq_status_code,
      procedure_name,
      procedure_description,
      package_name
    )
    VALUES ( 
      i_mq_id,
      i_json_record,
      i_error_code,
      i_error_message,
      i_error_location,
      i_mq_status_code,
      i_procedure_name,
      i_procedure_description,
      i_package_name
    );
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK; 
      RAISE;
  END mq_logger;

  /**************************************************************************
    Procedure: DWH.MQ_PKG.PROCESS_MQ_INBOUND
  **************************************************************************/
  PROCEDURE process_mq_inbound (
    i_thread_id        IN NUMBER,
    i_starting_mq_id   IN NUMBER  := 0,
    i_chunk_size       IN NUMBER  := 0,
    i_thread_count     IN NUMBER  := g_thread_count,
    i_status_type      IN NUMBER  := 202,
    i_bulk_mode        IN BOOLEAN := g_bulk_mode
  ) AS
    v_procedure_name   VARCHAR2(100 CHAR) := g_mq_main_prc;
    v_demo_changed     BOOLEAN            := FALSE;
    v_addr_changed     BOOLEAN            := FALSE;
    v_phone_changed    BOOLEAN            := FALSE;
    v_user             VARCHAR2(100 CHAR) := NVL(SYS_CONTEXT('userenv', 'client_identifier'), USER);
    v_demographic_hash VARCHAR2(32 CHAR);
    v_address_hash     VARCHAR2(32 CHAR);
    v_phone_hash       VARCHAR2(32 CHAR);
    v_per_thread_limit NUMBER := CASE WHEN i_chunk_size > 0 THEN CEIL(i_chunk_size / i_thread_count) ELSE 0 END;
    v_processed        NUMBER := 0;

    CURSOR mq_inbound_cursor IS
      SELECT
        mqi.mq_id,
        CASE WHEN mqi.demographic_id IS NOT NULL THEN mqi.demographic_id
             ELSE (SELECT demographic_id FROM dwh.demographic WHERE employee_id = mqi.employee_id AND demographic.record_status = (select lookup_id from dwh.mq_lookup where category = 'RECORD_STATUS' and lookup_desc = 'ACTIVE'))
             END AS demographic_id,
        mqi.employee_id,
        mqi.first_name,
        mqi.middle_name,
        mqi.last_name,
        CASE WHEN mqi.name_prefix_id IS NOT NULL THEN mqi.name_prefix_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'NAME_PREFIX' AND lookup_desc = NVL(UPPER(mqi.name_prefix), NULL))
             END AS name_prefix_id,
        CASE WHEN mqi.name_prefix IS NOT NULL THEN mqi.name_prefix
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'NAME_PREFIX' AND lookup_id = mqi.name_prefix_id)
             END AS name_prefix,
        CASE WHEN mqi.name_suffix_id IS NOT NULL THEN mqi.name_suffix_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'NAME_SUFFIX' AND lookup_desc = NVL(UPPER(mqi.name_suffix), NULL))
             END AS name_suffix_id,
        CASE WHEN mqi.name_suffix IS NOT NULL THEN mqi.name_suffix
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'NAME_SUFFIX' AND lookup_id = mqi.name_suffix_id)
             END AS name_suffix,
        mqi.birthdate,
        mqi.dt_of_death,
        mqi.email_addr,
        mqi.email_type,
        CASE WHEN mqi.gender_id IS NOT NULL THEN mqi.gender_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'GENDER' AND lookup_desc = NVL(UPPER(mqi.gender), 'UNKNOWN'))
             END AS gender_id,
        CASE WHEN mqi.gender IS NOT NULL THEN mqi.gender
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'GENDER' AND lookup_id = mqi.gender_id)
             END AS gender,
        CASE WHEN mqi.mar_status_id IS NOT NULL THEN mqi.mar_status_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'MAR_STATUS' AND lookup_desc = NVL(UPPER(mqi.mar_status), 'UNKNOWN'))
             END AS mar_status_id,
        CASE WHEN mqi.mar_status IS NOT NULL THEN mqi.mar_status
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'MAR_STATUS' AND lookup_id = mqi.mar_status_id)
             END AS mar_status,
        mqi.ssn,
        mqi.mq_demographic_hash,
        CASE WHEN mqi.phone_id IS NOT NULL THEN mqi.phone_id
             ELSE (SELECT p.phone_id FROM dwh.phone p
                   WHERE p.demographic_id = mqi.demographic_id
                   AND p.phone_type_id = (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'PHONE_TYPE' AND lookup_desc = 'MAIN'))
        END AS phone_id,
        mqi.phone,
        CASE WHEN mqi.phone_type_id IS NOT NULL THEN mqi.phone_type_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'PHONE_TYPE' AND lookup_desc = NVL(UPPER(mqi.phone_type), 'MAIN'))
             END AS phone_type_id,
        CASE WHEN mqi.phone_type IS NOT NULL THEN mqi.phone_type
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'PHONE_TYPE' AND lookup_id = mqi.phone_type_id)
             END AS phone_type,
        mqi.mq_phone_hash,
        CASE WHEN mqi.address_id IS NOT NULL THEN mqi.address_id
             ELSE (SELECT a.address_id FROM dwh.address a
                   WHERE a.demographic_id = mqi.demographic_id
                   AND a.address_type_id = (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'ADDRESS_TYPE' AND lookup_desc = 'HOME'))
        END AS address_id,
        mqi.address1,
        mqi.address2,
        mqi.address3,
        mqi.address4,
        mqi.city,
        mqi.county,
        mqi.state,
        mqi.postal,
        mqi.country,
        CASE WHEN mqi.address_type_id IS NOT NULL THEN mqi.address_type_id
             ELSE (SELECT lookup_id FROM dwh.mq_lookup WHERE category = 'ADDRESS_TYPE' AND lookup_desc = NVL(UPPER(mqi.address_type), 'HOME'))
             END AS address_type_id,
        CASE WHEN mqi.address_type IS NOT NULL THEN mqi.address_type
             ELSE (SELECT lookup_desc FROM dwh.mq_lookup WHERE category = 'ADDRESS_TYPE' AND lookup_id = mqi.address_type_id)
             END AS address_type,
        mqi.mq_address_hash,
        mqi.integration_id,
        mqi.mq_status,
        mqi.created_at,
        mqi.created_by,
        mqi.updated_at,
        mqi.updated_by,
        mq_source_view.employee_id AS source_employee_id,
        mq_source_view.mq_demographic_hash AS dwh_demographic_hash,
        mq_source_view.mq_address_hash AS dwh_address_hash,
        mq_source_view.mq_phone_hash AS dwh_phone_hash
      FROM dwh.mq_inbound mqi
      LEFT JOIN dwh.mq_source_vw mq_source_view
        ON mq_source_view.employee_id = mqi.employee_id
      WHERE mqi.mq_status = i_status_type
        AND ora_hash(mqi.mq_id, i_thread_count - 1) = i_thread_id - 1
      ORDER BY mqi.mq_id ASC
      FOR UPDATE OF mqi.mq_id SKIP LOCKED;

    TYPE t_row_type IS TABLE OF mq_inbound_cursor%ROWTYPE;
    v_batch        t_row_type;
    mqi_record mq_inbound_cursor%ROWTYPE;

    ---------------------------------------------------------------------------
    -- Nested: MD5 hash of demographic fields from the enriched record.
    -- CHR(0) marks nulls; CHR(30) (ASCII Record Separator) delimits fields
    -- so that adjacent field values cannot collide across boundaries.
    -- ID values are hashed (not text) so the hash is system-agnostic.
    ---------------------------------------------------------------------------
    FUNCTION calc_demographic_hash (
      mq_inbound_cursor_record IN mq_inbound_cursor%ROWTYPE
    ) RETURN VARCHAR2 AS
    BEGIN
      RETURN RAWTOHEX(STANDARD_HASH(
        NVL(mq_inbound_cursor_record.first_name, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.middle_name, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.last_name, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.name_prefix_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.name_suffix_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.birthdate, 'YYYY-MM-DD'), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.dt_of_death, 'YYYY-MM-DD'), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.gender_id), CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.mar_status_id), CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.email_addr, CHR(0)),
        'MD5'
      ));
    END calc_demographic_hash;

    ---------------------------------------------------------------------------
    -- Nested: MD5 hash of address fields from the enriched record.
    ---------------------------------------------------------------------------
    FUNCTION calc_address_hash (
      mq_inbound_cursor_record IN mq_inbound_cursor%ROWTYPE
    ) RETURN VARCHAR2 AS
    BEGIN
      RETURN RAWTOHEX(STANDARD_HASH(
        NVL(mq_inbound_cursor_record.address1, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.address2, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.address3, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.address4, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.city, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.county, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.state, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.postal, CHR(0)) || CHR(30) ||
        NVL(mq_inbound_cursor_record.country, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.address_type_id), CHR(0)),
        'MD5'
      ));
    END calc_address_hash;

    ---------------------------------------------------------------------------
    -- Nested: MD5 hash of phone fields from the enriched record.
    ---------------------------------------------------------------------------
    FUNCTION calc_phone_hash (
      mq_inbound_cursor_record IN mq_inbound_cursor%ROWTYPE
    ) RETURN VARCHAR2 AS
    BEGIN
      RETURN RAWTOHEX(STANDARD_HASH(
        NVL(mq_inbound_cursor_record.phone, CHR(0)) || CHR(30) ||
        NVL(TO_CHAR(mq_inbound_cursor_record.phone_type_id), CHR(0)),
        'MD5'
      ));
    END calc_phone_hash;

    ---------------------------------------------------------------------------
    -- Nested: update census to prevent nightly delta detection bypass.
    -- Closes over mqi_record no parameter needed.
    ---------------------------------------------------------------------------
    PROCEDURE census_sentinel AS
    BEGIN
      UPDATE dwh.census
      SET
        first_name     = mqi_record.first_name,
        middle_initial = SUBSTR(mqi_record.middle_name, 1, 1),
        last_name      = mqi_record.last_name,
        prefix         = mqi_record.name_prefix,
        suffix         = mqi_record.name_suffix,
        birthdate      = mqi_record.birthdate,
        dt_of_death    = mqi_record.dt_of_death,
        email_addr     = mqi_record.email_addr,
        gender         = mqi_record.gender,
        mar_status     = mqi_record.mar_status,
        employee_id    = mqi_record.employee_id,
        phone          = mqi_record.phone,
        address1       = mqi_record.address1,
        address2       = mqi_record.address2,
        address3       = mqi_record.address3,
        city           = mqi_record.city,
        state          = mqi_record.state,
        postal         = mqi_record.postal,
        country        = mqi_record.country,
        updated_at     = SYSTIMESTAMP,
        updated_by     = 'MQ_PKG'
      WHERE demographic_id = mqi_record.demographic_id;
    END census_sentinel;

  BEGIN

    OPEN mq_inbound_cursor;
    LOOP
      FETCH mq_inbound_cursor BULK COLLECT INTO v_batch LIMIT 2000;
      EXIT WHEN v_batch.COUNT = 0;

      FOR i IN 1..v_batch.COUNT LOOP
        EXIT WHEN v_per_thread_limit > 0 AND v_processed >= v_per_thread_limit;
        BEGIN
          SAVEPOINT start_of_row;
          v_demo_changed  := FALSE;
          v_addr_changed  := FALSE;
          v_phone_changed := FALSE;
          mqi_record      := v_batch(i);

          IF mqi_record.source_employee_id IS NULL THEN
            UPDATE dwh.mq_inbound mqi
            SET mqi.mq_status  = 404,
                mqi.updated_at = SYSTIMESTAMP,
                mqi.updated_by = v_user
            WHERE mqi.mq_id = mqi_record.mq_id;
            CONTINUE;
          END IF;

          -- Calculate hashes from the cursor-resolved state
          v_demographic_hash := calc_demographic_hash(mqi_record);
          v_address_hash     := calc_address_hash(mqi_record);
          v_phone_hash       := calc_phone_hash(mqi_record);

          -- NVL to TRUE: missing source hash means the record has never been hashed, treat as changed
          v_demo_changed  := NVL(v_demographic_hash != mqi_record.dwh_demographic_hash, TRUE);
          v_addr_changed  := NVL(v_address_hash     != mqi_record.dwh_address_hash,     TRUE);
          v_phone_changed := NVL(v_phone_hash       != mqi_record.dwh_phone_hash,       TRUE);

          /* Demographic update */
          IF v_demo_changed THEN
            UPDATE dwh.demographic d
            SET d.first_name    = mqi_record.first_name,
                d.middle_name   = mqi_record.middle_name,
                d.last_name     = mqi_record.last_name,
                d.prefix_id     = mqi_record.name_prefix_id,
                d.suffix_id     = mqi_record.name_suffix_id,
                d.birthdate     = mqi_record.birthdate,
                d.dt_of_death   = mqi_record.dt_of_death,
                d.gender_id     = mqi_record.gender_id,
                d.mar_status_id = mqi_record.mar_status_id,
                d.email_addr    = mqi_record.email_addr,
                d.updated_at    = SYSTIMESTAMP,
                d.updated_by    = v_user
            WHERE d.demographic_id = mqi_record.demographic_id;
          END IF;

          /* Address merge — insert if no HOME address exists for this demographic */
          IF v_addr_changed THEN
            MERGE INTO dwh.address addr
            USING DUAL
            ON (addr.address_id = mqi_record.address_id)
            WHEN MATCHED THEN
              UPDATE SET addr.address1        = mqi_record.address1,
                         addr.address2        = mqi_record.address2,
                         addr.address3        = mqi_record.address3,
                         addr.address4        = mqi_record.address4,
                         addr.city            = mqi_record.city,
                         addr.county          = mqi_record.county,
                         addr.state           = mqi_record.state,
                         addr.postal          = mqi_record.postal,
                         addr.country         = mqi_record.country,
                         addr.address_type_id = mqi_record.address_type_id,
                         addr.updated_at      = SYSTIMESTAMP,
                         addr.updated_by      = v_user
            WHEN NOT MATCHED THEN
              INSERT (demographic_id, address1, address2, address3, address4, city, county, state, postal, country, address_type_id, created_at, created_by, updated_at, updated_by)
              VALUES (mqi_record.demographic_id, mqi_record.address1, mqi_record.address2, mqi_record.address3, mqi_record.address4, mqi_record.city, mqi_record.county, mqi_record.state, mqi_record.postal, mqi_record.country, mqi_record.address_type_id, SYSTIMESTAMP, v_user, SYSTIMESTAMP, v_user);

            IF mqi_record.address_id IS NULL THEN
              SELECT address_id INTO mqi_record.address_id
              FROM dwh.address
              WHERE demographic_id = mqi_record.demographic_id
              AND address_type_id  = mqi_record.address_type_id;
            END IF;
          END IF;

          /* Phone merge — insert if no MAIN phone exists for this demographic */
          IF v_phone_changed THEN
            MERGE INTO dwh.phone phn
            USING DUAL
            ON (phn.phone_id = mqi_record.phone_id)
            WHEN MATCHED THEN
              UPDATE SET phn.phone         = mqi_record.phone,
                         phn.phone_type_id = mqi_record.phone_type_id,
                         phn.updated_at    = SYSTIMESTAMP,
                         phn.updated_by    = v_user
            WHEN NOT MATCHED THEN
              INSERT (demographic_id, phone, phone_type_id, created_at, created_by, updated_at, updated_by)
              VALUES (mqi_record.demographic_id, mqi_record.phone, mqi_record.phone_type_id, SYSTIMESTAMP, v_user, SYSTIMESTAMP, v_user);

            IF mqi_record.phone_id IS NULL THEN
              SELECT phone_id INTO mqi_record.phone_id
              FROM dwh.phone
              WHERE demographic_id = mqi_record.demographic_id
              AND phone_type_id    = mqi_record.phone_type_id;
            END IF;
          END IF;

          /* Census sentinel only in bulk mode when at least one domain changed */
          IF (v_demo_changed OR v_addr_changed OR v_phone_changed) AND i_bulk_mode THEN
            census_sentinel;
          END IF;

          -- Always write enriched values and computed hashes back to mq_inbound
          UPDATE dwh.mq_inbound mqi
          SET mqi.demographic_id      = mqi_record.demographic_id,
              mqi.gender_id           = mqi_record.gender_id,
              mqi.gender              = mqi_record.gender,
              mqi.mar_status_id       = mqi_record.mar_status_id,
              mqi.mar_status          = mqi_record.mar_status,
              mqi.name_prefix_id      = mqi_record.name_prefix_id,
              mqi.name_prefix         = mqi_record.name_prefix,
              mqi.name_suffix_id      = mqi_record.name_suffix_id,
              mqi.name_suffix         = mqi_record.name_suffix,
              mqi.phone_id            = mqi_record.phone_id,
              mqi.phone_type_id       = mqi_record.phone_type_id,
              mqi.phone_type          = mqi_record.phone_type,
              mqi.address_id          = mqi_record.address_id,
              mqi.address_type_id     = mqi_record.address_type_id,
              mqi.address_type        = mqi_record.address_type,
              mqi.mq_demographic_hash = v_demographic_hash,
              mqi.mq_address_hash     = v_address_hash,
              mqi.mq_phone_hash       = v_phone_hash,
              mqi.mq_status = CASE
                              WHEN v_demo_changed 
                                OR v_addr_changed 
                                OR v_phone_changed 
                              THEN 200
                              ELSE 304
                              END,
              mqi.updated_at = SYSTIMESTAMP,
              mqi.updated_by = v_user
          WHERE mqi.mq_id = mqi_record.mq_id;
          v_processed := v_processed + 1;

        EXCEPTION WHEN OTHERS THEN
          ROLLBACK TO start_of_row;
          mq_logger(
            i_mq_id          => mqi_record.mq_id,
            i_error_code     => SQLCODE,
            i_error_message  => SQLERRM,
            i_procedure_name => v_procedure_name
          );
          UPDATE dwh.mq_inbound mqi
          SET mqi.mq_status  = 500,
              mqi.updated_at = SYSTIMESTAMP,
              mqi.updated_by = v_user
          WHERE mqi.mq_id = mqi_record.mq_id;
        END;
      END LOOP;
      COMMIT;
      EXIT WHEN v_per_thread_limit > 0 AND v_processed >= v_per_thread_limit;
    END LOOP;
    CLOSE mq_inbound_cursor;
  END process_mq_inbound;

  /*******************************************************
    Procedure: assign_thread
  *******************************************************/
  PROCEDURE assign_thread (
    i_job_name     VARCHAR2 := 'MQ_INBOUND_THREADER',
    i_thread_count NUMBER   := g_thread_count
  ) AS
  BEGIN
    FOR thread_id IN 1..i_thread_count LOOP
      BEGIN
        dbms_scheduler.run_job(i_job_name || '_' || thread_id, use_current_session => FALSE);
      EXCEPTION
        WHEN OTHERS THEN
          IF sqlcode IN (-27478, -27431) THEN NULL;
          ELSE RAISE;
          END IF;
      END;
    END LOOP;
  END assign_thread;

  /**************************************************************************
    Procedure: build_multi_threader
  **************************************************************************/
  PROCEDURE build_multi_threader (
    i_job_name        VARCHAR2 := 'MQ_INBOUND_THREADER',
    i_job_type        VARCHAR2 := 'PLSQL_BLOCK',
    i_schema          VARCHAR2 := g_schema,
    i_pkg_name        VARCHAR2 := g_package_name,
    i_prc_name        VARCHAR2 := g_mq_main_prc,
    i_thread_count    NUMBER   := g_thread_count,
    i_chunk_size      NUMBER   := 0,
    i_status_type     NUMBER   := 202,
    i_repeat_interval VARCHAR2 := NULL,
    i_enabled         BOOLEAN  := TRUE,
    i_auto_drop       BOOLEAN  := FALSE
  ) AS
    v_sentinel   NUMBER := 0;
    v_job_name   VARCHAR2(100);
    v_job_action VARCHAR2(1000);
  BEGIN
    FOR thread_id IN 1..i_thread_count LOOP
      v_job_name   := i_job_name || '_' || thread_id;
      v_job_action := 'BEGIN '
        || i_schema || '.' || i_pkg_name || '.' || i_prc_name || '('
        || 'i_thread_id => '    || thread_id     || ', '
        || 'i_thread_count => ' || i_thread_count || ', '
        || 'i_chunk_size => '   || i_chunk_size   || ', '
        || 'i_status_type => '  || i_status_type
        || '); END;';

      SELECT COUNT(*) INTO v_sentinel
      FROM all_scheduler_jobs
      WHERE owner = USER AND job_name = v_job_name;

      IF v_sentinel = 0 THEN
        dbms_scheduler.create_job(
          job_name        => v_job_name,
          job_type        => i_job_type,
          job_action      => v_job_action,
          repeat_interval => i_repeat_interval,
          enabled         => i_enabled,
          auto_drop       => i_auto_drop
        );
      ELSE
        dbms_scheduler.set_job_action(job_name => v_job_name, job_action => v_job_action);
        IF i_repeat_interval IS NOT NULL THEN
          dbms_scheduler.set_attribute(name => v_job_name, attribute => 'repeat_interval', value => i_repeat_interval);
        END IF;
      END IF;
    END LOOP;
  END build_multi_threader;

  /**************************************************************************
    Procedure: bulk_process_mq_inbound
  **************************************************************************/
  PROCEDURE bulk_process_mq_inbound (
    i_chunk_size   IN NUMBER   := 0,
    i_thread_count IN NUMBER   := g_thread_count,
    i_status_type  IN NUMBER   := 202,
    i_job_name     IN VARCHAR2 := 'MQ_INBOUND_THREADER'
  ) AS
  BEGIN
    build_multi_threader(
      i_job_name     => i_job_name,
      i_thread_count => i_thread_count,
      i_chunk_size   => i_chunk_size,
      i_status_type  => i_status_type
    );
    assign_thread(i_job_name => i_job_name, i_thread_count => i_thread_count);
  END bulk_process_mq_inbound;

  /**************************************************************************
    Function: mqi_to_json
  **************************************************************************/
  FUNCTION mqi_to_json(mqi_record IN dwh.mq_inbound%ROWTYPE) 
  RETURN CLOB 
  AS
    v_procedure_name VARCHAR2(30) := 'MQI_TO_JSON';
    v_procedure_description VARCHAR2(200) := 'Serializes MQ_INBOUND record to JSON.';
    o_json CLOB;
  BEGIN
    o_json := JSON_OBJECT(
      'mq_id'               VALUE mqi_record.mq_id,
      'demographic_id'      VALUE mqi_record.demographic_id,
      'employee_id'         VALUE mqi_record.employee_id,
      'first_name'          VALUE mqi_record.first_name,
      'middle_name'         VALUE mqi_record.middle_name,
      'last_name'           VALUE mqi_record.last_name,
      'name_prefix_id'      VALUE mqi_record.name_prefix_id,
      'name_prefix'         VALUE mqi_record.name_prefix,
      'name_suffix_id'      VALUE mqi_record.name_suffix_id,
      'name_suffix'         VALUE mqi_record.name_suffix,
      'birthdate'           VALUE mqi_record.birthdate,
      'dt_of_death'         VALUE mqi_record.dt_of_death,
      'email_addr'          VALUE mqi_record.email_addr,
      'email_type'          VALUE mqi_record.email_type,
      'gender_id'           VALUE mqi_record.gender_id,
      'gender'              VALUE mqi_record.gender,
      'mar_status_id'       VALUE mqi_record.mar_status_id,
      'mar_status'          VALUE mqi_record.mar_status,
      'ssn'                 VALUE mqi_record.ssn,
      'mq_demographic_hash' VALUE mqi_record.mq_demographic_hash,
      'phone_id'            VALUE mqi_record.phone_id,
      'phone'               VALUE mqi_record.phone,
      'phone_type_id'       VALUE mqi_record.phone_type_id,
      'phone_type'          VALUE mqi_record.phone_type,
      'phone_ext'           VALUE mqi_record.phone_ext,
      'mq_phone_hash'       VALUE mqi_record.mq_phone_hash,
      'address_id'          VALUE mqi_record.address_id,
      'address1'            VALUE mqi_record.address1,
      'address2'            VALUE mqi_record.address2,
      'address3'            VALUE mqi_record.address3,
      'address4'            VALUE mqi_record.address4,
      'city'                VALUE mqi_record.city,
      'county'              VALUE mqi_record.county,
      'state'               VALUE mqi_record.state,
      'postal'              VALUE mqi_record.postal,
      'country'             VALUE mqi_record.country,
      'address_type_id'     VALUE mqi_record.address_type_id,
      'address_type'        VALUE mqi_record.address_type,
      'mq_address_hash'     VALUE mqi_record.mq_address_hash,
      'integration_id'      VALUE mqi_record.integration_id,
      'mq_status'           VALUE mqi_record.mq_status,
      'created_at'          VALUE mqi_record.created_at,
      'created_by'          VALUE mqi_record.created_by,
      'updated_at'          VALUE mqi_record.updated_at,
      'updated_by'          VALUE mqi_record.updated_by
      RETURNING CLOB 
    );
    
    RETURN JSON_SERIALIZE(o_json PRETTY);
    
  EXCEPTION
    WHEN OTHERS THEN
      mq_logger(
          i_mq_id          => mqi_record.mq_id,
          i_json_record    => o_json,
          i_error_code     => sqlcode,
          i_error_message  => dbms_utility.format_error_stack,
          i_error_location => dbms_utility.format_error_backtrace,
          i_mq_status_code => 500, 
          i_procedure_name => v_procedure_name,
          i_package_name   => g_package_name
      );
      RAISE;
  END mqi_to_json;

  /**************************************************************************
    Procedure: mq_inbound_loader
  **************************************************************************/
  PROCEDURE mq_inbound_loader( mqi_record IN dwh.mq_inbound%ROWTYPE ) 
  AS
    v_procedure_name VARCHAR2(32 CHAR) := 'MQ_INBOUND_LOADER';
  BEGIN
    SAVEPOINT start_of_row;
    INSERT INTO dwh.mq_inbound VALUES mqi_record; 
    COMMIT;
  EXCEPTION 
    WHEN OTHERS THEN
      ROLLBACK TO start_of_row;
      mq_logger(
        i_mq_id          => mqi_record.mq_id,
        i_json_record    => mqi_to_json(mqi_record),
        i_error_code     => sqlcode,
        i_error_message  => dbms_utility.format_error_stack,
        i_procedure_name => v_procedure_name,
        i_package_name   => g_package_name
      );
      RAISE;
  END mq_inbound_loader;

  /**************************************************************************
    Procedure: mq_json_loader
  **************************************************************************/
  PROCEDURE mq_json_loader (i_payload IN CLOB)
  AS
    v_procedure_name VARCHAR2(30 CHAR) := 'MQ_JSON_LOADER';
    TYPE t_batch IS TABLE OF dwh.mq_inbound%ROWTYPE;
    v_batch t_batch;
  BEGIN
    SAVEPOINT start_of_batch;
    OPEN c_json_payload(i_payload);
    FETCH c_json_payload BULK COLLECT INTO v_batch;
    CLOSE c_json_payload;
    FORALL i IN 1..v_batch.COUNT
      INSERT INTO dwh.mq_inbound VALUES v_batch(i);
    COMMIT;
    assign_thread;
  EXCEPTION
    WHEN OTHERS THEN
      IF c_json_payload%ISOPEN THEN CLOSE c_json_payload; END IF;
      ROLLBACK TO start_of_batch;
      mq_json_rbr_loader(i_payload);
  END mq_json_loader;

  /**************************************************************************
    Procedure: mq_json_rbr_loader
  **************************************************************************/
  PROCEDURE mq_json_rbr_loader(i_payload IN CLOB)
  AS
    v_procedure_name        VARCHAR2(30 CHAR)  := 'MQ_JSON_RBR_LOADER';
    v_procedure_description VARCHAR2(500 CHAR) := 'Row by row fallback for bulk JSON ingestion.';
    v_rec dwh.mq_inbound%ROWTYPE;
  BEGIN
    OPEN c_json_payload(i_payload);
    LOOP
      FETCH c_json_payload INTO v_rec;
      EXIT WHEN c_json_payload%NOTFOUND;
      BEGIN
        mq_inbound_loader(v_rec);
      EXCEPTION
        WHEN OTHERS THEN
          mq_logger(
            i_mq_id                 => v_rec.mq_id,
            i_json_record           => mqi_to_json(v_rec),
            i_error_code            => SQLCODE,
            i_error_message         => DBMS_UTILITY.FORMAT_ERROR_STACK,
            i_error_location        => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
            i_mq_status_code        => 500,
            i_procedure_name        => v_procedure_name,
            i_procedure_description => v_procedure_description,
            i_package_name          => g_package_name
          );
      END;
    END LOOP;
    CLOSE c_json_payload;
    COMMIT;
    assign_thread;
  EXCEPTION
    WHEN OTHERS THEN
      IF c_json_payload%ISOPEN THEN CLOSE c_json_payload; END IF;
      RAISE;
  END mq_json_rbr_loader;

  /**************************************************************************
    Procedure: dwh.mq_pkg.mq_truncate
  **************************************************************************/
  PROCEDURE mq_truncate (
    ilv_schema VARCHAR2 := g_schema,
    ilv_table  VARCHAR2 := NULL
  )
  AS
    vln_table_exists NUMBER := 0;
  BEGIN
    SELECT COUNT(*) INTO vln_table_exists
    FROM all_tables
    WHERE owner = UPPER(ilv_schema)
      AND table_name = UPPER(ilv_table);

    IF vln_table_exists = 1 THEN
      EXECUTE IMMEDIATE 'TRUNCATE TABLE '||ilv_schema||'.'||ilv_table;  
    END IF;
  END mq_truncate;

  /**************************************************************************
    Procedure: dwh.mq_pkg.mq_outbound_loader
  **************************************************************************/
  PROCEDURE mq_outbound_loader (
    ilb_reload BOOLEAN := false,
    iov_schema VARCHAR2 := g_schema,
    iov_table  VARCHAR2 := 'MQ_OUTBOUND'
  )
  AS
  BEGIN
    IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF;

    INSERT INTO dwh.mq_outbound (
      demographic_id, employee_id, first_name, middle_name, last_name,
      name_prefix_id, name_prefix, name_suffix_id, name_suffix,
      birthdate, dt_of_death, email_addr, gender_id, gender,
      mar_status_id, mar_status, ssn, 
      demogr_created_by, demogr_created_at,
      demogr_updated_by, demogr_updated_at,
      phone_id, phone, phone_type_id, phone_type, phone_ext,
      phone_created_by, phone_created_at, phone_updated_by, phone_updated_at,
      address_id, address1, address2, address3, address4,
      city, county, state, postal, country,
      address_type_id, address_type,
      address_created_by, address_created_at, address_updated_by, address_updated_at,
      updated_at, updated_by
    )
    SELECT 
      mqmvw.demographic_id, mqmvw.employee_id, mqmvw.first_name, mqmvw.middle_name, mqmvw.last_name,
      mqmvw.name_prefix_id, mqmvw.name_prefix, mqmvw.name_suffix_id, mqmvw.name_suffix,
      mqmvw.birthdate, mqmvw.dt_of_death, mqmvw.email_addr, mqmvw.gender_id, mqmvw.gender,
      mqmvw.mar_status_id, mqmvw.mar_status, mqmvw.ssn, 
      mqmvw.demographic_created_by, mqmvw.demographic_created_at,
      mqmvw.demographic_updated_by, mqmvw.demographic_updated_at,
      mqmvw.phone_id, mqmvw.phone, mqmvw.phone_type_id, mqmvw.phone_type, NULL,
      mqmvw.phone_created_by, mqmvw.phone_created_at, mqmvw.phone_updated_by, mqmvw.phone_updated_at,
      mqmvw.address_id, mqmvw.address1, mqmvw.address2, mqmvw.address3, mqmvw.address4,
      mqmvw.city, mqmvw.county, mqmvw.state, mqmvw.postal, mqmvw.country,
      mqmvw.address_type_id, mqmvw.address_type,
      mqmvw.address_created_by, mqmvw.address_created_at, mqmvw.address_updated_by, mqmvw.address_updated_at,
      SYSTIMESTAMP, 'MQ_PKG'
    FROM dwh.mq_source_vw mqmvw;

    COMMIT;
  END mq_outbound_loader;

  /**************************************************************************
    Procedure: dwh.mq_pkg.mq_outbound_employment_loader
  **************************************************************************/ 
  PROCEDURE mq_outbound_employment_loader (
    ilb_reload BOOLEAN := false,
    iov_schema VARCHAR2 := g_schema,
    iov_table  VARCHAR2 := 'MQ_OUTBOUND_EMPLOYMENT'
  ) AS
  BEGIN
    IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF;

    -- Note: mq_outbound_employment table not defined in provided SQL, 
    -- but aligning based on employment table structure.
    NULL; 
  END mq_outbound_employment_loader;

  /**************************************************************************
    Package Initialization Block:
  **************************************************************************/
BEGIN
  DBMS_SHARED_POOL.KEEP('DWH.MQ_PKG', 'P');
END mq_pkg;
/
