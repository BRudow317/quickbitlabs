/************************************************************************** Package: QBL.MQ_PKG Author: Blaine Rudow Last Edit: 2026-05-10 Description: This is a helper package for ingesting Master Data Management records in batch and incrementally from MuleSoft or another source. Helpful Links: https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/constraint.html https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/Oracle-object-types.html**************************************************************************/

CREATE OR REPLACE PACKAGE BODY qbl.mq_pkg AS
  /**************************************************************************
    Global Constant Values
  **************************************************************************/
    g_schema        CONSTANT VARCHAR2(20 CHAR) := 'PERF';
    g_package_name  CONSTANT VARCHAR2(20 CHAR) := 'MQ_PKG';
    g_package_user  CONSTANT VARCHAR2(10 CHAR) := 'MDM';
    g_mq_main_prc   CONSTANT VARCHAR2(20 CHAR) := 'PROCESS_MQ_INBOUND';
    g_thread_count  CONSTANT NUMBER            := 4;
    -- Must match the Specification exactly
    g_bulk_mode     CONSTANT BOOLEAN           := FALSE; 

  /**************************************************************************
    Cursor Implementation
  **************************************************************************/
  CURSOR c_json_payload(i_json CLOB) IS
      SELECT * FROM JSON_TABLE(i_json, '$[*]'
      COLUMNS (
        mq_id             NUMBER             PATH '$.mq_id',
        csm_mbr_demogr_id VARCHAR2(24 CHAR)  PATH '$.csm_mbr_demogr_id',
        pid               VARCHAR2(12 CHAR)  PATH '$.pid',
        first_name        VARCHAR2(64 CHAR)  PATH '$.first_name',
        middle_name       VARCHAR2(64 CHAR)  PATH '$.middle_name',
        last_name         VARCHAR2(64 CHAR)  PATH '$.last_name',
        name_prefix_id    NUMBER             PATH '$.name_prefix_id',
        name_prefix       VARCHAR2(16 CHAR)  PATH '$.name_prefix',
        name_suffix_id    NUMBER             PATH '$.name_suffix_id',
        name_suffix       VARCHAR2(16 CHAR)  PATH '$.name_suffix',
        -- Use VARCHAR2 then cast, or ensure ISO format (YYYY-MM-DD)
        birthdate         DATE               PATH '$.birthdate',
        dt_of_death       DATE               PATH '$.dt_of_death',
        email_addr        VARCHAR2(120 CHAR) PATH '$.email_addr',
        email_type        VARCHAR2(32 CHAR)  PATH '$.email_type',
        gender_id         NUMBER             PATH '$.gender_id',
        gender            VARCHAR2(32 CHAR)  PATH '$.gender',
        mar_status_id     NUMBER             PATH '$.mar_status_id',
        mar_status        VARCHAR2(32 CHAR)  PATH '$.mar_status',
        ssn               VARCHAR2(12 CHAR)  PATH '$.ssn',
        mq_demogr_hash    VARCHAR2(32 CHAR)  PATH '$.mq_demogr_hash',
        csm_mbr_phn_id    NUMBER             PATH '$.csm_mbr_phn_id',
        phone             VARCHAR2(32 CHAR)  PATH '$.phone',
        phone_type_id     NUMBER             PATH '$.phone_type_id',
        phone_type        VARCHAR2(32 CHAR)  PATH '$.phone_type',
        mq_phone_hash     VARCHAR2(32 CHAR)  PATH '$.mq_phone_hash',
        csm_mbr_addr_id   NUMBER             PATH '$.csm_mbr_addr_id',
        address1          VARCHAR2(64 CHAR)  PATH '$.address1',
        address2          VARCHAR2(64 CHAR)  PATH '$.address2',
        address3          VARCHAR2(64 CHAR)  PATH '$.address3',
        address4          VARCHAR2(64 CHAR)  PATH '$.address4',
        city              VARCHAR2(64 CHAR)  PATH '$.city',
        county            VARCHAR2(64 CHAR)  PATH '$.county',
        state             VARCHAR2(64 CHAR)  PATH '$.state',
        postal            VARCHAR2(16 CHAR)  PATH '$.postal',
        country           VARCHAR2(16 CHAR)  PATH '$.country',
        address_type      VARCHAR2(32 CHAR)  PATH '$.address_type',
        mq_address_hash   VARCHAR2(32 CHAR)  PATH '$.mq_address_hash',
        grr_id            VARCHAR2(64 CHAR)  PATH '$.grr_id',
        mulesoft_id       VARCHAR2(64 CHAR)  PATH '$.mulesoft_id',
        mq_status         NUMBER             PATH '$.mq_status',
        updated_at        TIMESTAMP          PATH '$.updated_at',
        updated_by        VARCHAR2(32 CHAR)  PATH '$.updated_by',
        created_at        TIMESTAMP          PATH '$.created_at',
        created_by        VARCHAR2(32 CHAR)  PATH '$.created_by'
    )
  );



  /**************************************************************************
    Package Logging Procedure: QBL.MQ_PKG.MQ_LOGGER 
  **************************************************************************/
  PROCEDURE mq_logger (
        i_mq_id                 IN NUMBER   DEFAULT NULL, -- Changed to NUMBER
        i_json_record           IN CLOB     DEFAULT NULL,
        i_error_code            IN NUMBER   DEFAULT NULL,
        i_error_message         IN VARCHAR2 DEFAULT NULL,
        i_error_location        IN VARCHAR2 DEFAULT NULL,
        i_mq_status_code        IN NUMBER   DEFAULT NULL, -- Changed to NUMBER
        i_procedure_name        IN VARCHAR2 DEFAULT NULL,
        i_procedure_description IN VARCHAR2 DEFAULT NULL,
        i_package_name          IN VARCHAR2 DEFAULT g_package_name
      )
    AS
      PRAGMA AUTONOMOUS_TRANSACTION; -- Correct placement
    BEGIN
      INSERT INTO qbl.mq_pkg_log (
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
        ROLLBACK; -- Critical: Autonomous transactions must close their own TX
        RAISE;
  END mq_logger;




/******************************************************* QBL.MQ_PKG.PROCESS_MQ_INBOUND *******************************************************/ PROCEDURE process_mq_inbound ( i_thread_id IN NUMBER, i_starting_mq_id IN NUMBER := 0, i_chunk_size IN NUMBER := 0,  i_thread_count IN NUMBER := g_thread_count, i_status_type IN VARCHAR2 := '202',  i_bulk_mode IN BOOLEAN := g_bulk_mode, i_pid_list IN CLOB DEFAULT EMPTY_CLOB() ) AS v_procedure_name VARCHAR2(100 CHAR) := g_mq_main_prc; v_procedure_desc VARCHAR2(100 CHAR) := v_procedure_name || ' is the main processing procedure of the ' || g_package_name || '. There are 4 outcomes: SUCCESS, ERROR, ' || 'NO_CHANGE (Old Data does not differ from the processed data)' || ', and NOT_FOUND (A PID was given not found in ERM.)' ; v_demo_changed BOOLEAN := FALSE; v_addr_changed BOOLEAN := FALSE; v_phone_changed BOOLEAN := FALSE; v_user VARCHAR2(100 CHAR) := nvl(sys_context('userenv','client_identifier'),user); CURSOR mq_inbound_cursor IS SELECT mqi.*, mqshv.* FROM qbl.mq_inbound mqi LEFT JOIN qbl.mq_source_hash_vw mqshv ON mqshv.csm_pen_id = mqi.pid WHERE mq_status = i_status_type AND ora_hash( mq_id, i_thread_count - 1 ) = i_thread_id - 1 --orahash starts at 0 
ORDER BY mq_id ASC FOR UPDATE OF mq_id SKIP LOCKED; TYPE t_row_type IS TABLE OF mq_inbound_cursor%rowtype; v_batch t_row_type; BEGIN OPEN mq_inbound_cursor; LOOP --batch loop 
FETCH mq_inbound_cursor BULK COLLECT INTO v_batch LIMIT 2000; EXIT WHEN v_batch.count = 0; 





          FOR i in 1 .. v_batch.COUNT LOOP 
            BEGIN -- 1 by 1 record loop
              SAVEPOINT start_of_row;
              
              -- FIX 2: Reset flags for EVERY record
              v_demo_changed  := FALSE;
              v_addr_changed  := FALSE;
              v_phone_changed := FALSE;
                          
              IF v_batch(i).csm_pen_id IS NULL THEN
                  UPDATE qbl.mq_inbound mqi
                  SET mqi.mq_status = 404, updated_at = SYSTIMESTAMP, updated_by = 'MDM'
                  WHERE mqi.mq_id = v_batch(i).mq_id;
                  CONTINUE;
              END IF;

              /* Demographic Section */
              IF v_batch(i).mq_demo_hash != v_batch(i).src_demo_hash THEN
                  v_demo_changed := TRUE;
                  UPDATE qbl.ps_csm_mbr_demogr d
                  SET d.csm_first_nm = v_batch(i).first_name,
                      -- ... rest of your columns ...
                      d.updated_by = 'MDM'
                  WHERE d.csm_mbr_demogr_id = v_batch(i).csm_mbr_demogr_id;
              END IF;

              /* Address Section */
              IF v_batch(i).mq_address_hash != v_batch(i).src_address_hash THEN
                  v_addr_changed := TRUE;
                  UPDATE qbl.ps_csm_mbr_addr addr
                  SET addr.csm_addr1 = v_batch(i).address1,
                      -- ... rest of your columns ...
                      addr.updated_by = 'MDM'
                  WHERE addr.csm_mbr_addr_id = v_batch(i).csm_mbr_addr_id;
              END IF;

              /* PHONE SECTION - FIX 1: Corrected Hash Check */
              IF v_batch(i).mq_phone_hash != v_batch(i).src_phone_hash THEN
                  v_phone_changed := TRUE;
                  UPDATE qbl.ps_csm_mbr_phn phn
                  SET phn.csm_phn = v_batch(i).phone,
                      phn.update_date = SYSTIMESTAMP,
                      phn.updated_by = 'MDM'
                  WHERE phn.csm_mbr_phn_id = v_batch(i).csm_mbr_phn_id;
              END IF;

              IF v_demo_changed OR v_addr_changed OR v_phone_changed THEN
                  IF i_bulk_mode THEN
                      -- FIX 3: Corrected procedure name
                      census_sentinel(
                          i_pen_id => v_batch(i).pid,
                          -- ... params ...
                      );
                  END IF;

                  UPDATE qbl.mq_inbound mqi
                  SET mqi.mq_status = 200, updated_at = SYSTIMESTAMP, updated_by = v_user
                  WHERE mqi.mq_id = v_batch(i).mq_id;
              ELSE
                  -- 304 No Change
                  UPDATE qbl.mq_inbound mqi
                  SET mqi.mq_status = 304, updated_at = SYSTIMESTAMP, updated_by = v_user
                  WHERE mqi.mq_id = v_batch(i).mq_id;
              END IF;

            EXCEPTION WHEN OTHERS THEN
                ROLLBACK TO start_of_row;
                -- Log the error to your table
                mq_logger(
                    i_mq_id => v_batch(i).mq_id,
                    i_error_code => SQLCODE,
                    i_error_message => SQLERRM,
                    i_procedure_name => v_procedure_name
                );
                UPDATE qbl.mq_inbound mqi
                SET mqi.mq_status = 500, updated_at = SYSTIMESTAMP, updated_by = v_user
                WHERE mqi.mq_id = v_batch(i).mq_id;
            END; 
          END LOOP;
          COMMIT; -- Commit the 2k batch


  /*******************************************************
    Procedure: assign_thread
  *******************************************************/
  PROCEDURE assign_thread (
      i_job_name VARCHAR2 := 'MQ_INBOUND_THREADER_', i_thread_id NUMBER
    ) 
    AS
    BEGIN
      dbms_scheduler.run_job(
        i_job_name || i_thread_id, use_current_session => FALSE
      );
      EXCEPTION
        WHEN OTHERS THEN
          -- ORA-27478: job is already running
          -- ORA-27431: job not in a state to run
          IF sqlcode IN (-27478, -27431) THEN NULL;
          ELSE RAISE;
          END IF;
  END assign_thread; -- FIX: Name must match header

  /**************************************************************************
    Procedure: build_multi_threader
  **************************************************************************/
  PROCEDURE build_multi_threader (
        i_job_name     VARCHAR2 := 'MQ_INBOUND_THREADER',
        i_job_type     VARCHAR2 := 'PLSQL_BLOCK',
        i_schema       VARCHAR2 := g_schema,
        i_pkg_name     VARCHAR2 := g_package_name,
        i_prc_name     VARCHAR2 := g_mq_main_prc,
        i_thread_count NUMBER   := g_thread_count,
        i_enabled      BOOLEAN  := TRUE,
        i_auto_drop    BOOLEAN  := FALSE
      ) 
    AS
      v_sentinel NUMBER := 0;
      v_job_action VARCHAR2(1000);
    BEGIN
      FOR i IN 1..i_thread_count LOOP
        -- FIX: Corrected Named Notation (parameter_name => value)
        v_job_action := 'BEGIN '
            || i_schema || '.' || i_pkg_name || '.' || i_prc_name || '('
            || 'i_thread_id => '    || i              || ', '
            || 'i_thread_count => ' || i_thread_count 
            || '); END;';

        SELECT COUNT(*)
          INTO v_sentinel
          FROM user_scheduler_jobs
          WHERE job_name = i_job_name || '_' || i;

        IF v_sentinel = 0 THEN
          dbms_scheduler.create_job(
            job_name   => i_job_name || '_' || i,
            job_type   => i_job_type,
            job_action => v_job_action,
            enabled    => i_enabled,
            auto_drop  => i_auto_drop
          );
        END IF;
      END LOOP;
  END build_multi_threader; -- FIX: Name must match header


  /**************************************************************************
    Procedure: census_sentinel
  **************************************************************************/
  PROCEDURE census_sentinel ( mqi_record IN qbl.mq_inbound%ROWTYPE ) 
    AS
      v_procedure_name VARCHAR2(32 CHAR) := 'CENSUS_SENTINEL';
      v_procedure_description VARCHAR2(480 CHAR) := 'Updates temporary census record to prevent nightly delta detection.';
    BEGIN
      UPDATE qbl.ps_csm_voya_census
       SET 
        csm_first_nm    = mqi_record.first_name,
        csm_middle_intl = SUBSTR(mqi_record.middle_name, 1, 1), -- Oracle SUBSTR is 1-indexed
        csm_last_nm     = mqi_record.last_name,  -- FIXED typo
        csm_birth_dt    = mqi_record.birthdate,
        csm_death_dt    = mqi_record.dt_of_death,
        csm_addr1       = mqi_record.address1,
        csm_addr2       = mqi_record.address2,
        csm_addr3       = mqi_record.address3,
        csm_city        = mqi_record.city,
        csm_state       = mqi_record.state,
        csm_postal      = mqi_record.postal,
        csm_country     = mqi_record.country,
        csm_phn         = mqi_record.phone,
        csm_email       = mqi_record.email_addr, -- FIXED typo
        csm_marital_st  = mqi_record.mar_status, -- FIXED typo
        csm_mbr_gender  = mqi_record.gender,
        updated_at      = SYSTIMESTAMP,
        updated_by      = 'MQ_PKG'
      WHERE csm_pen_id = mqi_record.pid;

    EXCEPTION
      WHEN OTHERS THEN
        mq_logger(
          i_mq_id                 => mqi_record.mq_id,
          -- Ensure mqi_to_json is defined in the package body!
          i_json_record           => mqi_to_json(mqi_record), 
          i_error_code            => SQLCODE,
          i_error_message         => DBMS_UTILITY.FORMAT_ERROR_STACK,
          i_error_location        => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
          i_procedure_name        => v_procedure_name,
          i_procedure_description => v_procedure_description,
          i_package_name          => g_package_name
        );
        RAISE;
  END census_sentinel;

  /**************************************************************************
    Function: mqi_to_json
  **************************************************************************/
  FUNCTION mqi_to_json(mqi_record IN qbl.mq_inbound%ROWTYPE) 
    RETURN CLOB 
    AS
      v_procedure_name VARCHAR2(30) := 'MQI_TO_JSON';
      v_procedure_description VARCHAR2(200) := 'Serializes MQ_INBOUND record to JSON.';
      o_json CLOB;
    BEGIN
      -- JSON_OBJECT logic is solid
      o_json := JSON_OBJECT(
        -- ... your existing mapping ...
        RETURNING CLOB 
      );
      
      RETURN JSON_SERIALIZE(o_json PRETTY);
      
    EXCEPTION
      WHEN OTHERS THEN
        -- FIX: Column name must match %ROWTYPE (mq_status, not mq_status_code)
        -- Note: Assigning to an IN parameter record requires it to be IN OUT,
        -- but since we are just logging, we can pass 500 directly to the logger.
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
  PROCEDURE mq_inbound_loader( mqi_record IN qbl.mq_inbound%ROWTYPE ) 
    AS
      v_procedure_name VARCHAR2(32 CHAR) := 'MQ_INBOUND_LOADER';
    BEGIN
      SAVEPOINT start_of_row;
      -- FIX: Added parentheses for record-based insert
      INSERT INTO qbl.mq_inbound VALUES (mqi_record); 
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
    BEGIN
      SAVEPOINT start_of_batch;
      INSERT INTO qbl.mq_inbound 
        SELECT * FROM c_json_payload(i_payload);
      COMMIT;
      
      -- TRIGGER THREADS HERE (Optional)
      -- assign_thread(1); 
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK TO start_of_batch;
        mq_json_rbr_loader(i_payload);
  END mq_json_loader;

  /**************************************************************************
    Procedure: mq_json_rbr_loader
  **************************************************************************/
  PROCEDURE mq_json_rbr_loader(i_payload IN CLOB) 
    AS
      v_procedure_name VARCHAR2(30 CHAR) := 'MQ_JSON_RBR_LOADER';
      v_procedure_description VARCHAR2(500 CHAR) := 'Row by row fallback for bulk JSON ingestion.';
    BEGIN
      FOR mqi_record IN c_json_payload(i_payload) LOOP
        BEGIN
          mq_inbound_loader(mqi_record);
        EXCEPTION 
          WHEN OTHERS THEN
            -- FIX 1: Removed (i) index
            mq_logger(
                i_mq_id                 => mqi_record.mq_id,
                i_json_record           => mqi_to_json(mqi_record),
                i_error_code            => SQLCODE,
                i_error_message         => DBMS_UTILITY.FORMAT_ERROR_STACK,
                i_error_location        => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                i_mq_status_code        => 500,
                i_procedure_name        => v_procedure_name,
                i_procedure_description => v_procedure_description,
                i_package_name          => g_package_name
            );
            -- FIX 2: Removed read-only assignment mqi_record.mq_status_code := 500;
        END;
      END LOOP;
      COMMIT;
  END mq_json_rbr_loader;

  /**************************************************************************
    Procedure: qbl.mq_pkg.mq_truncate
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

          IF vln_table_exists = 1 THEN -- FIX: Name matched
              EXECUTE IMMEDIATE 'TRUNCATE TABLE '||ilv_schema||'.'||ilv_table;  
              -- No COMMIT needed for DDL
          END IF;
  END mq_truncate;

  /**************************************************************************
    Procedure: qbl.mq_pkg.mq_outbound_loader
  **************************************************************************/
  PROCEDURE mq_outbound_loader (
      ilb_reload BOOLEAN := false,
      iov_schema VARCHAR2 := g_schema,
      iov_table  VARCHAR2 := 'MQ_OUTBOUND'
    )
    AS
    BEGIN
    IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF;

    INSERT INTO qbl.mq_outbound (
        csm_mbr_demogr_id, pid, first_name, middle_name, last_name,
        name_prefix_id, name_prefix, name_suffix_id, name_suffix,
        birthdate, dt_of_death, email_addr, gender_id, gender,
        mar_status_id, mar_status, ssn, demogr_trf_pre96_flg,
        demogr_status_id, demogr_created_by, demogr_create_date,
        demogr_updated_by, demogr_updated_date, -- FIX: Changed update_date to match table
        csm_mbr_phn_id, phone, phone_type_id, phone_type, phone_ext,
        phone_created_by, phone_create_date, phone_updated_by, phone_update_date,
        csm_mbr_addr_id, address1, address2, address3, address4,
        city, county, state, postal, country,
        /* address_type, */ -- Removed to match SELECT count
        address_created_by, address_create_date, address_updated_by, address_updated_date,
        updated_at, updated_by
      )
      SELECT 
        mqmvw.csm_mbr_demogr_id, mqmvw.pid, mqmvw.first_name, mqmvw.middle_name, mqmvw.last_name,
        mqmvw.name_prefix_id, mqmvw.name_prefix, mqmvw.name_suffix_id, mqmvw.name_suffix,
        mqmvw.birthdate, mqmvw.dt_of_death, mqmvw.email_addr, mqmvw.gender_id, mqmvw.gender,
        mqmvw.mar_status_id, mqmvw.mar_status, mqmvw.ssn, mqmvw.demogr_trf_pre96_flg,
        mqmvw.demogr_status_id, mqmvw.demogr_created_by, mqmvw.demogr_create_date,
        mqmvw.demogr_updated_by, mqmvw.demogr_update_date,
        mqmvw.csm_mbr_phn_id, mqmvw.phone, mqmvw.phone_type_id, mqmvw.phone_type, mqmvw.phone_ext,
        mqmvw.phone_created_by, mqmvw.phone_create_date, mqmvw.phone_updated_by, mqmvw.phone_update_date,
        mqmvw.csm_mbr_addr_id, mqmvw.address1, mqmvw.address2, mqmvw.address3, mqmvw.address4,
        mqmvw.city, mqmvw.county, mqmvw.state, mqmvw.postal, mqmvw.country,
        mqmvw.address_created_by, mqmvw.address_create_date, mqmvw.address_updated_by, mqmvw.address_update_date,
        mqmvw.retrieved_at, mqmvw.retrieved_by
      FROM qbl.mq_member_vw mqmvw
      WHERE mqmvw.csm_mbr_demogr_id IN (SELECT csm_mbr_demogr_id FROM qbl.ps_csm_mbr)
      ORDER BY mqmvw.pid; -- FIX: Valid Alias

    COMMIT;
  END mq_outbound_loader;


  /**************************************************************************
    Procedure: qbl.mq_pkg.mq_outbound_erm_plan_loader
  **************************************************************************/  
  PROCEDURE mq_outbound_erm_plan_loader (
      ilb_reload BOOLEAN := false,
      iov_schema VARCHAR2 := g_schema,
      iov_table  VARCHAR2 := 'MQ_OUTBOUND_ERM_PLAN'
    )
    AS
    BEGIN
      /* Truncates all the table data */
      IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF;

      INSERT INTO qbl.mq_outbound_erm_plan (
        mq_id,
        pid,
        csm_mbr_demogr_id,
        plan_status,
        plan_code
      )
      SELECT
        mqo_em.mq_id,
        mqo_em.pid,
        mqo_em.csm_mbr_demogr_id,
        sub_mfs.csm_fnd_st_typ_ds AS plan_status,
        sub_mfs.fund_code AS plan_code
      -- FIX: Corrected table name from mq_outbound_erm_member to mq_outbound
      FROM qbl.mq_outbound mqo_em 
      INNER JOIN (
        SELECT 
          mfs.csm_mbr_demogr_id,
          mfs.csm_fnd_st_date,
          st.csm_fnd_st_typ_ds,
          f.fund_code
        FROM qbl.ps_csm_mbr_fnd_st mfs
        INNER JOIN (
          SELECT
            csm_mbr_demogr_id,
            csm_fund_id,
            MAX(csm_fnd_st_date) AS max_st_date
          FROM qbl.ps_csm_mbr_fnd_st
          WHERE TRUNC(csm_fnd_st_date) <= TRUNC(SYSDATE)
          GROUP BY csm_mbr_demogr_id, csm_fund_id
        ) suba
          ON mfs.csm_mbr_demogr_id = suba.csm_mbr_demogr_id
            AND mfs.csm_fund_id = suba.csm_fund_id
            AND mfs.csm_fnd_st_date = suba.max_st_date
        LEFT JOIN qbl.ps_csm_fnd_st_typ st
          ON st.csm_fnd_st_typ_id = mfs.csm_fnd_st_typ_id
        LEFT JOIN qbl.ps_csm_fund f
          ON f.fund_id = mfs.csm_fund_id
      ) sub_mfs
        ON mqo_em.csm_mbr_demogr_id = sub_mfs.csm_mbr_demogr_id
      ORDER BY mqo_em.pid, sub_mfs.fund_code;
    
      COMMIT;
  END mq_outbound_erm_plan_loader;

  /**************************************************************************
    Procedure: qbl.mq_pkg.mq_outbound_erm_employment_loader
  ************************************************************************/ 
  PROCEDURE mq_outbound_erm_employment_loader (
      ilb_reload BOOLEAN := false,
      iov_schema VARCHAR2 := g_schema,
      iov_table  VARCHAR2 := 'MQ_OUTBOUND_ERM_EMPLOYMENT'
    ) AS
    BEGIN
      IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF;

      INSERT INTO qbl.mq_outbound_erm_employment (
        mq_id,
        pid,
        csm_mbr_demogr_id, 
        member_id,
        plan_code,
        sub_unit_cd,
        sub_unit_name,
        hire_dt,
        termination_dt
        )
        SELECT 
          mqo_em.mq_id,
          mqo_em.pid,
          mqo_em.csm_mbr_demogr_id,
          mfv.csm_mbr_id       AS member_id,
          mfv.csm_fund_cd      AS plan_code,
          mfv.csm_sub_unit_cd  AS sub_unit_cd,
          su.csm_unit_title    AS sub_unit_name,
          mfv.csm_hire_dt      AS hire_dt,
          mfv.csm_term_dt      AS termination_dt
          -- FIX: Corrected table name from mq_outbound_erm_member to mq_outbound
          FROM qbl.mq_outbound mqo_em 
          JOIN qbl.ps_csm_mbr_fund_vw mfv 
            ON mfv.csm_pen_id = mqo_em.pid
          LEFT OUTER JOIN qbl.ps_csm_sub_unit su
            ON mfv.csm_sub_unit_id = su.sub_unit_id
          ORDER BY mqo_em.pid, mfv.csm_mbr_id;

      COMMIT;

  END mq_outbound_erm_employment_loader;

/************************************************************************** Procedure: qbl.mq_pkg.insert_mq_outbound_erm_service **************************************************************************/ PROCEDURE mq_outbound_erm_service_loader ( ilb_reload BOOLEAN := false, iov_schema VARCHAR2 := g_schema, iov_table VARCHAR2 := 'MQ_OUTBOUND_ERM_SERVICE' ) AS BEGIN /* Truncates all the table data */ IF ilb_reload THEN mq_truncate(iov_schema, iov_table); END IF; INSERT INTO qbl.mq_outbound_erm_service ( mq_id, pid, csm_mbr_demogr_id, fund_code, eligible_service, creditable_service, potential_creditable, purchased_service ) SELECT mqo_em.mq_id, mqo_em.pid, mqo_em.csm_mbr_demogr_id, csc.fund_code, SUM(NVL(csc.eligible_service, 0)) AS eligible_service, SUM(NVL(csc.creditable_service, 0)) AS creditable_service, SUM(NVL(csc.potential_creditable,0)) AS potential_creditable, SUM(NVL(csc.purchased_service, 0)) AS purchased_service FROM qbl.mq_outbound_erm_member mqo_em JOIN qbl.ps_csm_curr_svc_credit csc ON csc.pension_id = mqo_em.pension_id GROUP BY csc.pension_id, csc.fund_code ORDER BY csc.pension_id, csc.fund_code; COMMIT; END mq_outbound_erm_service_loader; 
/************************************************************************** Package Initialization Block: **************************************************************************/END mq_pkg;/