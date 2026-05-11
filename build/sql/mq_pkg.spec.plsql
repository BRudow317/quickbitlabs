/**************************************************************************
  Package: dwh.MQ_PKG 
  Author: Blaine Rudow 
  Last Edit: 2026-05-10 
  Description: This is a helper package for ingesting Master Data Management 
               records in batch and incrementally from MuleSoft or another source.
**************************************************************************/

CREATE OR REPLACE PACKAGE dwh.mq_pkg AS

  /**************************************************************************
    Global Constant Values
  **************************************************************************/
  g_schema        CONSTANT VARCHAR2(20 CHAR) := 'DWH';
  g_package_name  CONSTANT VARCHAR2(20 CHAR) := 'MQ_PKG';
  g_package_user  CONSTANT VARCHAR2(10 CHAR) := 'PLUGIN';
  g_mq_main_prc   CONSTANT VARCHAR2(20 CHAR) := 'PROCESS_MQ_INBOUND';
  g_thread_count  CONSTANT NUMBER            := 4;
  g_bulk_mode     CONSTANT BOOLEAN           := FALSE; 

  /**************************************************************************
    Public Procedures and Functions
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
  );

  PROCEDURE process_mq_inbound ( 
    i_thread_id      IN NUMBER, 
    i_starting_mq_id IN NUMBER   := 0, 
    i_chunk_size     IN NUMBER   := 0,  
    i_thread_count   IN NUMBER   := g_thread_count, 
    i_status_type    IN NUMBER   := 202,  
    i_bulk_mode      IN BOOLEAN  := g_bulk_mode
  );

  PROCEDURE bulk_process_mq_inbound (
    i_chunk_size   IN NUMBER   := 0,
    i_thread_count IN NUMBER   := g_thread_count,
    i_status_type  IN NUMBER   := 202,
    i_job_name     IN VARCHAR2 := 'MQ_INBOUND_THREADER'
  );

  PROCEDURE assign_thread (
    i_job_name     VARCHAR2 := 'MQ_INBOUND_THREADER',
    i_thread_count NUMBER   := g_thread_count
  );

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
  );

  FUNCTION mqi_to_json(mqi_record IN dwh.mq_inbound%ROWTYPE) RETURN CLOB;

  PROCEDURE mq_inbound_loader( mqi_record IN dwh.mq_inbound%ROWTYPE );

  PROCEDURE mq_json_loader (i_payload IN CLOB);

  PROCEDURE mq_json_rbr_loader(i_payload IN CLOB);

  PROCEDURE mq_truncate (
    ilv_schema VARCHAR2 := g_schema,
    ilv_table  VARCHAR2 := NULL
  );

  PROCEDURE mq_outbound_loader (
    ilb_reload BOOLEAN  := false,
    iov_schema VARCHAR2 := g_schema,
    iov_table  VARCHAR2 := 'MQ_OUTBOUND'
  );

  PROCEDURE mq_outbound_employment_loader (
    ilb_reload BOOLEAN  := false,
    iov_schema VARCHAR2 := g_schema,
    iov_table  VARCHAR2 := 'MQ_OUTBOUND_EMPLOYMENT'
  );

END mq_pkg;
/
