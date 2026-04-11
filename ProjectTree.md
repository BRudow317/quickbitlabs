Q:/quickbitlabs/server/
├── api/
│   ├── auth.py
│   ├── leads.py
│   └── users.py
├── auth/
├── configs/
│   ├── __init__.py
│   ├── db.py
│   └── settings.py
├── core/
│   ├── jwt.py
│   └── security.py
├── engine/
├── models/
│   ├── AuthModels.py
│   ├── lead.py
│   └── user.py
├── plugins/
│   ├── lambdalith/
│   ├── oracle/
│   │   ├── tests/
│   │   ├── __init__.py
│   │   ├── arrow_array.py
│   │   ├── Oracle.py
│   │   ├── OracleArrowFrame.py
│   │   ├── OracleClient.py
│   │   ├── OracleDialect.py
│   │   ├── OracleEngine.py
│   │   ├── OracleServices.py
│   │   ├── OracleTools.py
│   │   ├── OracleTypeMap.py
│   │   └── README.md
│   ├── postgres/
│   │   ├── postgres_utils/
│   │   │   └── type_converter.py
│   │   ├── services/
│   │   │   ├── query.py
│   │   │   └── table_ops.py
│   │   ├── __init__.py
│   │   ├── Postgres.py
│   │   ├── PostgresEngine.py
│   │   ├── PostgresServices.py
│   │   └── PostgresTypeMap.py
│   ├── readers/
│   │   ├── reader_utils/
│   │   │   ├── csv_utils.py
│   │   │   ├── filter_null_bytes.py
│   │   │   └── list_from_generator.py
│   │   ├── base.py
│   │   ├── Csv.py
│   │   └── plan.md
│   ├── scratch/
│   │   └── README.md
│   ├── sf/
│   │   ├── engines/
│   │   │   ├── SfAuth.py
│   │   │   ├── SfBulk2Engine.py
│   │   │   ├── SfClient.py
│   │   │   ├── SfRestEngine.py
│   │   │   └── SfToolingEngine.py
│   │   ├── models/
│   │   │   ├── SfDialect.py
│   │   │   ├── SfExceptions.py
│   │   │   ├── SfModels.py
│   │   │   └── SfTypeMap.py
│   │   ├── services/
│   │   │   ├── SfArrowServices.py
│   │   │   ├── SfParquetServices.py
│   │   │   └── SfServices.py
│   │   ├── tests/
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── csv_utils.py
│   │   │   ├── date_to_iso8601.py
│   │   │   ├── filter_null_bytes.py
│   │   │   ├── list_from_generator.py
│   │   │   └── soql_utils.py
│   │   ├── __init__.py
│   │   ├── ReadMe Sf Plugin.md
│   │   ├── repl.py
│   │   └── Salesforce.py
│   ├── tests/
│   ├── utils/
│   │   ├── date_to_iso8601.py
│   │   ├── filter_null_bytes.py
│   │   ├── install_package.py
│   │   └── list_from_generator.py
│   ├── PluginModels.py
│   ├── PluginProtocol.py
│   ├── PluginRegistry.py
│   └── PluginResponse.py
├── services/
│   ├── __init__.py
│   └── FullMigration.py
├── tests/
├── utils/
│   ├── __init__.py
│   ├── encrypt.py
│   ├── helpers.py
│   └── logger.py
├── __init__.py
├── ProjectTree.md
├── README.md
└── start_server.py
