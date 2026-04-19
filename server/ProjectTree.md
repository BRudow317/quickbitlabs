Q:/quickbitlabs/server/
├── api/
│   ├── __init__.py
│   ├── auth.py
│   ├── catalog.py
│   ├── column.py
│   ├── data.py
│   ├── entity.py
│   ├── leads.py
│   ├── migration.py
│   ├── session.py
│   └── users.py
├── configs/
│   ├── __init__.py
│   ├── db.py
│   └── settings.py
├── core/
│   ├── __init__.py
│   ├── ArrowFrame.py
│   ├── DataFrame.py
│   ├── federation.py
│   ├── jwt.py
│   └── security.py
├── models/
│   ├── __init__.py
│   ├── AuthModels.py
│   ├── lead.py
│   └── user.py
├── plugins/
│   ├── oracle/
│   │   ├── tests/
│   │   ├── __init__.py
│   │   ├── LICENSE
│   │   ├── Oracle.py
│   │   ├── OracleArrowFrame.py
│   │   ├── OracleClient.py
│   │   ├── OracleDialect.py
│   │   ├── OracleEngine.py
│   │   ├── OracleServices.py
│   │   ├── OracleTools.py
│   │   └── OracleTypeMap.py
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
│   │   │   └── csv_utils.py
│   │   ├── __init__.py
│   │   ├── LICENSE
│   │   ├── repl.py
│   │   └── Salesforce.py
│   ├── LICENSE
│   ├── PluginModels.py
│   ├── PluginProtocol.py
│   ├── PluginRegistry.py
│   └── PluginResponse.py
├── services/
│   ├── __init__.py
│   ├── FullMigration.py
│   ├── new_session.py
│   ├── SfToSfMigration.py
│   └── sync_systems.py
├── __init__.py
├── ProjectTree.md
├── README.md
└── start_server.py
