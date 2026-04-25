Q:/quickbitlabs/
├── .claude/
│   ├── .gitignore
│   ├── CLAUDE.md
│   └── settings.local.json
├── .data/
│   ├── backend_antipattern_report.md
│   └── results.csv
├── .gemini/
│   ├── .gitignore
│   ├── GEMINI.md
│   └── plan.md
├── .keys/
│   ├── aes256.key
│   └── jwt.key
├── frontend/
│   ├── assets/
│   │   └── rudow_fam.jpg
│   ├── src/
│   │   ├── api/
│   │   │   ├── openapi/
│   │   │   │   ├── client/
│   │   │   │   │   ├── client.gen.ts
│   │   │   │   │   ├── index.ts
│   │   │   │   │   ├── types.gen.ts
│   │   │   │   │   └── utils.gen.ts
│   │   │   │   ├── core/
│   │   │   │   │   ├── auth.gen.ts
│   │   │   │   │   ├── bodySerializer.gen.ts
│   │   │   │   │   ├── params.gen.ts
│   │   │   │   │   ├── pathSerializer.gen.ts
│   │   │   │   │   ├── queryKeySerializer.gen.ts
│   │   │   │   │   ├── serverSentEvents.gen.ts
│   │   │   │   │   ├── types.gen.ts
│   │   │   │   │   └── utils.gen.ts
│   │   │   │   ├── client.gen.ts
│   │   │   │   ├── index.ts
│   │   │   │   ├── schemas.gen.ts
│   │   │   │   ├── sdk.gen.ts
│   │   │   │   └── types.gen.ts
│   │   │   ├── migrationApi.ts
│   │   │   └── sessionApi.ts
│   │   ├── assets/
│   │   │   ├── react.svg
│   │   │   └── vite.svg
│   │   ├── auth/
│   │   │   ├── AuthContext.tsx
│   │   │   └── authenticateUser.ts
│   │   ├── components/
│   │   │   ├── FileDropzone.tsx
│   │   │   └── Navbar.tsx
│   │   ├── configs/
│   │   │   └── localCache.ts
│   │   ├── context/
│   │   │   ├── BreakpointContext.tsx
│   │   │   ├── DataContext.tsx
│   │   │   ├── QueryClientContext.tsx
│   │   │   └── ThemeContext.tsx
│   │   ├── hooks/
│   │   ├── layouts/
│   │   │   └── Layout.tsx
│   │   ├── models/
│   │   ├── pages/
│   │   │   ├── ContactsPage.tsx
│   │   │   ├── DataMartPage.tsx
│   │   │   ├── HomePage.tsx
│   │   │   ├── ImportPage.tsx
│   │   │   ├── MigrationPage.tsx
│   │   │   └── ProfilePage.tsx
│   │   ├── styles/
│   │   │   ├── ColorTokens.css
│   │   │   ├── fonts.css
│   │   │   ├── index.css
│   │   │   └── styles.css
│   │   ├── templates/
│   │   │   ├── about.html
│   │   │   ├── base.html
│   │   │   ├── globe-loader.html
│   │   │   └── index.html
│   │   ├── utils/
│   │   │   ├── cn.ts
│   │   │   ├── getComponentHeight.ts
│   │   │   └── normalizeBasename.ts
│   │   ├── App.tsx
│   │   └── main.tsx
│   ├── eslint.config.js
│   ├── index.html
│   ├── LICENSE
│   ├── openapi-ts.config.ts
│   ├── package-lock.json
│   ├── package.json
│   ├── README.md
│   ├── tailwind.config.js
│   ├── tsconfig.json
│   ├── tsconfig.node.json
│   ├── vite.config.d.ts
│   └── vite.config.ts
├── scripts/
├── server/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── auth.py
│   │   ├── catalog.py
│   │   ├── column.py
│   │   ├── data.py
│   │   ├── entity.py
│   │   ├── files.py
│   │   ├── info.py
│   │   ├── migration.py
│   │   ├── registry.py
│   │   ├── session.py
│   │   └── users.py
│   ├── configs/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── catalog_registry.py
│   │   ├── DuckDBDialect.py
│   │   ├── federation.py
│   │   ├── jwt.py
│   │   └── security.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── db.py
│   │   ├── ServerDatabase.py
│   │   └── setup_tables.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── ArrowFrame.py
│   │   ├── DataFrame.py
│   │   └── user.py
│   ├── plugins/
│   │   ├── excel/
│   │   │   ├── __init__.py
│   │   │   ├── Excel.py
│   │   │   ├── ExcelEngine.py
│   │   │   ├── ExcelService.py
│   │   │   └── ExcelTypeMap.py
│   │   ├── oracle/
│   │   │   ├── tests/
│   │   │   ├── __init__.py
│   │   │   ├── LICENSE
│   │   │   ├── Oracle.py
│   │   │   ├── OracleArrowFrame.py
│   │   │   ├── OracleClient.py
│   │   │   ├── OracleDialect.py
│   │   │   ├── OracleEngine.py
│   │   │   ├── OracleServices.py
│   │   │   ├── OracleTools.py
│   │   │   └── OracleTypeMap.py
│   │   ├── readers/
│   │   │   ├── __init__.py
│   │   │   ├── CsvEngine.py
│   │   │   ├── FeatherEngine.py
│   │   │   ├── ParquetEngine.py
│   │   │   ├── Reader.py
│   │   │   ├── ReaderEncryption.py
│   │   │   ├── ReaderModels.py
│   │   │   ├── ReaderService.py
│   │   │   └── ReaderTypeMap.py
│   │   ├── sf/
│   │   │   ├── engines/
│   │   │   │   ├── SfAuth.py
│   │   │   │   ├── SfBulk2Engine.py
│   │   │   │   ├── SfClient.py
│   │   │   │   ├── SfRestEngine.py
│   │   │   │   └── SfToolingEngine.py
│   │   │   ├── models/
│   │   │   │   ├── SfDialect.py
│   │   │   │   ├── SfExceptions.py
│   │   │   │   ├── SfModels.py
│   │   │   │   └── SfTypeMap.py
│   │   │   ├── services/
│   │   │   │   ├── SfArrowServices.py
│   │   │   │   ├── SfParquetServices.py
│   │   │   │   └── SfServices.py
│   │   │   ├── tests/
│   │   │   ├── utils/
│   │   │   │   ├── __init__.py
│   │   │   │   └── csv_utils.py
│   │   │   ├── __init__.py
│   │   │   ├── LICENSE
│   │   │   ├── repl.py
│   │   │   └── Salesforce.py
│   │   ├── LICENSE
│   │   ├── PluginModels.py
│   │   ├── PluginProtocol.py
│   │   ├── PluginRegistry.py
│   │   └── PluginResponse.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── CatalogMigration.py
│   │   ├── file_service.py
│   │   ├── session_service.py
│   │   └── sync_systems.py
│   ├── tools/
│   │   ├── rename_stream.py
│   │   └── sync_systems_to_db.py
│   ├── __init__.py
│   ├── ProjectTree.md
│   ├── README.md
│   └── start_app.py
├── tests/
├── .gitignore
├── boot_server.py
├── command book.md
├── FullProjectTree.md
├── LICENSE
├── Plugin Framework Rules.md
├── pyproject.toml
├── quickbitlabs.code-workspace
├── README.md
└── requirements.txt
