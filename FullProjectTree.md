Q:/quickbitlabs/
├── .claude/
│   ├── .gitignore
│   ├── CLAUDE.md
│   ├── settings.json
│   └── settings.local.json
├── .data/
│   ├── 710 Pioneer Woods Taxes and Insurance.md
│   ├── backend_antipattern_report.md
│   ├── Malformed Case.csv
│   ├── MortgageHistory.csv
│   ├── MortgageHistory.fixed.csv
│   ├── MortgageHistory.html
│   ├── Newrez Insurance Info 710 Pioneer Woods Dr 2026-4-26.csv
│   ├── Newrez Tax Info 710 Pioneer Woods Dr 2026-4-26.csv
│   ├── Opportunity.csv
│   └── results.csv
├── .gemini/
│   ├── .gitignore
│   ├── GEMINI.md
│   └── plan.md
├── .keys/
│   ├── aes256.key
│   └── jwt.key
├── frontend/
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
│   │   │   ├── rudow_fam.jpg
│   │   │   └── vite.svg
│   │   ├── auth/
│   │   │   ├── AuthContext.tsx
│   │   │   └── authenticateUser.ts
│   │   ├── components/
│   │   │   ├── globe/
│   │   │   │   ├── visual-globe.css
│   │   │   │   └── VisualGlobe.tsx
│   │   │   ├── radix/
│   │   │   │   ├── AlertToaster.tsx
│   │   │   │   ├── DataTable.tsx
│   │   │   │   ├── EntityBrowser.tsx
│   │   │   │   ├── FileDropzone.tsx
│   │   │   │   ├── FileUploader.tsx
│   │   │   │   ├── Navbar.tsx
│   │   │   │   ├── NavSelector.tsx
│   │   │   │   ├── QueryBuilder.tsx
│   │   │   │   └── RegistryList.tsx
│   │   │   ├── ui/
│   │   │   │   ├── avatar.tsx
│   │   │   │   ├── badge.tsx
│   │   │   │   ├── breadcrumb.tsx
│   │   │   │   ├── button.tsx
│   │   │   │   ├── calendar.tsx
│   │   │   │   ├── card.tsx
│   │   │   │   ├── chart.tsx
│   │   │   │   ├── checkbox.tsx
│   │   │   │   ├── collapsible.tsx
│   │   │   │   ├── command.tsx
│   │   │   │   ├── dialog.tsx
│   │   │   │   ├── empty.tsx
│   │   │   │   ├── field.tsx
│   │   │   │   ├── input-group.tsx
│   │   │   │   ├── input.tsx
│   │   │   │   ├── item.tsx
│   │   │   │   ├── label.tsx
│   │   │   │   ├── menubar.tsx
│   │   │   │   ├── navigation-menu.tsx
│   │   │   │   ├── pagination.tsx
│   │   │   │   ├── popover.tsx
│   │   │   │   ├── progress.tsx
│   │   │   │   ├── scroll-area.tsx
│   │   │   │   ├── select.tsx
│   │   │   │   ├── separator.tsx
│   │   │   │   ├── sheet.tsx
│   │   │   │   ├── skeleton.tsx
│   │   │   │   ├── slider.tsx
│   │   │   │   ├── sonner.tsx
│   │   │   │   ├── spinner.tsx
│   │   │   │   ├── switch.tsx
│   │   │   │   ├── table.tsx
│   │   │   │   ├── tabs.tsx
│   │   │   │   ├── textarea.tsx
│   │   │   │   ├── toggle-group.tsx
│   │   │   │   ├── toggle.tsx
│   │   │   │   └── tooltip.tsx
│   │   │   ├── ApiErrorInterceptor.tsx
│   │   │   ├── CrudDataTable.tsx
│   │   │   ├── EntityTreeNav.tsx
│   │   │   ├── FilterBuilder.tsx
│   │   │   ├── GroupedEntityDropdown.tsx
│   │   │   ├── JoinBuilder.tsx
│   │   │   ├── RQBQueryBuilder.tsx
│   │   │   ├── ShadcnDataTable.tsx
│   │   │   ├── ShadcnEntityBrowser.tsx
│   │   │   ├── ShadcnFileUploader.tsx
│   │   │   ├── ShadcnQueryBuilder.tsx
│   │   │   ├── ShadcnRegistryList.tsx
│   │   │   ├── SortBuilder.tsx
│   │   │   └── SourceEntitySelector.tsx
│   │   ├── configs/
│   │   │   └── localCache.ts
│   │   ├── context/
│   │   │   ├── BreakpointContext.tsx
│   │   │   ├── DataContext.tsx
│   │   │   ├── QueryClientContext.tsx
│   │   │   ├── ThemeContext.tsx
│   │   │   └── ToastContext.tsx
│   │   ├── hooks/
│   │   ├── layouts/
│   │   │   ├── LargeLayout.tsx
│   │   │   ├── Layout.tsx
│   │   │   ├── MediumLayout.tsx
│   │   │   └── SmallLayout.tsx
│   │   ├── lib/
│   │   │   └── utils.ts
│   │   ├── pages/
│   │   │   ├── AdminPage.tsx
│   │   │   ├── ContactsPage.tsx
│   │   │   ├── HomePage.tsx
│   │   │   ├── MigrationPage.tsx
│   │   │   ├── ProfilePage.tsx
│   │   │   ├── PrototypePage.tsx
│   │   │   ├── PrototypeShadcnPage.tsx
│   │   │   └── QueryBuilderPage.tsx
│   │   ├── sections/
│   │   │   ├── ActiveSessionSection.tsx
│   │   │   ├── DataMartSection.tsx
│   │   │   ├── DataPreviewSection.tsx
│   │   │   ├── ImportSection.tsx
│   │   │   ├── PrototypeSection.tsx
│   │   │   ├── ShadcnDataMartSection.tsx
│   │   │   ├── ShadcnImportSection.tsx
│   │   │   └── ShadcnThemeSection.tsx
│   │   ├── styles/
│   │   │   ├── ColorTokens.css
│   │   │   ├── global.css
│   │   │   └── index.css
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
│   │   ├── main.tsx
│   │   └── QBL Frontend Rules.md
│   ├── components.json
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
│   │   ├── DuckDBDialect.py
│   │   ├── federation.py
│   │   ├── jwt.py
│   │   └── security.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── db.py
│   │   ├── ServerDatabase.py
│   │   └── sql_tools.py
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
│   │   ├── catalog_registry.py
│   │   ├── CatalogMigration.py
│   │   ├── file_service.py
│   │   ├── session_service.py
│   │   └── sync_systems.py
│   ├── tools/
│   │   ├── create_user.py
│   │   ├── rename_stream.py
│   │   ├── sync_schemas.py
│   │   └── sync_systems_to_db.py
│   ├── uploads/
│   │   └── admin/
│   │       ├── MortgageHistory.fixed__MortgageHistory.fixed.parquet
│   │       └── Opportunity__Opportunity.parquet
│   ├── __init__.py
│   ├── app.py
│   └── README.md
├── tests/
├── .gitignore
├── command book.md
├── docker-compose.yml
├── FullProjectTree.md
├── LICENSE
├── main.py
├── Plugin Framework Rules.md
├── pyproject.toml
├── quickbitlabs.code-workspace
├── README.md
└── requirements.txt
