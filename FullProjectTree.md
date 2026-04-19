Q:/quickbitlabs/
├── .claude/
│   └── CLAUDE.md
├── .gemini/
│   ├── .gitignore
│   ├── GEMINI.md
│   ├── plan.md
│   └── project_context.md
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
│   │   │   ├── pluginsApi.ts
│   │   │   └── sessionApi.ts
│   │   ├── assets/
│   │   │   ├── react.svg
│   │   │   └── vite.svg
│   │   ├── auth/
│   │   │   ├── AuthContext.tsx
│   │   │   └── authenticateUser.ts
│   │   ├── components/
│   │   │   ├── CreateLeadDialog.tsx
│   │   │   ├── DynamicField.tsx
│   │   │   ├── DynamicForm.tsx
│   │   │   ├── DynamicValidation.tsx
│   │   │   ├── FieldInfo.tsx
│   │   │   ├── Grid.tsx
│   │   │   ├── LeadsTable.tsx
│   │   │   └── TanstackForm.tsx
│   │   ├── configs/
│   │   │   └── localCache.ts
│   │   ├── context/
│   │   │   ├── BreakpointContext.tsx
│   │   │   ├── DataContext.tsx
│   │   │   ├── QueryClientContext.tsx
│   │   │   └── ThemeContext.tsx
│   │   ├── hooks/
│   │   ├── layouts/
│   │   │   ├── AppLayout.tsx
│   │   │   └── Layout.tsx
│   │   ├── models/
│   │   ├── pages/
│   │   │   ├── DataMartPage.tsx
│   │   │   ├── HomePage.tsx
│   │   │   ├── MigrationPage.tsx
│   │   │   ├── QueryPage.tsx
│   │   │   └── TablePage.tsx
│   │   ├── styles/
│   │   │   ├── ColorTokens.css
│   │   │   ├── fonts.css
│   │   │   ├── index.css
│   │   │   └── styles.css
│   │   ├── templates/
│   │   │   ├── about.html
│   │   │   ├── base.html
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
│   │   ├── leads.py
│   │   ├── migration.py
│   │   ├── session.py
│   │   └── users.py
│   ├── configs/
│   │   ├── __init__.py
│   │   ├── db.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── ArrowFrame.py
│   │   ├── DataFrame.py
│   │   ├── federation.py
│   │   ├── jwt.py
│   │   └── security.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── AuthModels.py
│   │   ├── lead.py
│   │   └── user.py
│   ├── plugins/
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
│   │   ├── FullMigration.py
│   │   ├── new_session.py
│   │   ├── SfToSfMigration.py
│   │   └── sync_systems.py
│   ├── __init__.py
│   ├── ProjectTree.md
│   ├── README.md
│   └── start_server.py
├── .gitignore
├── FullProjectTree.md
├── LICENSE
├── main.py
├── pyproject.toml
├── quickbitlabs.code-workspace
├── README.md
└── requirements.txt
