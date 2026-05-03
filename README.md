# QuickBitLabs
FastAPI + PyArrow + React + TypeScript + Vite

See server/readme.md and frontend/readme.md for detailed information on the backend and frontend respectively.

## Startup
```shell
python "Q:/scripts/boot.py" -v  -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./main.py
```
# Frontend Documentation
1. [@hey-api/openapi-ts](https://heyapi.dev/)
2. [Tailwind CSS](https://tailwindcss.com/)
3. [Vite](https://vite.dev/)
4. [React](https://react.dev/)
5. [ESLint](https://eslint.org/)
6. [TypeScript](https://www.typescriptlang.org/)
7. [TanStack: react-query, react-table, react-form](https://tanstack.com/)
8. [React Router](https://reactrouter.com/)
9. [React Icons](https://react-icons.github.io/react-icons/)
10. [React-querybuilder](https://react-querybuilder.js.org/)
11. [Axios](https://axios-http.com/)
12. [Zod](https://zod.dev/)
13. [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)
14. [shadcn/ui](https://ui.shadcn.com/)
15. [Radix UI](https://www.radix-ui.com/)

# Backend Documentation
1. [Python](https://docs.python.org/3.13/py-modindex.html)
2. [FastAPI](https://fastapi.tiangolo.com/)
3. [Pydantic](https://pydantic.dev/)
4. [Pydantic Settings](https://pydantic.dev/latest/settings/) for .env management and config validation
5. [PyArrow Interchange](https://arrow.apache.org/docs/python/interchange_protocol.html)
6. [Uvicorn](https://www.uvicorn.org/)
7. [Pytest](https://docs.pytest.org/)
8. [pyjwt](https://pyjwt.readthedocs.io/)
9. [argon2-cffi](https://argon2-cffi.readthedocs.io/)
10. [cryptography](https://cryptography.io/)
11. [pydantic-settings](https://pydantic.dev/latest/settings/)
12. [httpx](https://www.python-httpx.org/)
13. [oracledb](https://oracle.github.io/python-oracledb/)
14. [Apache Arrow](https://arrow.apache.org/)
15. [Pandas](https://pandas.pydata.org/)
16. [Polars](https://www.pola.rs/)
17. [Docker](https://www.docker.com/)
18. [Docker Compose](https://docs.docker.com/compose/)
19. [DuckDB Python API Reference:](https://duckdb.org/docs/current/clients/python/overview/)

## [The Twelve Factors](https://www.12factor.net/)
### [I. Codebase](https://www.12factor.net/codebase)
One codebase tracked in revision control, many deploys
### [II. Dependencies](https://www.12factor.net/dependencies)
Explicitly declare and isolate dependencies
### [III. Config](https://www.12factor.net/config)
Store config in the environment
### [IV. Backing services](https://www.12factor.net/backing-services)
Treat backing services as attached resources
### [V. Build, release, run](https://www.12factor.net/build-release-run)
Strictly separate build and run stages
### [VI. Processes](https://www.12factor.net/processes)
Execute the app as one or more stateless processes
### [VII. Port binding](https://www.12factor.net/port-binding)
Export services via port binding
### [VIII. Concurrency](https://www.12factor.net/concurrency)
Scale out via the process model
### [IX. Disposability](https://www.12factor.net/disposability)
Maximize robustness with fast startup and graceful shutdown
### [X. Dev/prod parity](https://www.12factor.net/dev-prod-parity)
Keep development, staging, and production as similar as possible
### [XI. Logs](https://www.12factor.net/logs)
Treat logs as event streams
### [XII. Admin processes](https://www.12factor.net/admin-processes)
Run admin/management tasks as one-off processes

## Removed
1. [NumPy](https://numpy.org/)
2. [SQLAlchemy](https://www.sqlalchemy.org/)
3. [Pandas](https://pandas.pydata.org/)


## For Later
1. [Arrow Flight SQL](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)