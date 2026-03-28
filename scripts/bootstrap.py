import os

def create_pyscript_stack():
    # Root Level Structure
    structure = {
        ".": ["main.py", ".env", "Makefile", "package.json", "pyproject.toml"],
        "client": [
            "main.tsx", "App.tsx", "index.html", "tsconfig.json", "vite.config.ts"
        ],
        "client/api": [],
        "client/components": ["LeadTable.tsx"],
        "client/components/ui": [],
        "client/configs": ["app_constants.ts"],
        "client/context": ["AuthProvider.tsx"],
        "client/layout": ["Navbar.tsx", "DashboardLayout.tsx"],
        "client/models": [],
        "client/pages": ["Login.tsx", "Dashboard.tsx"],
        "client/styles": ["tailwind.css", "index.css"],
        "client/utils": ["cn.ts"],
        "server": ["app.py", "alembic.ini"],
        "server/api": ["leads.py", "auth.py", "users.py"],
        "server/configs": ["db.py", "settings.py"],
        "server/core": ["security.py", "jwt.py"],
        "server/models": ["user.py", "lead.py"],
        "server/utils": ["logger.py", "helpers.py"],
        "server/alembic": [],
        "tests/client": ["App.test.tsx"],
        "tests/server": ["test_leads.py"],
        "database": [],
        "data/mocks": [],
        "data": ["seed_data.py"],
        "dist": []
    }

    print("Building PyScriptStack Architecture...")

    for path, files in structure.items():
        # Create directories
        os.makedirs(path, exist_ok=True)
        # Create empty placeholder files
        for file in files:
            file_path = os.path.join(path, file)
            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    pass
                print(f"Created: {file_path}")

    print("\nDone! Your Hybrid Root is ready.")
    print("Next step: Populating the Makefile and root configs.")

if __name__ == "__main__":
    create_pyscript_stack()