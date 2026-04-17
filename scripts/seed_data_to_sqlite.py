from sqlmodel import Session, select
from server.configs.db import engine
from server.models.user import User
from server.models.lead import Lead
from server.core.security import get_password_hash

def seed_data():
    with Session(engine) as session:
        # 1. Check if the user already exists
        existing_user = session.exec(select(User).where(User.username == "admin@example.com")).first()
        
        if not existing_user:
            print("🌱 Seeding Admin User...")
            admin = User(
                username="admin@example.com",
                hashed_password=get_password_hash("password123"), # Sane dev password
                is_active=True
            )
            session.add(admin)
            session.commit()
            session.refresh(admin)
            
            print("🌱 Seeding Test Leads...")
            leads = [
                Lead(first_name="Alice", last_name="Alpha", email="alice@test.com", owner_id=admin.id),
                Lead(first_name="Bob", last_name="Bravo", email="bob@test.com", owner_id=admin.id),
                Lead(first_name="Charlie", last_name="Delta", email="charlie@test.com", owner_id=admin.id),
            ]
            session.add_all(leads)
            session.commit()
            print("✅ Seeding Complete!")
        else:
            print("⚠️ Data already exists, skipping seed.")

if __name__ == "__main__":
    seed_data()