"""
WM Land Screener — User Management Script
==========================================
Run this to add, update, or remove users from credentials.yaml.
Passwords are hashed with bcrypt before being stored.

Usage:
    python manage_users.py
"""
import getpass
import sys
from pathlib import Path

import yaml
from yaml.loader import SafeLoader

try:
    import bcrypt
except ImportError:
    print("Install dependencies first:  pip install -r requirements.txt")
    sys.exit(1)

CREDENTIALS_FILE = Path(__file__).parent / "credentials.yaml"


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def load_config() -> dict:
    if not CREDENTIALS_FILE.exists():
        print(f"credentials.yaml not found at {CREDENTIALS_FILE}")
        sys.exit(1)
    with open(CREDENTIALS_FILE) as f:
        return yaml.load(f, Loader=SafeLoader)


def save_config(config: dict):
    with open(CREDENTIALS_FILE, "w") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    print(f"Saved to {CREDENTIALS_FILE}")


def add_or_update_user(config: dict):
    users = config["credentials"]["usernames"]

    username = input("Username (no spaces): ").strip().lower()
    if not username:
        print("Username cannot be empty.")
        return

    existing = username in users
    if existing:
        print(f"User '{username}' already exists — updating.")

    first_name  = input("First name: ").strip()
    last_name   = input("Last name: ").strip()
    email       = input("Email (optional): ").strip()
    role        = input("Role [user/admin] (default: user): ").strip().lower() or "user"
    if role not in ("user", "admin"):
        print("Role must be 'user' or 'admin'. Defaulting to 'user'.")
        role = "user"

    password = getpass.getpass("Password: ")
    confirm  = getpass.getpass("Confirm password: ")
    if password != confirm:
        print("Passwords do not match. No changes made.")
        return
    if len(password) < 6:
        print("Password must be at least 6 characters.")
        return

    users[username] = {
        "first_name": first_name,
        "last_name":  last_name,
        "email":      email,
        "password":   hash_password(password),
        "role":       role,
    }
    action = "Updated" if existing else "Added"
    print(f"\n{action} user '{username}' ({role}).")


def remove_user(config: dict):
    users = config["credentials"]["usernames"]
    if not users:
        print("No users exist yet.")
        return

    print("\nExisting users:", ", ".join(users.keys()))
    username = input("Username to remove: ").strip().lower()
    if username not in users:
        print(f"User '{username}' not found.")
        return
    confirm = input(f"Remove '{username}'? [y/N]: ").strip().lower()
    if confirm == "y":
        del users[username]
        print(f"Removed user '{username}'.")


def list_users(config: dict):
    users = config["credentials"]["usernames"]
    if not users:
        print("No users.")
        return
    print(f"\n{'Username':<20} {'Name':<25} {'Role':<10} Email")
    print("-" * 70)
    for uname, data in users.items():
        name = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
        role  = data.get("role", "user")
        email = data.get("email", "")
        has_pw = "✓" if data.get("password") else "✗ (no password set)"
        print(f"{uname:<20} {name:<25} {role:<10} {email}  pw:{has_pw}")


def main():
    print("=" * 50)
    print("  WM Land Screener — User Management")
    print("=" * 50)

    config = load_config()

    print("\nWhat would you like to do?")
    print("  1. Add / update a user")
    print("  2. Remove a user")
    print("  3. List users")
    print("  4. Quit")
    choice = input("\nChoice [1-4]: ").strip()

    if choice == "1":
        add_or_update_user(config)
        save_config(config)
    elif choice == "2":
        remove_user(config)
        save_config(config)
    elif choice == "3":
        list_users(config)
    elif choice == "4":
        print("Bye.")
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    main()
