#!/usr/bin/env python3
"""One-time script to encrypt BLASTA API credentials into secrets/BLASTA.ini.

Run from the project root:
    python scripts/encrypt_blasta_creds.py

This creates:
    secrets/BLASTA.key  — Fernet key (keep secure, never commit)
    secrets/BLASTA.ini  — username (plain) + encrypted password
"""
import getpass
import os
import stat
from cryptography.fernet import Fernet


def main():
    username = input("BLASTA username: ").strip()
    password = getpass.getpass("BLASTA password: ")

    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted_password = cipher.encrypt(password.encode()).decode()

    os.makedirs('secrets', exist_ok=True)
    with open('secrets/BLASTA.key', 'w') as f:
        f.write(key.decode())
    os.chmod('secrets/BLASTA.key', stat.S_IRUSR | stat.S_IWUSR)  # 0o600

    with open('secrets/BLASTA.ini', 'w') as f:
        f.write(f"Username={username}\n")
        f.write(f"Password={encrypted_password}\n")
    os.chmod('secrets/BLASTA.ini', stat.S_IRUSR | stat.S_IWUSR)  # 0o600

    print("Saved secrets/BLASTA.ini and secrets/BLASTA.key")
    print("Keep both files secure. They are already in .gitignore.")


if __name__ == '__main__':
    main()
