#!/usr/bin/env python3
"""One-time script to encrypt BLASTA API credentials into secrets/BLASTA.ini.

Run from the project root:
    python scripts/encrypt_blasta_creds.py

This creates:
    secrets/BLASTA.ini  — username (plain) + encrypted password

The Fernet key is printed to the terminal, not written to disk. Storing the
key in the same secrets/ directory as the ciphertext it decrypts means
anyone with read access to that directory holds both the lock and the key —
export it as an environment variable instead:

    export IBIS_BLASTA_FERNET_KEY='<the printed key>'

and set it wherever the pipeline actually runs (the container's env, the
platform's secret store, etc). Pass --write-key-file if you need the old
file-based fallback for local development only.
"""
import argparse
import getpass
import os
import stat
from cryptography.fernet import Fernet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--write-key-file', action='store_true',
        help='Also write secrets/BLASTA.key (local-dev fallback only — '
             'co-locates the key with its ciphertext).',
    )
    args = parser.parse_args()

    username = input("BLASTA username: ").strip()
    password = getpass.getpass("BLASTA password: ")

    key = Fernet.generate_key()
    cipher = Fernet(key)
    encrypted_password = cipher.encrypt(password.encode()).decode()

    os.makedirs('secrets', exist_ok=True)
    with open('secrets/BLASTA.ini', 'w') as f:
        f.write(f"Username={username}\n")
        f.write(f"Password={encrypted_password}\n")
    os.chmod('secrets/BLASTA.ini', stat.S_IRUSR | stat.S_IWUSR)  # 0o600
    print("Saved secrets/BLASTA.ini (encrypted password). It is already in .gitignore.")

    print()
    print("Fernet key (set this as an environment variable, do not save it to a file):")
    print(f"  IBIS_BLASTA_FERNET_KEY={key.decode()}")

    if args.write_key_file:
        with open('secrets/BLASTA.key', 'w') as f:
            f.write(key.decode())
        os.chmod('secrets/BLASTA.key', stat.S_IRUSR | stat.S_IWUSR)  # 0o600
        print()
        print("Also wrote secrets/BLASTA.key as a local-dev fallback — "
              "remove it once IBIS_BLASTA_FERNET_KEY is set in your environment.")


if __name__ == '__main__':
    main()
