#!/usr/bin/env python3
import os
import sys


def bootstrap_env():
    """
    Synchronizes specific environment variables from the current process environment
    to the .env file. This bridges the gap when running in restricted environments
    (like VS Code Extension Hosts) where python-dotenv might load a stale/partial file
    but the process itself has access to the correct variables.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_file = os.path.join(project_root, ".env")

    # Variables that must be synced if present in the runtime environment
    vars_to_sync = [
        "RETRIEVAL_API_URL",
        "RAG_ADMIN_USERNAME",
        "RAG_ADMIN_PASSWORD",
        "IS_CLOUD_BASED",
        "RUDDERSTACK_PAT",
    ]

    # Read existing .env to check what's already there (optional, but good for idempotency)
    existing_env = {}
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    existing_env[k] = v

    # Determine updates
    updates = {}
    for var in vars_to_sync:
        val = os.environ.get(var)
        if val:
            # If current env has a value, we want to ensure it's in .env
            # We overwrite if it's different or missing, to ensure runtime truth is respected.
            if existing_env.get(var) != val:
                updates[var] = val
                print(f"[Bootstrap] Syncing {var} from environment to .env")

    # Append updates to .env
    if updates:
        with open(env_file, "a") as f:
            if os.path.getsize(env_file) > 0 and not open(
                env_file, "r"
            ).read().endswith("\n"):
                f.write("\n")
            for k, v in updates.items():
                f.write(f"{k}={v}\n")
    else:
        print("[Bootstrap] No environment updates needed.")


if __name__ == "__main__":
    bootstrap_env()
