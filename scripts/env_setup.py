import os
import getpass
from collections import OrderedDict

ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

# Define environment variable groups with metadata for extensibility
ENV_GROUPS = OrderedDict([
    ("RUDDERSTACK", [
        {"name": "RUDDERSTACK_PAT", "required": True, "secret": True, "help": "Your RudderStack personal access token"},
    ]),
])

def read_env_file(path):
    env = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    return env

def prompt_var(var, current=None, secret=False, help_text=None, required=True):
    prompt = f"{var}"
    if help_text:
        prompt += f" ({help_text})"
    if current:
        prompt += f" [{current}]"
    prompt += ": "
    while True:
        if secret:
            val = getpass.getpass(prompt)
        else:
            val = input(prompt)
        if not val and current:
            return current
        if val:
            return val
        if not required:
            return ""
        print("This field is required.")

def main():
    env = read_env_file(ENV_FILE)
    env_exists = os.path.exists(ENV_FILE)

    # Check if any required variables are missing in the existing .env
    missing_required = []
    for group_name, var_metas in ENV_GROUPS.items():
        for var_meta in var_metas:
            var = var_meta["name"]
            if var_meta.get("required", True) and var not in env:
                missing_required.append(var)

    # Check auth method variables - remove Snowflake auth check
    values = env.copy()

    # Prompt for RudderStack variables
    for var_meta in ENV_GROUPS["RUDDERSTACK"]:
        var = var_meta["name"]
        if not env_exists or var not in env or not env[var]:
            values[var] = prompt_var(
                var,
                current=env.get(var),
                secret=var_meta.get("secret", False),
                help_text=var_meta.get("help"),
                required=var_meta.get("required", True)
            )

    # Write to .env
    print("\nWriting values to .env...")
    with open(ENV_FILE, "w") as f:
        for group_name, var_metas in ENV_GROUPS.items():
            for var_meta in var_metas:
                var = var_meta["name"]
                f.write(f"{var}={values[var]}\n")
    print(".env file created/updated successfully!\n")

if __name__ == "__main__":
    main()