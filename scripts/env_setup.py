import os
import getpass
from collections import OrderedDict

ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")

# Define environment variable groups with metadata for extensibility
ENV_GROUPS = OrderedDict([
    ("RUDDERSTACK", [
        {"name": "RUDDERSTACK_PAT", "required": True, "secret": True, "help": "Your RudderStack personal access token"},
    ]),
    ("ENVIRONMENT", [
        {"name": "IS_CLOUD_BASED", "required": False, "secret": False, "help": "Set to 'true' if running in kubernetes pod environment (skips virtual environment creation)", "default": "false"},
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

    # Start with all existing values to preserve them
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

    # Prompt for Environment variables
    for var_meta in ENV_GROUPS["ENVIRONMENT"]:
        var = var_meta["name"]
        # Set default value if not present
        if var not in values:
            default_value = var_meta.get("default", "")
            if not env_exists or var not in env:
                values[var] = prompt_var(
                    var,
                    current=default_value,
                    secret=var_meta.get("secret", False),
                    help_text=var_meta.get("help"),
                    required=var_meta.get("required", True)
                )
            else:
                values[var] = env.get(var, default_value)

    # Write to .env - PRESERVE ALL EXISTING VARIABLES
    print("\nWriting values to .env...")
    with open(ENV_FILE, "w") as f:
        # First write the managed variables
        for group_name, var_metas in ENV_GROUPS.items():
            for var_meta in var_metas:
                var = var_meta["name"]
                f.write(f"{var}={values[var]}\n")
        
        # Then write any other variables that were in the original file
        # This preserves Bedrock config and any other custom variables
        managed_vars = {var_meta["name"] for group_name, var_metas in ENV_GROUPS.items() for var_meta in var_metas}
        for key, value in env.items():
            if key not in managed_vars:
                f.write(f"{key}={value}\n")
    print(".env file created/updated successfully!\n")

if __name__ == "__main__":
    main()