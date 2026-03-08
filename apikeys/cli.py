#!/usr/bin/env python3
"""CLI for the EconScraper API Key Manager.

Usage:
    python -m apikeys.cli status              # Show which keys are set vs missing
    python -m apikeys.cli plan                # Show prioritized setup plan
    python -m apikeys.cli add fred <key>      # Store a key in the vault
    python -m apikeys.cli validate            # Test all stored keys
    python -m apikeys.cli validate fred       # Test a specific key
    python -m apikeys.cli inject              # Write vault keys to .env
    python -m apikeys.cli instructions fred   # Show signup instructions
    python -m apikeys.cli open fred           # Open signup URL in browser
    python -m apikeys.cli export              # Export keys as .env format
    python -m apikeys.cli quickstart          # Show instant-signup APIs
    python -m apikeys.cli no-key              # List sources that need no key
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def cmd_status(args):
    """Show current key configuration status."""
    from apikeys.injector import KeyInjector
    from apikeys.vault import KeyVault

    injector = KeyInjector()
    vault = KeyVault()

    configured = injector.get_configured_keys()
    missing = injector.get_missing_keys()
    vault_keys = vault.list_keys()

    print("\n=== EconScraper API Key Status ===\n")

    if configured:
        print(f"  CONFIGURED ({len(configured)}):")
        for k in configured:
            validity = ""
            if k["api_id"] in vault_keys:
                v = vault_keys[k["api_id"]]
                if v["is_valid"] is True:
                    validity = " [valid]"
                elif v["is_valid"] is False:
                    validity = " [INVALID]"
            print(f"    {k['api_name']:<45} {k['env_var']:<30} {k['key_preview']}{validity}")

    if missing:
        print(f"\n  MISSING ({len(missing)}):")
        # Group by priority
        for priority in ("high", "medium", "low"):
            group = [m for m in missing if m["priority"] == priority]
            if group:
                print(f"    [{priority.upper()} PRIORITY]")
                for m in group:
                    method = "instant signup" if m["signup_method"] == "instant" else "manual setup"
                    print(f"      {m['api_name']:<43} {m['env_var']:<28} ({method})")

    print(f"\n  Vault: {len(vault_keys)} keys stored")
    print()


def cmd_plan(args):
    """Show prioritized setup plan."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    plan = prov.get_setup_plan()
    estimate = prov.estimate_setup_time()

    print(f"\n=== API Key Setup Plan ===")
    print(f"Total APIs: {estimate['total_apis']} | "
          f"Instant: {estimate['instant_signup']} | "
          f"Manual: {estimate['manual_signup']} | "
          f"Est. time: {estimate['estimated_time']}\n")

    current_priority = None
    for item in plan:
        if item["priority"] != current_priority:
            current_priority = item["priority"]
            print(f"  [{current_priority.upper()} PRIORITY]")

        method_icon = ">" if item["signup_method"] == "instant" else "*"
        print(f"    {method_icon} {item['name']:<45} {item['free_tier']:<40} ~{item['time_estimate']}")
        print(f"      {item['signup_url']}")

    print(f"\n  > = instant signup  * = manual setup\n")


def cmd_add(args):
    """Add a key to the vault."""
    from apikeys.catalog import CATALOG
    from apikeys.vault import KeyVault

    api_id = args.api_id
    key = args.key

    if api_id not in CATALOG:
        print(f"Unknown API: {api_id}")
        print(f"Available: {', '.join(sorted(CATALOG.keys()))}")
        return

    vault = KeyVault()
    info = CATALOG[api_id]
    env_var = info.get("env_var", "")

    vault.store(api_id, key, env_var=env_var)
    print(f"Stored key for {info['name']} ({env_var})")

    # Auto-validate
    if args.validate:
        from apikeys.validator import KeyValidator
        validator = KeyValidator()
        result = validator.validate(api_id, key)
        vault.update_validation(api_id, result["is_valid"])
        status = "VALID" if result["is_valid"] else "INVALID"
        print(f"Validation: {status} — {result['message']}")

    # Auto-inject
    if args.inject:
        from apikeys.injector import KeyInjector
        injector = KeyInjector()
        injector.inject_all({env_var: key})
        print(f"Injected {env_var} into .env")


def cmd_validate(args):
    """Validate API keys."""
    from apikeys.validator import KeyValidator
    from apikeys.vault import KeyVault

    validator = KeyValidator()
    vault = KeyVault()

    if args.api_id:
        # Validate specific key
        key = vault.get(args.api_id)
        if not key:
            print(f"No key found for {args.api_id} in vault")
            return
        result = validator.validate(args.api_id, key)
        vault.update_validation(args.api_id, result["is_valid"])
        _print_validation(result)
    else:
        # Validate all vault keys
        keys = vault.list_keys()
        if not keys:
            print("No keys in vault. Use 'add' to store keys first.")
            return

        print(f"\nValidating {len(keys)} keys...\n")
        for api_id in keys:
            key = vault.get(api_id)
            result = validator.validate(api_id, key)
            vault.update_validation(api_id, result["is_valid"])
            _print_validation(result)


def _print_validation(result: dict):
    if result["is_valid"] is True:
        icon = "[OK]"
    elif result["is_valid"] is False:
        icon = "[FAIL]"
    else:
        icon = "[?]"

    rl = ""
    if result.get("rate_limit"):
        remaining = result["rate_limit"].get("remaining", "?")
        limit = result["rate_limit"].get("limit", "?")
        rl = f" (rate: {remaining}/{limit})"

    print(f"  {icon} {result['api_id']:<20} {result['message']}{rl}")


def cmd_inject(args):
    """Inject vault keys into .env."""
    from apikeys.injector import KeyInjector

    injector = KeyInjector()
    result = injector.sync_from_vault()
    print(f"Injected {result.get('injected', 0)} keys into .env")


def cmd_instructions(args):
    """Show signup instructions."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    info = prov.get_instructions(args.api_id)

    if not info:
        print(f"Unknown API: {args.api_id}")
        return

    print(f"\n=== {info['name']} ===")
    print(f"Provider: {info['provider']}")
    print(f"Free tier: {info['free_tier']}")
    print(f"URL: {info['signup_url']}")
    print(f"\nSteps:")
    for i, step in enumerate(info["steps"], 1):
        print(f"  {i}. {step}")

    if info.get("env_vars"):
        print(f"\nEnvironment variables to set:")
        for var in info["env_vars"]:
            print(f"  {var}")
    elif info.get("env_var"):
        print(f"\nEnvironment variable: {info['env_var']}")

    if info.get("tip"):
        print(f"\nTip: {info['tip']}")

    print()


def cmd_open(args):
    """Open signup URL in browser."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    if prov.open_signup(args.api_id):
        info = prov.get_instructions(args.api_id)
        print(f"Opened {info['name']} signup page in browser")
    else:
        print(f"Unknown API: {args.api_id}")


def cmd_export(args):
    """Export vault keys as .env format."""
    from apikeys.vault import KeyVault

    vault = KeyVault()
    print(vault.export_env())


def cmd_quickstart(args):
    """Show instant-signup APIs."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    apis = prov.get_quick_start_apis()

    print("\n=== Quick Start — Instant Signup APIs ===\n")
    for api in apis:
        print(f"  {api['name']}")
        print(f"    Free tier: {api['free_tier']}")
        print(f"    Signup: {api['signup_url']}")
        print(f"    Env var: {api['env_var']}")
        print()


def cmd_nokey(args):
    """List sources that work without API keys."""
    from apikeys.provisioner import KeyProvisioner

    prov = KeyProvisioner()
    sources = prov.get_no_key_apis()

    print("\n=== Sources That Work Without API Keys ===\n")
    for source in sources:
        print(f"  - {source}")
    print()


def main():
    parser = argparse.ArgumentParser(
        prog="apikeys",
        description="EconScraper API Key Manager",
    )
    subparsers = parser.add_subparsers(dest="command")

    # status
    subparsers.add_parser("status", help="Show key configuration status")

    # plan
    subparsers.add_parser("plan", help="Show prioritized setup plan")

    # add
    add_parser = subparsers.add_parser("add", help="Store a key in the vault")
    add_parser.add_argument("api_id", help="API identifier (e.g., fred, finnhub)")
    add_parser.add_argument("key", help="The API key value")
    add_parser.add_argument("--validate", "-v", action="store_true", help="Validate after storing")
    add_parser.add_argument("--inject", "-i", action="store_true", help="Inject into .env after storing")

    # validate
    validate_parser = subparsers.add_parser("validate", help="Validate API keys")
    validate_parser.add_argument("api_id", nargs="?", help="Specific API to validate (all if omitted)")

    # inject
    subparsers.add_parser("inject", help="Write vault keys to .env")

    # instructions
    inst_parser = subparsers.add_parser("instructions", help="Show signup instructions")
    inst_parser.add_argument("api_id", help="API identifier")

    # open
    open_parser = subparsers.add_parser("open", help="Open signup URL in browser")
    open_parser.add_argument("api_id", help="API identifier")

    # export
    subparsers.add_parser("export", help="Export keys as .env format")

    # quickstart
    subparsers.add_parser("quickstart", help="Show instant-signup APIs")

    # no-key
    subparsers.add_parser("no-key", help="List sources needing no key")

    args = parser.parse_args()

    commands = {
        "status": cmd_status,
        "plan": cmd_plan,
        "add": cmd_add,
        "validate": cmd_validate,
        "inject": cmd_inject,
        "instructions": cmd_instructions,
        "open": cmd_open,
        "export": cmd_export,
        "quickstart": cmd_quickstart,
        "no-key": cmd_nokey,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
