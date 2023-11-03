import click
from sync.config import init
from sync.cli.util import configure_profile
import json
from pathlib import Path

# Path to the profiles directory
PROFILES_DIR = Path("~/.sync/profiles").expanduser()


@click.group()
def profiles():
    """Manage profiles for the Sync CLI"""
    pass


@profiles.command()
@click.argument("profile_name", required=True)
@click.option("-f", "--force", is_flag=True, help="Overwrite existing profile")
def create(profile_name, force=False):
    """Create and activate new profile"""
    if (PROFILES_DIR / profile_name).exists() and not force:
        click.echo(f"Profile '{profile_name}' already exists. Use --force to overwrite.")
        return
    else:
        configure_profile(profile_name)
        set_profile(profile_name)


@profiles.command()
@click.argument("profile-name")
def set(profile_name):
    """Set the active profile."""
    set_profile(profile_name)


@profiles.command()
def list():
    """List available profiles"""
    avail_profiles = [profile.name for profile in PROFILES_DIR.glob("*") if profile.is_dir()]
    if not avail_profiles:
        click.echo("No profiles found.")
    else:
        click.echo("Available profiles:")
        for profile in avail_profiles:
            click.echo(f"  {profile}")


@profiles.command()
def current():
    """Return the current profile."""
    current_profile_dir = PROFILES_DIR / "current_profile"
    if not current_profile_dir.exists():
        click.echo("No profile set.")
        return None
    else:
        click.echo(f"Current profile: {current_profile_dir.resolve().name}")
        return


def set_profile(profile_name):
    """Set the active profile."""
    profile_dir = PROFILES_DIR / profile_name
    if not profile_dir.exists():
        click.echo(f"Profile '{profile_name}' does not exist.")
        return

    current_profile_dir = PROFILES_DIR / "current_profile"
    current_profile_dir.symlink_to(profile_dir, target_is_directory=True)

    click.echo(f"Profile set to '{profile_name}'.")
