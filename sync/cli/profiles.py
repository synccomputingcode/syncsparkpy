from pathlib import Path

import click

from sync.cli.util import configure_profile
from sync.config import set_profile, get_profile_name

PROFILES_DIR = Path("~/.sync/profiles").expanduser()


@click.group()
def profiles():
    """Manage profiles for the Sync CLI"""
    pass


@profiles.command()
@click.argument("profile_name", required=True)
@click.option("-f", "--force", is_flag=True, help="Overwrite existing profile")
def create(profile_name, force=False):
    """Create and activate new profile

    :param profile_name: name of the profile to create
    :type profile_name: str
    :param force: overwrite existing profile, defaults to False
    :type force: bool, optional
    """
    profile_dir = PROFILES_DIR / profile_name

    if profile_dir.exists() and not force:
        click.echo(f"Profile '{profile_name}' already exists. Use -f or --force to overwrite.")
        return
    elif profile_dir.exists() and force:
        set_profile(profile_name)
        click.echo(f"Profile '{profile_name}' already exists. Overwriting.")
    else:
        set_profile(profile_name)

    configure_profile(profile=profile_name)


@profiles.command()
@click.argument("profile-name")
def switch(profile_name):
    """switch the active profile

    :param profile_name: name of the profile to set
    :type profile_name: str
    """
    profile_dir = PROFILES_DIR / profile_name
    if not profile_dir.exists():
        click.echo(f"Profile '{profile_name}' does not exist.")
        return
    set_profile(profile_name)
    click.echo(f"Switched to profile '{profile_name}'")


@profiles.command()
def list():
    """List available profiles"""
    avail_profiles = [profile.name for profile in PROFILES_DIR.glob("*") if
                      profile.is_dir() and profile.name != "current"]
    if not avail_profiles:
        click.echo("No profiles found")
    else:
        click.echo("Available profiles:")
        for profile in avail_profiles:
            click.echo(f"  {profile}")


@profiles.command()
def active():
    """Return the current profile."""
    current_profile = get_profile_name()
    if current_profile == "current":
        click.echo("No active profile. Use `sync-cli profiles <switch/create>` to set or create one.")
    else:
        click.echo(f"Current profile: {current_profile}")
