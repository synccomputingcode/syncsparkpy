from pathlib import Path

import click

from sync.cli.util import configure_profile
from sync.config import set_profile, get_profile, clear_configurations


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
        set_given_profile(profile_name)
        click.echo(f"Profile '{profile_name}' already exists. Overwriting.")
    else:
        clear_configurations(profile_name)

    configure_profile(profile=profile_name)


@profiles.command()
@click.argument("profile-name")
def set(profile_name):
    """Set the active profile

    :param profile_name: name of the profile to set
    :type profile_name: str
    """
    set_given_profile(profile_name)
    click.echo(f"Profile set to '{profile_name}'")


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
def get():
    """Return the current profile."""
    current_profile = get_profile()
    click.echo(f"Current profile: {current_profile}")
    return


def set_given_profile(profile_name):
    """Set the active profile

    :param profile_name: name of the profile to set
    :type profile_name: str
    """
    profile_dir = PROFILES_DIR / profile_name
    if not profile_dir.exists():
        click.echo(f"Profile '{profile_name}' does not exist")
        return

    set_profile(profile_name)
