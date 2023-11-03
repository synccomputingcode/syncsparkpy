import click
from sync.config import set_profile, get_profile, clear_configurations
from sync.cli.util import configure_profile
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
    """Create and activate new profile

    :param profile_name: name of the profile to create
    :type profile_name: str
    :param force: overwrite existing profile, defaults to False
    :type force: bool, optional
    """
    if (PROFILES_DIR / profile_name).exists() and not force:
        click.echo(f"Profile '{profile_name}' already exists. Use -f or --force to overwrite.")
        return
    elif (PROFILES_DIR / profile_name).exists() and force:
        set_given_profile(profile_name)
        click.echo(f"Profile '{profile_name}' already exists. Overwriting.")
        configure_profile(profile_name)
    else:
        # clear existing profile config for prompts
        clear_configurations()
        configure_profile(profile=profile_name)


@profiles.command()
@click.argument("profile-name")
def set(profile_name):
    """Set the active profile

    :param profile_name: name of the profile to set
    :type profile_name: str
    """
    set_given_profile(profile_name)
    click.echo(f"Profile set to '{profile_name}'.")


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
        click.echo(f"Profile '{profile_name}' does not exist.")
        return

    set_profile(profile_name)
