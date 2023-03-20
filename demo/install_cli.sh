# Source this script to install sync-cli in a virtual environment at ../venv


HOME_DIR="$(dirname "$(dirname "$(readlink -f $0)")")"

if ! [[ $(python --version 2>/dev/null) =~ 3.10 ]]; then
  >&2 echo "Python 3.10 is required"
  return 1
fi

if [[ -z $VIRTUAL_ENV ]]; then
  if ! { python -m venv "$HOME_DIR/venv" && source "$HOME_DIR/venv/bin/activate"; }; then
    >&2 echo "Failed to activate virtual environment"
    return 1
  fi
fi

pip install -e $HOME_DIR

cat <<"EOD"
                                     ___
   _______  ______  _____      _____/ (_)
  / ___/ / / / __ \/ ___/_____/ ___/ / /
 (__  ) /_/ / / / / /__/_____/ /__/ / /
/____/\__, /_/ /_/\___/      \___/_/_/
     /____/


sync-cli successfully installed!

Try `sync-cli --help` to see available commands and options.

EOD
