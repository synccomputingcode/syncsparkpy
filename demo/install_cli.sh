# Source this script to install sync-cli in a virtual environment at ../venv


SYNC_HOME="$(dirname "$(dirname "$(readlink -f $0)")")"

function install_sync() {
  local python_candidate
  while read python_candidate; do
    if [[ $("$python_candidate" --version 2>/dev/null) =~ (^|[^[:digit:].])3.10(\.|$) ]]; then
      local SYNC_PYTHON="$python_candidate"
      break
    fi
  done < <(command -v python python3)

  if [[ -z $SYNC_PYTHON ]]; then
    >&2 echo "Python 3.10 is required"
    return 1
  fi

  if [[ -z $VIRTUAL_ENV ]]; then
    if ! { "$SYNC_PYTHON" -m venv "$SYNC_HOME/venv" && source "$SYNC_HOME/venv/bin/activate"; }; then
      >&2 echo "Failed to activate virtual environment"
      return 1
    fi
  fi

  local SYNC_PIP="$(command -v pip3 pip | sed q)"

  if "$SYNC_PIP" install -e $SYNC_HOME; then
    cat <<"EOD"
                                         ___
       _______  ______  _____      _____/ (_)
      / ___/ / / / __ \/ ___/_____/ ___/ / /
     (__  ) /_/ / / / / /__/_____/ /__/ / /
    /____/\__, /_/ /_/\___/      \___/_/_/
         /____/


    Installation successful!

    Try `sync-cli --help` to see available commands and options.

EOD
  else
    >&2 echo "Failed to install Sync CLI"
    return 1
  fi
}

install_sync
