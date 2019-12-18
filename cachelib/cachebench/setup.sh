#! /bin/bash

#### USAGE ###

# setup.sh [TRACEDIR]

# setup symlinks to production data for use in replay tests.
# TRACEDIR specifies a root directory to look in for traces.
# Otherwise, the default location from workload_characterization is used.
# See the workload_characterization README for more info on downloading
# production data.

DATADIR=${1:-"/data/users/$(whoami)/traces/minimal"}
for i in "${DATADIR}"/*csv
do
  ln -s "$i" "$(basename "$i")"
done
