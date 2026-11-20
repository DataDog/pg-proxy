#!/bin/sh
# Initialize a disposable cluster on first start and run the server, or run any
# other command (such as pg_regress) unchanged.
set -eu

if [ "${1:-}" != "postgres" ]; then
	exec "$@"
fi
shift

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	# C locale and trust authentication, as for a regression-test server.
	initdb --no-locale --encoding=UTF8 --username=postgres --auth=trust >/dev/null
	echo "listen_addresses = '*'" >>"$PGDATA/postgresql.conf"
	echo "host all all all trust" >>"$PGDATA/pg_hba.conf"
fi
exec postgres "$@"
