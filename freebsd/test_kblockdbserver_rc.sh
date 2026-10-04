#!/bin/sh
# Smoke test for freebsd/kblockdbserver: checks POSIX sh syntax and the
# presence of the rc.subr conventions rc(8) relies on to manage the service
# (PROVIDE/REQUIRE/KEYWORD header, rcvar, load_rc_config, run_rc_command).
set -eu

cd "$(dirname "$0")"
script="kblockdbserver"

fail() {
	echo "FAIL: $1" >&2
	exit 1
}

sh -n "$script" || fail "script has a syntax error"

for marker in \
	'# PROVIDE: kblockdbserver' \
	'# REQUIRE:' \
	'# KEYWORD: shutdown' \
	'. /etc/rc.subr' \
	'rcvar=' \
	'load_rc_config' \
	'run_rc_command "\$1"'
do
	grep -q "$marker" "$script" || fail "missing expected line matching: $marker"
done

echo "PASS: $script looks like a well-formed rc.d script"
