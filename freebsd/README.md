# FreeBSD rc.d script

`kblockdbserver` is a FreeBSD `rc.d` script for managing `kblockdbserver` as
a system service (start/stop/restart/status via `service kblockdbserver ...`).

kblockdbserver doesn't daemonize itself, so the script runs it under
`daemon(8)`, which backgrounds it, writes its pidfile, and redirects its
stdout/stderr to a log file.

## Install

```sh
install -m 555 freebsd/kblockdbserver /usr/local/etc/rc.d/kblockdbserver
```

Build the release binary and put it (and your real config) where the
service expects them, e.g.:

```sh
install -d /usr/local/kblockdb
install -m 555 target/release/kblockdbserver /usr/local/bin/kblockdbserver
install -m 640 kblockdbserver.toml /usr/local/kblockdb/kblockdbserver.toml
pw useradd kblockdb -d /nonexistent -s /usr/sbin/nologin
chown -R kblockdb:kblockdb /usr/local/kblockdb
```

## Configure

In `/etc/rc.conf`:

```sh
kblockdbserver_enable="YES"
kblockdbserver_dir="/usr/local/kblockdb"       # also the server's cwd
# kblockdbserver_bin="/usr/local/bin/kblockdbserver"
# kblockdbserver_config="${kblockdbserver_dir}/kblockdbserver.toml"
# kblockdbserver_user="kblockdb"
# kblockdbserver_group="kblockdb"
# kblockdbserver_logfile="/var/log/kblockdbserver.log"
# kblockdbserver_flags=""
```

`kblockdbserver_dir` matters: `kblockdbserver.toml`'s `data_dir` and the
binary's default `--config ./kblockdbserver.toml` are both resolved
relative to the process's working directory, so the script `cd`s there
before starting the daemon.

## Use

```sh
service kblockdbserver start
service kblockdbserver stop
service kblockdbserver restart
service kblockdbserver status
```

## Test

`test_kblockdbserver_rc.sh` is a syntax/structure smoke test for the rc.d
script itself (it doesn't require FreeBSD or a running server):

```sh
./freebsd/test_kblockdbserver_rc.sh
```
