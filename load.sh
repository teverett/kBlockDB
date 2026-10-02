target/release/kblockdbcli --password changeme create-database demo 2>/dev/null
target/release/kblockdbcli --password changeme --db demo query "SET (material='stone', bearingkg=200, phase='solid', temperature=20.3, melting=false, burning=false, evaporating=false) IN (0,0,0,0) TO (9,9,9,1)"
target/release/kblockdbcli --password changeme --db demo query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
target/release/kblockdbcli --password changeme --db demo query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE version > 0"
target/release/kblockdbcli --password changeme --db demo query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE created> now()"
target/release/kblockdbcli --password changeme --db demo query "SELECT count(*) FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
target/release/kblockdbcli --password changeme --db demo query "SELECT max(bearingkg) FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
target/release/kblockdbcli --password changeme --db demo query "SELECT max(bearingkg),count(*) FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"


