target/release/kblockdbcli --password changeme query "SET (material='stone', bearingkg=200, phase='solid', temperature=20.3, melting=false, burning=false, evaporating=false) IN (0,0,0,0) TO (9,9,9,1)"
target/release/kblockdbcli --password changeme query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
