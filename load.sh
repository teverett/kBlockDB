target/release/kblockdbcli --password changeme query "SET (material='stone', bearing=200, phase='solid', temperature=20.3, burning='no', evaporating='no') IN (0,0,0,0) TO (9,9,9,1)"
target/release/kblockdbcli --password changeme query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
