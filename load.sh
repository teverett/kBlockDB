target/release/kblockdbcli --password changeme query "SET (material='stone', hardness=1.5, luminous='no', transparent='no', flammable='no', rarity='common') IN (0,0,0,0) TO (9,9,9,1)"
target/release/kblockdbcli --password changeme query "SELECT * FROM (0,0,0,0) TO (9,9,9,1) WHERE material = 'stone'"
