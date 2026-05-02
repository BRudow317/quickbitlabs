# Command Book

## Program Commands
```shell
clear; python "./scripts/boot.py" -v  -l ./.logs --env homelab  --exec ./build_server.py

python "Q:/scripts/boot.py" -v  -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./sync.py

python "Q:/scripts/boot.py" -v  -l ./.logs --env homelab --config Q:/.secrets/.env --exec pytest ./tests/test_startup.py

python Q:/scripts/encrypt.py aes256 ./quickbitlabs/.keys
```

## DB Administration Commands
```shell
sqlplus / as sysdba
sqlplus system/password@//localhost:1521/oracledb
lsnrctl status
lsnrctl start
```