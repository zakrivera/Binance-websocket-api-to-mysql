# Binance websocket api to mysql

This script saves all the Binance websocket's data to mysql.

## Maybe this script outdated, it just publicated for testing.

You need to pull submodules using ```git submodule update --init --recursive``` for the first time.


To update submodules, use ```git submodule update --recursive --remote```

## Install:

1. Create a database in mysql and import flash.sql
2. Config the sql settings in Market_logger.py
3. If needed, install pip & unicorn-binance-websocket-api:

```sudo apt-get install python3-pip```

`pip install unicorn-binance-websocket-api --upgrade`

4. Run!
