# dba_mongodb
DBA scripts for MongoDB

## Requirements

- The *pymongo* module is required.


## Usage

Show fragmentation:
	```
	python fragmentation.py --host <your host> -s
    Total fragmentation = 337.1GiB
    ```
    ```
	python fragmentation.py --host <your host> -d thedatabase
	thedatabase =  78.0MiB
	```
Compact:
	```
	python fragmentation.py --host <your host> -d thedatabase -A compact
	```