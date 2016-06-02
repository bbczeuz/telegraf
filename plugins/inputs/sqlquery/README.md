# SQL Query Input Plugin

The SQL query plugin performs SQL queries on databases

### Configuration:

```
# Sample Config:
[[inputs.sqlquery]]

## Instance name
name = "sql_testtable" # required. Value used as metric name

## DB Driver
driver = "oci8" # required. Options: oci8 (Oracle), postgres

## Server URL
server_url = "user/password@localhost/xe" # required. Connection URL to pass to the DB driver

## Queries to perform
queries = ["SELECT portid,location,portname FROM testtable"] # required. List of queries to be performed

tag_cols = ["LOCATION"] # optional. List of columns that are extracted as tags (remaining columns will be used as fields)

```

Plugin supports multiple instances

### Tags:

- as defined per tag_cols
- Filtering is case sensitive (!) SQL typically isn't

### Example output:

```
./telegraf -config telegraf.conf -test -input-filter sqlquery -test
* Plugin: sqlquery, Collection 1
> sql_testtable,LOCATION=ZUE0000,host=vm0 PORTID="1000",PORTNAME="ge-0-0-0" 1464634409708036891
> sql_testtable,LOCATION=ZUE0000,host=vm0 PORTID="1001",PORTNAME="ge-0-0-1" 1464634409708119078
> sql_testtable,LOCATION=ZUE0001,host=vm0 PORTID="1002",PORTNAME="ge-0-0-2" 1464634409708160194
```
