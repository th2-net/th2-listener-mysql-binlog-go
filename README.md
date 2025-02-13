# th2-read-mysql-binlog-go

Read mysql binlog component connects to mysql data base as `Replication Slave` to read binlog in realtime and send information about `INSERT`, `UPDATE`, `DELETE` operation via RabbitMQ in th2 raw message format. Each raw message has JSON format

## mysql requirements

### user requirements

User must have the grants
* `replication slave` - access for reading binlog
* select - access for selecting data from schema.tables to be observed

Create user SQL script:
```sql
CREATE USER 'th2'@'%' IDENTIFIED BY 'th2';
GRANT REPLICATION SLAVE ON *.* TO 'th2'@'%';
GRANT SELECT ON <target db>.* TO 'th2'@'%';
FLUSH PRIVILEGES;
```

### server requirements

Binlog must be enabled on the mysql server. `binlog_format` option must have `ROW` value

```
[mysqld]
server_id		           = 1
log_bin			           = /var/log/mysql/mysql-bin.log
binlog_expire_logs_seconds = 864000
max_binlog_size            = 100M
binlog_format              = ROW #Very important if you want to receive write, update and delete row events
binlog_row_metadata        = FULL
binlog_row_image           = FULL
```

reference:
* https://github.com/julien-duponchelle/python-mysql-replication?tab=readme-ov-file#mysql-server-settings
* https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html

queries for current value check:
```sql
show variables like 'server_id';
show variables like 'log_bin';
show variables like 'binlog_expire_logs_seconds';
show variables like 'max_binlog_size';
show variables like 'binlog_format';
show variables like 'binlog_row_metadata';
show variables like 'binlog_row_image';
```

## raw message format

Read-mysql-binlog produces th2 raw message where each message has JSON body format and binlog position in properties

### th2 message properties

* `name` (example: `binlog.000001`) - binlog file name
* `pos` (example: `6787`) - position in binlog file. This value is growing in a binfile, each record in a binfile has unique value.
* `seq` (example: `23`) - sequence in binlog file. This value is growing in a binfile, several records can have the same sequence.
* `timestamp` (example: `1737623816545341000`) - immediate commit time from binlog file.

### th2 message body

Each body contains the fields:
* `Schema` - SQL schema name
* `Table` - SQL table name
* `Operation` - SQL operation name

#### test schema
```
CREATE TABLE IF NOT EXISTS test.type_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    int_col INT,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    bigint_col BIGINT,
    decimal_col DECIMAL(10,2),
    float_col FLOAT,
    double_col DOUBLE,
    char_col CHAR(10),
    varchar_col VARCHAR(50),
    text_col TEXT,
    blob_col BLOB,
    date_col DATE,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    time_col TIME,
    year_col YEAR,
    json_col JSON
);
```

#### insert message

This message contains the field:
* `Inserted` - list of dictionaries with column value pairs for inserted and generated data

Example:
```json
{
  "Schema": "test",
  "Table": "type_test",
  "Operation": "INSERT",
  "Inserted": [
    {
      "bigint_col": 9223372036854775807,
      "blob_col": "U2FtcGxlIEJMT0IgZGF0YQ==",
      "char_col": "A",
      "date_col": "2024-02-12",
      "datetime_col": "2024-02-12 14:30:00",
      "decimal_col": "12345.67",
      "double_col": 2.71828,
      "float_col": 3.14,
      "id": 1,
      "int_col": 100,
      "json_col": "{\"key\":\"value\"}",
      "mediumint_col": 500000,
      "smallint_col": 32000,
      "text_col": "U2FtcGxlIHRleHQgZGF0YQ==",
      "time_col": "14:30:00",
      "timestamp_col": "2025-02-12 13:10:37",
      "tinyint_col": 1,
      "varchar_col": "create-update-delete-test",
      "year_col": 2024
    }
  ]
}
```

#### update message

This message contains the field:
* `Updated` - list of dictionaries contained two fields:
    * `Before` - dictionary with column value pairs of record before update
    * `After` - dictionary with column value pairs of record after update

Example:
```json
{
  "Schema": "test",
  "Table": "type_test",
  "Operation": "UPDATE",
  "Updated": [
    {
      "Before": {
        "bigint_col": 9223372036854775807,
        "blob_col": "U2FtcGxlIEJMT0IgZGF0YQ==",
        "char_col": "A",
        "date_col": "2024-02-12",
        "datetime_col": "2024-02-12 14:30:00",
        "decimal_col": "12345.67",
        "double_col": 2.71828,
        "float_col": 3.14,
        "id": 1,
        "int_col": 100,
        "json_col": "{\"key\":\"value\"}",
        "mediumint_col": 500000,
        "smallint_col": 32000,
        "text_col": "U2FtcGxlIHRleHQgZGF0YQ==",
        "time_col": "14:30:00",
        "timestamp_col": "2025-02-12 13:10:37",
        "tinyint_col": 1,
        "varchar_col": "create-update-delete-test",
        "year_col": 2024
      },
      "After": {
        "bigint_col": 9000000000000000000,
        "blob_col": "VXBkYXRlZCBCTE9CIGRhdGE=",
        "char_col": "B",
        "date_col": "2025-02-12",
        "datetime_col": "2025-02-12 16:00:00",
        "decimal_col": "98765.43",
        "double_col": 5.43656,
        "float_col": 6.28,
        "id": 1,
        "int_col": 200,
        "json_col": "{\"new_key\":\"new_value\"}",
        "mediumint_col": 400000,
        "smallint_col": 31000,
        "text_col": "VXBkYXRlZCB0ZXh0IGRhdGE=",
        "time_col": "16:00:00",
        "timestamp_col": "2025-02-12 13:10:37",
        "tinyint_col": 2,
        "varchar_col": "Updated create-update-delete-test",
        "year_col": 2025
      }
    }
  ]
}
```

#### delete message

This message contains the field:
* `Deleted` - list of dictionaries with column value pairs of deleted record

Example:
```json
{
  "Schema": "test",
  "Table": "type_test",
  "Operation": "DELETE",
  "Deleted": [
    {
      "bigint_col": 9000000000000000000,
      "blob_col": "VXBkYXRlZCBCTE9CIGRhdGE=",
      "char_col": "B",
      "date_col": "2025-02-12",
      "datetime_col": "2025-02-12 16:00:00",
      "decimal_col": "98765.43",
      "double_col": 5.43656,
      "float_col": 6.28,
      "id": 1,
      "int_col": 200,
      "json_col": "{\"new_key\":\"new_value\"}",
      "mediumint_col": 400000,
      "smallint_col": 31000,
      "text_col": "VXBkYXRlZCB0ZXh0IGRhdGE=",
      "time_col": "16:00:00",
      "timestamp_col": "2025-02-12 13:10:37",
      "tinyint_col": 2,
      "varchar_col": "Updated create-update-delete-test",
      "year_col": 2025
    }
  ]
}
```

## component configuration

### custom config

* **Connection** (required) - mysql connection settings
* **Schemas** (required) - schema to tables dictionary for observing
* **Alias** (required) - th2 session alias.
* **Group** (optional) - th2 session group. Default value is value of `Alias` option

### pins config

* `mq` (required) - at least one publish pin with attributes: ['transport-group','publish']
* `grpc` (required) - client pin for `com.exactpro.th2.dataprovider.lw.grpc.DataProviderService` service. The pin should be connected to lw-data-provider run in gRPC mode.

th2 CR example
```yml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: read-mysql
spec:
  disabled: false
  imageName: ghcr.io/th2-net/th2-read-mysql-binlog-go
  imageVersion: v0.0.0-20230227123356-3b6c4aceea8f-TH2-5269-13284302048-a8b4b7a
  type: th2-read
  customConfig:
    Connection:
      Host: kos-perftest-kuber-master
      Port: 30700
      Username: th2
      Password: th2
    Schemas:
      mydb: 
        - mytable
    Alias: mysql_A_01
    Group: mysql_G_01
  pins:
    mq:
      publishers:      
      - name: to_mstore
        attributes: [transport-group, publish]
    grpc:
      client:
        - name: to_lwdp
          serviceClass: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider-grpc
              pin: server
  loggingConfig: |
    global_level=info
    disable_sampling=false
    time_field=time
    time_format=2006-01-02 15:04:05.000
    level_field=level
    message_field=message
    error_field=error
  extendedSettings:
    service:
      enabled: false
    resources:
      limits:
        cpu: 300m
        memory: 300Mi
      requests:
        cpu: 200m
        memory: 200Mi
```

## useful links:

* [go-mysql-org/go-mysql](https://github.com/go-mysql-org/go-mysql) - A pure Go library to handle MySQL network protocol and replication as used by MySQL and MariaDB.
* [julien-duponchelle/python-mysql-replication](https://github.com/julien-duponchelle/python-mysql-replication) - Pure Python Implementation of MySQL replication protocol build on top of PyMYSQL. 
* [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) - A MySQL-Driver for Go's database/sql package