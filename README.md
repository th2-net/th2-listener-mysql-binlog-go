# th2-read-mysql-binlog-go

read mysql binlog component connects to mysql data base as `Replication Slave` to read binlog in realtime and send information about `INSERT`, `UPDATE`, `DELETE` operation via RabbitMQ in th2 raw message format. Each raw message has JSON format

## mysql user requirements

```sql
CREATE USER 'th2'@'%' IDENTIFIED BY 'th2';
GRANT REPLICATION SLAVE ON *.* TO 'th2'@'%';
GRANT SELECT ON <target db>.* TO 'th2'@'%';
FLUSH PRIVILEGES;
```

## raw message format

read-mysql-binlog produces th2 raw message where each message has JSON body format and binlog position in properties

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

#### insert



INSERT example:
```json

```

UPDATE example:
```json

```

DELETE example:
```json

```

## errors

* `getting binlog event failure: ERROR 1236 (HY000): Client requested source to start replication from position > file size` 