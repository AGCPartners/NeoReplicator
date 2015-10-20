# NeoReplicator
A MySQL to Neo4J replicator based on binlog listener running on Node.js.

NeoReplicator is a further development of 

This package has been tested with MySQL server 5.5.40 and 5.6.19. All MySQL server versions >= 5.1.15 are supported.
It is briefly tested with MariaDB 10.0

## Quick Start

```javascript
var neorep = new NeoReplicator({
  mysql: { port: '3306', host: 'localhost', user: 'neorep', password: 'neorep' },
  neo4j: { port: '7474', host: 'localhost', user: 'neo4j', password: 'neo4j' },
  mapping: { /* see in example.js */ }
});

// Replication must be started, as a parameter pass the name of the MySQL database to replicate
neorep.start(
  includeSchema: { 'mysqlDatabaseName': true }
});
```

For a complete implementation see [`example.js`](example.js)...

## Installation

* Requires Node.js v0.10+

  ```bash
  $ npm install neoReplicator
  ```

* Enable MySQL binlog in `my.cnf`, restart MySQL server after making the changes.
  > From [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html), binlog checksum is enabled by default. Zongji can work with it, but it doesn't really verify it.

  ```
  # binlog config
  server-id        = 1
  log_bin          = /var/log/mysql/mysql-bin.log
  expire_logs_days = 10            # optional
  max_binlog_size  = 100M          # optional

  # Very important if you want to receive write, update and delete row events
  binlog_format    = row
  ```
* Create an account with replication privileges, e.g. given privileges to account `neorep` (or any account that you use to read binary logs)

  ```sql
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'neorep'@'localhost'
  ```

## Reference

This package is a further development of ['Zong Ji'](https://github.com/nevill/zongji) library
The main dependencies are:

* https://github.com/felixge/node-mysql
* https://github.com/philippkueng/node-neo4j

## License
MIT
