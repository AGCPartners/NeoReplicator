# NeoReplicator
A MySQL to Neo4J replicator based on binlog listener running on Node.js.

### Update

Thanks to a great feedback from guys from Neo Technology we have updated the cypher queries to use more of the potential Neo4J has.
Due to some major changes in cypher queries structure of the mapping has been changed. Take a look at the [`example.js`](example.js) file.
In the future the main focus will be on changing from single connection to a connection pool for connecting to the mysql master.

### Goals

The package is still in development and some heavy change can occur.
The final goal is to have an app that will more or less instantaneously replicate the changes made in MySQL database to a Neo4J graph database.

### TODO list

* create nodes from insert queries ✓
* create relations based on foreign keys ✓
* create relations based on Many-To-Many relations ✓
* update nodes after update queries ✓
* delete nodes ✓
* delete relations ✓
* improve stability ✓
* error reporting
* start from last replicated position
* write tests
* Travis Integration
* Proper git structure(dev branch, git-flow)
* reconnect after the connection is dropped by mysql server ✓

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

This package is a further development of ['Zong Ji'](https://github.com/nevill/zongji) library.
The main dependencies are:

* https://github.com/felixge/node-mysql
* https://github.com/philippkueng/node-neo4j

## License
MIT
