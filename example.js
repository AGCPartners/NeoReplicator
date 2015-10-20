// Client code
var NeoReplicator = require('./');

var mapping = {
  Users: {
    type: 'node',
    name: 'User',
    primaryKey: 'id',
    properties: ['firstName','lastName','email'],
    relations: {}
  },
  Companies: {
    type: 'node',
    name: 'Company',
    primaryKey: 'id',
    properties: ['name','state','city'],
    relations: {
      country: 'IS_IN'
    }
  },
  Countries: {
    type: 'node',
    name: 'Country',
    primaryKey: 'id',
    properties: ['name','shortName']
  },
  UserCompanies: {
    type: 'relation',
    name: 'WORKS_FOR',
    primaryKey: 'id',
    startNode: 'userId',
    endNode: 'companyId',
    properties: ['department']
  }
};

var neorep = new NeoReplicator({
  mysql: {
    host: 'localhost',
    user: 'neo4j',
    password: 'wdF5uA3r',
    port: '3306'
  },
  neo4j: {
    host: 'localhost',
    user: 'neo4j',
    password: 'A4qMLUUH',
    port: '7474'
  },
  mapping: mapping
});

neorep.start({
  includeSchema: { 'databaseName': true }
});

process.on('SIGINT', function() {
  console.log('Got SIGINT.');
  neorep.stop();
  process.exit();
});
