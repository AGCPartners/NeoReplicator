// Client code
var NeoReplicator = require('./');

var mapping = {
  Users: {
    type: 'node',
    label: 'User',
    primaryKey: 'id',
    properties: ['firstName','lastName','email'],
    relations: {}
  },
  Companies: {
    type: 'node',
    label: 'Company',
    primaryKey: 'id',
    properties: ['name','state','city'],
    relations: {
      country: {
        label: 'IS_IN',
        endNodeLabel: 'Country'
      }
    }
  },
  Countries: {
    type: 'node',
    label: 'Country',
    primaryKey: 'id',
    properties: ['name','shortName']
  },
  UserCompanies: {
    type: 'relation',
    label: 'WORKS_FOR',
    primaryKey: 'id',
    startNode: {
      label: 'User',
      primaryKey: 'userId'
    },
    endNode: {
      label: 'Company',
      primaryKey: 'companyId'
    },
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
    port: '7474',
    secure: false
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
