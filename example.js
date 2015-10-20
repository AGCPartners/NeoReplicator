// Client code
var NeoRep = require('./');

var neorep = new NeoRep({
  host     : 'localhost',
  user     : 'neorep',
  password : 'neorep',
  // debug: true
});

neorep.on('binlog', function(evt) {
  evt.dump();
});

neorep.start({
  includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
});

process.on('SIGINT', function() {
  console.log('Got SIGINT.');
  neorep.stop();
  process.exit();
});
