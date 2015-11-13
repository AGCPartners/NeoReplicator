var util = require('util');
var BinlogEvent = require('./binlog_event').BinlogEvent;
var Common = require('./common');

var Version2Events = [
  0x1e, // WRITE_ROWS_EVENT_V2,
  0x1f, // UPDATE_ROWS_EVENT_V2,
  0x20, // DELETE_ROWS_EVENT_V2
];

var CHECKSUM_SIZE = 4;

/**
 * Generic RowsEvent class
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 *   neorep: NeoReplicator instance
 **/

function RowsEvent(parser, options, neorep) {
  BinlogEvent.apply(this, arguments);
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);
  this.useChecksum = neorep.useChecksum;
  this.neo4j = neorep.neo4jDb;
  this.cypherQueue = neorep.cypherQueue;

  // Version 2 Events
  if (Version2Events.indexOf(options.eventType) !== -1) {
    this.extraDataLength = parser.parseUnsignedNumber(2);
    // skip extra data
    parser.parseBuffer(this.extraDataLength - 2);
  }

  // Body
  this.numberOfColumns = parser.parseLengthCodedNumber();

  this.tableMap = options.tableMap;

  var tableData = this.tableMap[this.tableId];
  if(tableData === undefined){
    // TableMap event was filtered
    parser._offset = parser._packetEnd;
    this._filtered = true;
  }else{
    var database = tableData.parentSchema;
    var table = tableData.tableName;
    var columnsPresentBitmapSize = Math.floor((this.numberOfColumns + 7) / 8);
    // Columns present bitmap exceeds 4 bytes with >32 rows
    // And is not handled anyways so just skip over its space
    parser._offset += columnsPresentBitmapSize;
    if(this._hasTwoRows) {
      // UpdateRows event slightly different, has new and old rows represented
      parser._offset += columnsPresentBitmapSize;
    }

    if(this.useChecksum){
      // Ignore the checksum at the end of this packet
      parser._packetEnd -= CHECKSUM_SIZE;
    }

    this.rows = [];
    while (!parser.reachedPacketEnd()) {
      this.rows.push(this._fetchOneRow(parser));
    }

    if(this.useChecksum){
      // Skip past the checksum at the end of the packet
      parser._packetEnd += CHECKSUM_SIZE;
      parser._offset += CHECKSUM_SIZE;
    }
  }
}

util.inherits(RowsEvent, BinlogEvent);

RowsEvent.prototype.setTableMap = function(tableMap) {
  this.tableMap = tableMap;
};

RowsEvent.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
};

RowsEvent.prototype._updateNode = function(instConf) {
  var self = this;
  this.rows.forEach(function(rawRow) {
    var row = rawRow.after;
    var originalPK = rawRow.before[instConf.primaryKey];
    var addProps = '';
    var removeProps = '';
    var relations = [];
    Object.keys(row).forEach(function(name) {
      if(instConf.properties.indexOf(name) > -1 || name === instConf.primaryKey) {
        if(row[name] === null) {
          if(removeProps.length) {
            removeProps += ', instance.' + (name === instConf.primaryKey ? 'mysql_pk':name);
          } else {
            removeProps += 'instance.' + (name === instConf.primaryKey ? 'mysql_pk':name);
          }
        } else {
          if(addProps.length) {
            addProps += ', instance.' + (name === instConf.primaryKey ? 'mysql_pk':name) + '=\'' + row[name] + '\'';
          } else {
            addProps += 'instance.' + (name === instConf.primaryKey ? 'mysql_pk':name) + '=\'' + row[name] + '\'';
          }
        }
      }

      if(typeof instConf.relations !== 'undefined' && instConf.relations.hasOwnProperty(name)) {
        if(row[name] !== null) {
          var relQuery = 'MATCH (a:' + instConf.label + ' { mysql_pk:\'' + originalPK + '\'}), (b:' +
            instConf.relations[name].endNodeLabel + ' { mysql_pk: \'' + row[name] + '\'}) CREATE (a)-[:' +
            instConf.relations[name].label + ']->(b)';
          relations.push(relQuery);
        } else {
          var relQuery = 'MATCH (a:' + instConf.label + ' { mysql_pk:\'' + originalPK + '\'})-[c:' +
            instConf.relations[name].label + ']-(b:'+instConf.relations[name].endNodeLabel + ' { mysql_pk: \'' +
            row[name] + '\'}) DELETE c';
          relations.push(relQuery);
        }
      }
    });

    var query = 'MATCH (instance:' + instConf.label + ' { mysql_pk: \'' + originalPK + '\'})';
    query += addProps.length ? (' SET ' + addProps) : '';
    query += removeProps.length ? (' REMOVE ' + removeProps) : '';

    self.cypherQueue.push(query);
    relations.forEach(function(rel) {
      self.cypherQueue.push(rel);
    });
  });

  return;
};

RowsEvent.prototype._createNode = function(instConf) {
  var self = this;
  this.rows.forEach(function(row) {
    var originalPK = row[instConf.primaryKey];   
    var props = '';
    var relations = [];
    // console.log(row);
    Object.keys(row).forEach(function(name) {
      if(instConf.properties.indexOf(name) > -1 || name === instConf.primaryKey && row[name] !== null) {
        if(props.length) {
          props += ', ' + (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
        } else {
          props += (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
        }        
      }

      if(typeof instConf.relations !== 'undefined' && instConf.relations.hasOwnProperty(name) && row[name] !== null) {
        var relQuery = 'MATCH (a:' + instConf.label + ' { mysql_pk:\'' + originalPK + '\'}), (b:' +
          instConf.relations[name].endNodeLabel + ' { mysql_pk: \'' + row[name] +
          '\'}) CREATE (a)-[:'+instConf.relations[name].label+']->(b)';
        relations.push(relQuery);
      }
    });

    var query = 'CREATE (instance:' + instConf.label + ' { ' + props + ' })';

    self.cypherQueue.push(query);
    relations.forEach(function(rel) {
      self.cypherQueue.push(rel);
    });
  });

  return;
};

RowsEvent.prototype._deleteNode = function(instConf) {
  var self = this;
  this.rows.forEach(function(row) {
    var originalPK = row[instConf.primaryKey];
    props = '';
    relations = [];
    Object.keys(row).forEach(function(name) {
      if(typeof instConf.relations !== 'undefined' && instConf.relations.hasOwnProperty(name) && row[name] !== null) {
        var relQuery = 'MATCH (a:' + instConf.label + ' { mysql_pk:\'' + originalPK + '\'})-[c:' +
          instConf.relations[name].label + ']->(b:'+instConf.relations[name].endNodeLabel +
          ' { mysql_pk: \'' + row[name] + '\'}) delete c';
        relations.push(relQuery);
      }
    });

    var query = 'MATCH (instance:' + instConf.label + ' { mysql_pk: \'' + originalPK + '\'}) delete instance';

    relations.forEach(function(rel) {
      self.cypherQueue.push(rel);
    });
    self.cypherQueue.push(query);
  });

  return;
};

RowsEvent.prototype._updateRelation = function(instConf) {
  var self = this;
  this.rows.forEach(function(rawRow) {
    var row = rawRow.after;
    var originalPK = rawRow.before[instConf.primaryKey];
    var addProps = '';
    var removeProps = '';

    Object.keys(row).forEach(function(name) {
      if(instConf.properties.indexOf(name) > -1 || name === instConf.primaryKey) {
        if(row[name] === null) {
          if(removeProps.length) {
            removeProps += ', c.' + (name === instConf.primaryKey ? 'mysql_pk':name);
          } else {
            removeProps += 'c.' + (name === instConf.primaryKey ? 'mysql_pk':name);
          }
        } else {
          if(addProps.length) {
            addProps += ', c.' + (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
          } else {
            addProps += 'c.' + (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
          }          
        }
      }
    });

    var relQuery = 'MATCH (a:'+row[instConf.startNode.label]+' { mysql_pk:\'' + row[instConf.startNode.primaryKey] + '\'})-[c:'+
      instConf.label+' { mysql_pk: \''+row[instConf.primaryKey]+'\'}]-(b:'+row[instConf.endNode.label]+' { mysql_pk: \'' + 
      row[instConf.endNode.primaryKey] + '\'})';

    relQuery += addProps.length ? (' SET ' + addProps) : '';
    relQuery += removeProps.length ? (' REMOVE ' + removeProps) : '';
    self.cypherQueue.push(relQuery);
  });
  return;
};

RowsEvent.prototype._createRelation = function(instConf) {
  var self = this;
  this.rows.forEach(function(row){
    var originalPK = row[instConf.primaryKey];
    var props = '';
    Object.keys(row).forEach(function(name) {
      if(instConf.properties.indexOf(name) > -1 || name === instConf.primaryKey && row[name] !== null) {
        if(props.length) {
          props += ', ' + (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
        } else {
          props += (name === instConf.primaryKey ? 'mysql_pk':name) + ':\'' + row[name] + '\'';
        }        
      }

    });
    var relQuery = 'MATCH (a:' + row[instConf.startNode.label] + ' { mysql_pk:\'' + row[instConf.startNode.primaryKey] +
      '\'}), (b:' + row[instConf.endNode.label] + ' { mysql_pk: \'' + row[instConf.endNode.primaryKey] +
      '\'}) CREATE (a)-[:' + instConf.label + '{ ' + props + ' }]->(b)';
    self.cypherQueue.push(relQuery);
  });

  return;
};

RowsEvent.prototype._deleteRelation = function(instConf) {
  var self = this;
  this.rows.forEach(function(row) {
    var relQuery = 'MATCH (a:'+row[instConf.startNode.label]+' { mysql_pk:\'' + row[instConf.startNode.primaryKey] +
      '\'})-[c:' + instConf.label + '{ mysql_pk: \'' + row[instConf.primaryKey] + '\' }]-(b:' + row[instConf.endNode.label] +
      ' { mysql_pk: \'' + row[instConf.endNode,primaryKey] + '\'}) delete c';
    self.cypherQueue.push(relQuery);
  });

  return;
};

RowsEvent.prototype.replicate = function(config, next) {
  BinlogEvent.prototype.dump.apply(this);
  var queryType = this.getTypeName();

  console.log(queryType);
  if(config.hasOwnProperty(this.tableMap[this.tableId].tableName)) {
    var instConf = config[this.tableMap[this.tableId].tableName];

    if(instConf.type === 'node') {
      if(queryType === 'DeleteRows') {
        this._deleteNode(instConf);
      } else if(queryType === 'UpdateRows') {
        this._updateNode(instConf);
      } else if(queryType === 'WriteRows') {
        this._createNode(instConf);
      }
    } else if(instConf.type === 'relation') {
     if(queryType === 'DeleteRows') {
        this._deleteRelation(instConf);
      } else if(queryType === 'UpdateRows') {
        this._updateRelation(instConf);
      } else if(queryType === 'WriteRows') {
        this._createRelation(instConf);
      }
    }

    next();
  } else {
    return next();
  }
};

RowsEvent.prototype._fetchOneRow = function(parser) {
  return readRow(this.tableMap[this.tableId], parser);
};

var readRow = function(tableMap, parser) {
  var row = {}, column, columnSchema;
  var nullBitmapSize = Math.floor((tableMap.columns.length + 7) / 8);
  var nullBuffer = parser._buffer.slice(parser._offset, 
                                        parser._offset + nullBitmapSize);
  var curNullByte, curBit;
  parser._offset += nullBitmapSize;

  for (var i = 0; i < tableMap.columns.length; i++) {
    curBit = i % 8;
    if(curBit === 0) curNullByte = nullBuffer.readUInt8(Math.floor(i / 8));
    column = tableMap.columns[i];
    columnSchema = tableMap.columnSchemas[i];
    if((curNullByte & (1 << curBit)) === 0){
      row[column.name] = Common.readMysqlValue(parser, column, columnSchema);
    }else{
      row[column.name] = null;
    }
  }
  return row;
};

// Subclasses
function WriteRows(parser, options) {
  RowsEvent.apply(this, arguments);
}

util.inherits(WriteRows, RowsEvent);

function DeleteRows(parser, options) {
  RowsEvent.apply(this, arguments);
}

util.inherits(DeleteRows, RowsEvent);

function UpdateRows(parser, options) {
  this._hasTwoRows = true;
  RowsEvent.apply(this, arguments);
}

util.inherits(UpdateRows, RowsEvent);

UpdateRows.prototype._fetchOneRow = function(parser) {
  var tableMap = this.tableMap[this.tableId];
  return {
    before: readRow(tableMap, parser),
    after: readRow(tableMap, parser)
  };
};

UpdateRows.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
};

exports.WriteRows = WriteRows;
exports.DeleteRows = DeleteRows;
exports.UpdateRows = UpdateRows;
