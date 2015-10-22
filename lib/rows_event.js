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

RowsEvent.prototype.replicate = function(config, next) {
  BinlogEvent.prototype.dump.apply(this);
  var props = '';
  var relations = [];
  var tableMap = this.tableMap[this.tableId];
  var neo4j = this.neo4j;
  var queue = this.cypherQueue;
  var queryType = this.getTypeName();

  if(queryType === 'DeleteRows') {
    if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'node') {
      this.rows.forEach(function(rawRow) {
        if(rawRow.hasOwnProperty('before') && rawRow.hasOwnProperty('after')) {
          var row = rawRow.after;
          var originalPK = rawRow.before[config[tableMap.tableName].primaryKey];
        } else {
          var row = rawRow;
          var originalPK = rawRow[config[tableMap.tableName].primaryKey];
        }
        props = '';
        relations = [];
        Object.keys(row).forEach(function(name) {
          if(typeof config[tableMap.tableName].relations !== 'undefined' && config[tableMap.tableName].relations.hasOwnProperty(name) && row[name] !== null) {
            var relQuery = 'MATCH (a { mysql_id:\'' + originalPK + '\'})-[c:'+config[tableMap.tableName].relations[name]+']->(b { mysql_id: \'' + row[name] + '\'}) delete c';
            relations.push(relQuery);
          }
        });

        var query = 'MATCH (instance:' + config[tableMap.tableName].name + ' { mysql_id: \'' + originalPK + '\'}) delete instance';

        relations.forEach(function(rel) {
          queue.push(rel);
        });
        queue.push(query);
      });
    } else if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'relation') {
      this.rows.forEach(function(row) {
        var relQuery = 'MATCH (a { mysql_id:\'' + row[config[tableMap.tableName].startNode] + '\'})-[c:'+config[tableMap.tableName].name+'{ mysql_id: \''+row[config[tableMap.tableName].primaryKey]+'\' }]-(b { mysql_id: \'' + row[config[tableMap.tableName].endNode] + '\'}) delete c';
        queue.push(relQuery);
      });
    }
  } else {
    if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'node') {
      this.rows.forEach(function(rawRow) {
        if(rawRow.hasOwnProperty('before') && rawRow.hasOwnProperty('after')) {
          var row = rawRow.after;
          var originalPK = rawRow.before[config[tableMap.tableName].primaryKey];
        } else {
          var row = rawRow;
          var originalPK = rawRow[config[tableMap.tableName].primaryKey];
        }
        props = '';
        relations = [];
        Object.keys(row).forEach(function(name) {
          if(config[tableMap.tableName].properties.indexOf(name) > -1) {
            if(props.length) {
              props += ', instance.' + (name === config[tableMap.tableName].primaryKey ? 'mysql_id':name) + '=\'' + row[name] + '\'';
            } else {
              props += 'instance.' + (name === config[tableMap.tableName].primaryKey ? 'mysql_id':name) + '=\'' + row[name] + '\'';
            }        
          }

          if(typeof config[tableMap.tableName].relations !== 'undefined' && config[tableMap.tableName].relations.hasOwnProperty(name) && row[name] !== null) {
            var relQuery = 'MATCH (a { mysql_id:\'' + originalPK + '\'}), (b { mysql_id: \'' + row[name] + '\'}) CREATE (a)-[:'+config[tableMap.tableName].relations[name]+']->(b)';
            relations.push(relQuery);
          }
        });

        var query = 'MERGE (instance:' + config[tableMap.tableName].name + ' { mysql_id: \'' + originalPK + '\'})' +
              ' ON CREATE SET ' + props +
              ' ON MATCH SET ' + props +
              ' RETURN instance'; 

        queue.push(query);
        relations.forEach(function(rel) {
          queue.push(rel);
        });
      });
    } else if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'relation') {
      this.rows.forEach(function(row) {
        props = '';
        relations = [];
        Object.keys(row).forEach(function(name) {
          if(config[tableMap.tableName].properties.indexOf(name) > -1) {
            if(props.length) {
              props += ', ' + (name === config[tableMap.tableName].primaryKey ? 'mysql_id':name) + ':\'' + row[name] + '\'';
            } else {
              props += (name === config[tableMap.tableName].primaryKey ? 'mysql_id':name) + ':\'' + row[name] + '\'';
            }        
          }

        });
        var relQuery = 'MATCH (a { mysql_id:\'' + row[config[tableMap.tableName].startNode] + '\'}), (b { mysql_id: \'' + row[config[tableMap.tableName].endNode] + '\'}) CREATE (a)-[:'+config[tableMap.tableName].name+'{ '+props+' }]->(b)';
        queue.push(relQuery);
      });
    }
  }
  next();
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
