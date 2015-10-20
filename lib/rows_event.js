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

// RowsEvent.prototype.dump = function() {
//   BinlogEvent.prototype.dump.apply(this);
//   console.log('Affected columns:', this.numberOfColumns);
//   console.log('Changed rows:', this.rows.length);
//   console.log('Values:');
//   this.rows.forEach(function(row) {
//     console.log('--');
//     Object.keys(row).forEach(function(name) {
//       console.log('Column: %s, Value: %s', name, row[name]);
//     });
//   });
// };

RowsEvent.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('dump');
};

RowsEvent.prototype.replicate = function(config) {
  BinlogEvent.prototype.dump.apply(this);
  var props = '';
  var relations = [];
  var tableMap = this.tableMap[this.tableId];
  var neo4j = this.neo4j;

  if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'node') {
    this.rows.forEach(function(row) {
      props = '';
      relations = [];
      console.log('--');
      Object.keys(row).forEach(function(name) {
        if(config[tableMap.tableName].properties.indexOf(name) > -1) {
          if(props.length) {
            props += ', instance.' + (name === 'id' ? 'mysql_id':name) + '=\'' + row[name] + '\'';
          } else {
            props += 'instance.' + (name === 'id' ? 'mysql_id':name) + '=\'' + row[name] + '\'';
          }        
        }

        if(typeof config[tableMap.tableName].relations !== 'undefined' && config[tableMap.tableName].relations.hasOwnProperty(name)) {
          var relQuery = 'MATCH (a { mysql_id:\'' + row['id'] + '\'}), (b { mysql_id: \'' + row[name] + '\'}) CREATE (a)-[:'+config[tableMap.tableName].relations[name]+']->(b)';
          relations.push(relQuery);
        }
      });

      var query = 'MERGE (instance:' + config[tableMap.tableName].name + ' { mysql_id: \'' + row['id'] + '\'})' +
            ' ON CREATE SET ' + props +
            ' ON MATCH SET ' + props +
            ' RETURN instance'; 
      neo4j.cypherQuery(query, function(err, res) {
        if (err) {
          // console.log('===== Cypher Error ======');
          // console.log('Query: ' + query);
          // console.log('Error: ');
          // console.log(err);
        } else {
          // console.log('===== Cypher Success =====');
          // console.log('Data:');
          // console.log(res.data);
        }
        relations.forEach(function(rel) {
          neo4j.cypherQuery(rel, function(err, res) {
            if (err) {
              // console.log('===== Cypher Relation Error ======');
              // console.log('Query: ' + rel);
              // console.log('Error: ');
              // console.log(err);
            } else {
              // console.log('===== Cypher Relation Success =====');
              // console.log('Data:');
              // console.log(res.data);
            }   
          });
        });
      });
    });
  } else if(config.hasOwnProperty(tableMap.tableName) && config[tableMap.tableName].type === 'relation') {
    this.rows.forEach(function(row) {
      props = '';
      relations = [];
      Object.keys(row).forEach(function(name) {
        if(config[tableMap.tableName].properties.indexOf(name) > -1) {
          if(props.length) {
            props += ', ' + (name === 'id' ? 'mysql_id':name) + ':\'' + row[name] + '\'';
          } else {
            props += (name === 'id' ? 'mysql_id':name) + ':\'' + row[name] + '\'';
          }        
        }

      });
      var relQuery = 'MATCH (a { mysql_id:\'' + row[config[tableMap.tableName].startNode] + '\'}), (b { mysql_id: \'' + row[config[tableMap.tableName].endNode] + '\'}) CREATE (a)-[:'+config[tableMap.tableName].name+'{ '+props+' }]->(b)';
      neo4j.cypherQuery(relQuery, function(err, res) {
        if (err) {
          console.log('===== Cypher MtM Error ======');
          console.log('Query: ' + relQuery);
          console.log('Error: ');
          console.log(err);
        } else {
          console.log('===== Cypher MtM Success =====');
          console.log('Data:');
          console.log(res.data);
        }
      });
    });
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
  console.log('Affected columns:', this.numberOfColumns);
  console.log('Changed rows:', this.rows.length);
  console.log('Values:');
  this.rows.forEach(function(row) {
    console.log('--');
    Object.keys(row.before).forEach(function(name) {
      console.log('Column: %s, Value: %s => %s', name, row.before[name], row.after[name]);
    });
  });
};

exports.WriteRows = WriteRows;
exports.DeleteRows = DeleteRows;
exports.UpdateRows = UpdateRows;