var sql=require('msnodesql')
  , logger = require('./logger')
  , config = require('./config')
  , async = require('async')


var conn_str = "Driver={SQL Server Native Client 11.0};Server={(local)\\" 
                        + config.sqlServerName + "};Database={" + config.database 
                        + "};uid=" + config.username + ";PWD=" + config.password + ";";

var spaceTypes = {}
  , spaceContents = {}
  , spaces = {}
  , connection;

exports.run = function(data) {
  sql.open(conn_str, function(err, conn) {
    if (err) {
      logger.error('Can not connect to Sql server, Aborted!');
      return logger.error(err);
    }

    connection = conn;
    // 对space type做缓存
    conn.queryRaw("select * from ISIS.TD_SPACE_TYPE", function(err, results) {
      if (err) {
        logger.error('Can not open table ISIS.TD_SPACE_TYPE, Aborted!');
        return logger.log(err);
      } else {
        for (var i = 0; i < results.rows.length; i++) {
          var key = results.rows[i][1] + '&' + results.rows[i][6];
          spaceTypes[key] = results.rows[i][0];
        }

        // 对space content缓存
        conn.queryRaw("select * from ISIS.TD_SPACE_CONTENT", function(err, results) {
          if (err) {
            logger.error('Can not open table ISIS.TD_SPACE_CONTENT, Aborted!');
            return logger.log(err);
          } else {
            for (var i = 0; i < results.rows.length; i++) {
              var key = results.rows[i][1] + '&' + results.rows[i][10];
              spaceContents[key] = results.rows[i][0];
            }

            // 按顺序导入没一条记录
            async.eachSeries(data, handler, function(err){
              if(err){
                logger.error("Line: " + err.record.index + 'Failed at step: ' + err.step);
                return logger.error(err);
              }

              logger.info("Success. Total lines: " + data.length);
            });
          }
        }) // space content cache
      }
    }) // space type cache
  }) //sql.open
}

function handler(record, cb){
  async.series({
    zone: function(next){
      // set zon_spaceTypeId
      record.zone_spaceTypeId = spaceTypes['Zone&' + record.accountId];
      hanleFiled(connection, record, 'zone', next);
    },

    building: function(next){
      // set building_spaceTypeId
      record.building_spaceTypeId = spaceTypes['Building&' + record.accountId];
      hanleFiled(connection, record, 'building', next);
    },

    floor: function(next) {
      // set floor_spaceTypeId
      record.floor_spaceTypeId = spaceTypes['Floor&' + record.accountId];
      hanleFiled(connection, record, 'floor', next);
    },

    room: function(next) {
      // set room_spaceTypeId
      record.room_spaceTypeId = spaceTypes[record.roomType + '&' + record.accountId];
      hanleFiled(connection, record, 'room', next);
    },

    content: function(next) {
      // 查询floor content
      record.spaceContentId = spaceContents[record.floorType + '&' + record.accountId];
      if(!record.spaceContentId){
        var error = new Error('Space Content is not existed:' + record.floorType + '&' + record.accountId);
        next(error, 'content');
      }

      var query = "INSERT INTO ISIS.TD_SPACE_INV_CONTENT(dc_Quantity,dc_Length,dc_Width,in_SpaceID,in_SpaceContentID) VALUES(?,?,?,?,?)";
      var done = false;
      connection.queryRaw(query, [record.sqm, record.length, record.width, record.room_spaceId, record.spaceContentId], 
        function(err, results){
          if(err) return next(err, 'content');

          if(results.rowcount > 0 && !done){
            done = true;
            next(null, 'content');
          }
        }
      )
    },

    fixture: function(next) {
      // 查询fixture
      record.f_spaceContentId = spaceContents['fixture' + '&' + record.accountId];
      if(!record.f_spaceContentId){
        var error = new Error('Fixture Space Content is not existed:' + 'fixture&' + record.accountId);
        next(error, 'fixture');
      }

      var query = "INSERT INTO ISIS.TD_SPACE_INV_CONTENT(dc_Quantity,in_SpaceID,in_SpaceContentID) VALUES(?,?,?,?,?)";
      var done = false;
      connection.queryRaw(query, [record.sqm, record.room_spaceId, record.f_spaceContentId], 
        function(err, results){
          if(err) return next(err, 'fixture');

          if(results.rowcount > 0 && !done){
            done = true;
            next(null, 'fixture');
          }
        }
      )
    }
  },
    
  function(err, results){
    if(err){
      if(err.ignore){
        // 警告，但不退出
        logger.warn("Line: " + record.index + " " + err)
        cb();
      }else{
        err.record = record;
        err.step = results[results.length-1];
        return cb(err);
      }
    }else{
      logger.info(record.join(', '));
      cb();
    }

  });
}

function hanleFiled(connection, record, space_level, next){

  if(!record[space_level+'_spaceTypeId']){
    var error = new Error('Space Type Id is not existed.')
    next(error, space_level);
  }
  
  isSpaceExisted(connection, record, space_level, function(err, id) {
    if (err) return next(err, space_level);

    if (id) {
        logger.info('Line: ' + record.index + ' ' + space_level + ' existed. Id: ' + id);
        record[space_level + '_spaceId'] = id;
      if(space_level != 'room'){
        next(null, space_level);
      }else{
        var error = new Error('Room existed!');
        error.ignore = true;
        next(error, 'room');
      }
    } else {
      // create space
      createSpace(connection, record, space_level, function(err, id) {
        if (err) return next(err, space_level);

        record[space_level + '_spaceId'] = id;
        logger.info('Line: ' + record.index + ' new ' + space_level + ' created. Id: ' + id);
        next(null, space_level);
      }); // create space
    }
  })

}

function isSpaceExisted(conn, record, space_level, cb){
  var key
    , query
    , params;

  switch(space_level){
    case 'zone':
      key = record.zone + '&' + record.accountId + '&' + record.zone_spaceTypeId;
      query = "SELECT * from ISIS.TD_SPACE_INVENTORY WHERE tx_SpaceName=? AND in_AccountID=? AND in_SpaceTypeID=?";
      params = [record.zone, record.accountId, record.zone_spaceTypeId];
      break;

    case 'building':
      key = record.building + '&' + record.accountId + '&' + record.building_spaceTypeId + '&' + record.zone_spaceId;
      query = "SELECT * from ISIS.TD_SPACE_INVENTORY WHERE tx_SpaceName=? AND in_AccountID=? AND in_SpaceTypeID=? AND in_ParentID=?";
      params = [record.building, record.accountId, record.building_spaceTypeId, record.zone_spaceId];
      break;

    case 'floor':
      key = record.floor + '&' + record.accountId + '&' + record.floor_spaceTypeId + '&' + record.building_spaceId;
      query = "SELECT * from ISIS.TD_SPACE_INVENTORY WHERE tx_SpaceName=? AND in_AccountID=? AND in_SpaceTypeID=? AND in_ParentID=?";
      params = [record.floor, record.accountId, record.floor_spaceTypeId, record.building_spaceId];
      break;

    case 'room':
      key = record.room + '&' + record.accountId + '&' + record.room_spaceTypeId + '&' + record.floor_spaceId;
      query = "SELECT * from ISIS.TD_SPACE_INVENTORY WHERE tx_SpaceName=? AND in_AccountID=? AND in_SpaceTypeID=? AND in_ParentID=?";
      params = [record.room, record.accountId, record.room_spaceTypeId, record.floor_spaceId];
      break;
  }

  // 从缓存中取出新建的 space id
  if(spaces[key]) return cb(null, spaces[key]);

  // conn.queryRaw 检查记录 
  conn.queryRaw(query, params, 
    function(err, results){
      if(err) return cb(err);

      if(results.rows.length > 0){
        cb(null, id);
      }else{
        cb(null, null);
      }
  })// conn.queryRaw 检查记录 
}

function createSpace(conn, record, space_level, cb){
  var key
    , query
    , params;

  switch(space_level){
    case 'zone':
      key = record.zone + '&' + record.accountId + '&' + record.zone_spaceTypeId;
      query = "INSERT INTO ISIS.TD_SPACE_INVENTORY(tx_SpaceName,in_AccountID,tf_Active,in_SpaceTypeID,tf_Inspectable,tf_HRS) VALUES(?,?,?,?,?,?)";
      params = [record.zone, record.accountId, record.active, record.zone_spaceTypeId, 1, 1];
      break;

    case 'building':
      key = record.building + '&' + record.accountId + '&' + record.building_spaceTypeId + '&' + record.zone_spaceId;
      query = "INSERT INTO ISIS.TD_SPACE_INVENTORY(tx_SpaceName,in_AccountID,tf_Active,in_SpaceTypeID,tf_Inspectable,tf_HRS,in_ParentID) VALUES(?,?,?,?,?,?,?)";
      params = [record.building, record.accountId, record.active, record.building_spaceTypeId, 1, 1, record.zone_spaceId];
      break;

    case 'floor':
      key = record.floor + '&' + record.accountId + '&' + record.floor_spaceTypeId + '&' + record.building_spaceId;
      query = "INSERT INTO ISIS.TD_SPACE_INVENTORY(tx_SpaceName,in_AccountID,tf_Active,in_SpaceTypeID,tf_Inspectable,tf_HRS,in_ParentID) VALUES(?,?,?,?,?,?,?)";
      params = [record.floor, record.accountId, record.active, record.floor_spaceTypeId, 1, 1, record.building_spaceId];
      break;

    case 'room':
      key = record.room + '&' + record.accountId + '&' + record.room_spaceTypeId + '&' + record.floor_spaceId;
      query = "INSERT INTO ISIS.TD_SPACE_INVENTORY(tx_SpaceName,in_AccountID,tf_Active,in_SpaceTypeID,tf_Inspectable,tf_HRS,in_ParentID) VALUES(?,?,?,?,?,?,?,?)";
      params = [record.room, record.accountId, record.active, record.room_spaceTypeId, 1, 1, record.floor_spaceId, record.beds];
      break;
  }

  var done = false;
  conn.queryRaw(query, params,
    function(err, results){
      if(err) return cb(err);

      // 插入成功，执行回调一次
      if(results.rowcount > 0 && !done){
        done = true;

        conn.queryRaw("SELECT @@identity", 
          function(err, results){
            if(err) return cb(err);

            if(results.rows.length > 0){
              var id = results.rows[0][0];

              // 缓存新建立的space
              if(space_level != 'room')
                spaces[key] = id;

              cb(null, id);
            }else{
              cb(new Error('Line: ' + record.index + ' failed at SELECT @@identity. Contact Junhua. Aborted!' + space_level));
            }
          }
        ) // conn.queryRaw 查询插入结果
      } // if(results.rowcount > 0 && !done)
    }
  ) // conn.queryRaw 插入
}