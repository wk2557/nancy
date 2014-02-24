var sql=require('msnodesql')
  , logger = require('./logger')
  , config = require('./config')


var conn_str = "Driver={SQL Server Native Client 11.0};Server={(local)\\" 
                        + config.sqlServerName + "};Database={" + config.database 
                        + "};uid=" + config.username + ";PWD=" + config.password + ";";

var spaceTypes = {};
var spaceContents = {};

exports.run = function(data) {
  sql.open(conn_str, function(err, conn) {
    if (err) {
      logger.error('Can not connect to Sql server, Aborted!');
      return logger.error(err);
    }

    // 对space type做缓存
    conn.queryRaw("select * from ISIS.TD_SPACE_TYPE", function(err, results) {
      if (err) {
        logger.error('Can not open table ISIS.TD_SPACE_TYPE, Aborted!');
        return logger.log(err);
      } else {
        for (var i = 0; i < results.rows.length; i++) {
          var key = results.rows[i][1] + ' & ' + results.rows[i][6];
          spaceTypes[key] = results.rows[i][0];
        }

        // 对space content缓存
        conn.queryRaw("select * from ISIS.TD_SPACE_CONTENT", function(err, results) {
          if (err) {
            logger.error('Can not open table ISIS.TD_SPACE_CONTENT, Aborted!');
            return logger.log(err);
          } else {
            for (var i = 0; i < results.rows.length; i++) {
              var key = results.rows[i][1] + ' & ' + results.rows[i][10];
              spaceContents[key] = results.rows[i][0];
            }

            handle(conn, data, 0);
          }
        }) // space content cache
      }
    }) // space type cache
  }) //sql.open
}

function handle(conn, data, pos){

}

function handleZone(conn, record){
  // zone
  record.zone_spaceTypeId = spaceTypes['Zone & ' + record.accountId];
  conn.queryRaw("SELECT * from ISIS.TD_SPACE_INVENTORY WHERE tx_SpaceName=? AND in_AccountID=? AND in_SpaceTypeID=?", 
    [record.zone, record.accountId, record.zone_spaceTypeId], 
    function(err, results){
      if(err){
        logger.error('Line: ' + record.index + ' failed while deal with Zone field. Error detail: ')
        return logger.error(err);
      }

      if(results.rows.length > 0){
        logger.info('Line: ' + record.index + ' zone existed.');
      }else{
        var done = false;
        conn.queryRaw("INSERT INTO ISIS.TD_SPACE_INVENTORY(tx_SpaceName,in_AccountID,tf_Active,in_SpaceTypeID,tf_Inspectable,tf_HRS) VALUES(?,?,?,?,?,?)",
          [record.zone, record.accountId, record.active, record.zone_spaceTypeId, 1, 1],
          function(err, results){
            if(err){
              logger.error('Line: ' + record.index + ' failed while create zone. Error detail: ')
              return logger.error(err);
            }

            if(results.rowcount > 0 && !done){
              done = true;

              conn.queryRaw("SELECT @@identity", 
                function(err, results){
                  if(err){
                    logger.error('Line: ' + record.index + ' failed after new Zone created. Error detail: ')
                    return logger.error(err);
                  }

                  if(results.rows.length > 0){
                    record.zone_spaceId = results.rows[0][0];
                    logger.info('Line: ' + record.index + ' new zone created. id: ' + record.zone_spaceId);
                  }else{
                    logger.error('Line: ' + record.index + ' failed at creating new Zone. Contact Junhua.')
                  }
                }
              ) // conn.queryRaw 查询插入结果
            } // 插入成功，执行回调一次
              
          }
        ) // conn.queryRaw 插入
      }

  })  // conn.queryRaw 检查有没有这个记录
}