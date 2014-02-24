var csv = require('csv'),
	config = require('./config'),
	logger = require('./logger'),
	importer = require('./importer'),
	C_ZONE = 0,
	C_BUILDDING = 1,
	C_FLOOR = 2,
	C_ROOM = 3,
	C_ROOM_TYPE = 4,
	C_FLOOR_TYPE = 5,
	C_ACTIVE = 6,
	C_SQM = 7,
	C_LENGTH = 8,
	C_WIDTH = 9,
	C_FIXTRUES = 10,
	C_BEDS = 11,
	C_ACCOUNTID=12;

csv()
	.from.path(config.file_path, { delimiter: ',', escape: '"' })
	.on('error', function(error){
	  	logger.error(error.message);
	})
	.to.array(function(data_in_array) {
		var data = [];
		var hasError = false;
		for(var i = 1, length = data_in_array.length; i < length; ++i){
			var r = {
				index: i,
				zone: data_in_array[i][C_ZONE],
				building: data_in_array[i][C_BUILDDING],
				floor: data_in_array[i][C_FLOOR],
				room: data_in_array[i][C_ROOM],
				roomType: data_in_array[i][C_ROOM_TYPE],
				floorType: data_in_array[i][C_FLOOR_TYPE],
				active: data_in_array[i][C_ACTIVE],
				sqm: data_in_array[i][C_SQM],
				length: data_in_array[i][C_LENGTH],
				width: data_in_array[i][C_WIDTH],
				fixtures: data_in_array[i][C_FIXTRUES],
				beds: data_in_array[i][C_BEDS],
				accountId: data_in_array[i][C_ACCOUNTID]
			}

			if(!validate(r)) hasError = true;

			data.push(r);
		}

		if(hasError){
			logger.error('Please fix all import data.');
		}
		else{
			logger.info('Starting to import data...... totle lines: ' + data.length);
			importer.run(data);
		}
	});

// 数据校验&转换
function validate(record){
	if(record.active.toLowerCase() == 'yes')
		record.active = 1;
	else if(record.active.toLowerCase() == 'no')
		record.active = 0;
	else {
		logger.error('Line ' + record.index + ' field ACTIVE is set invalid value: ' + record.active);
		return false;
	}

	var sqm = Number(record.sqm);
	if( isNaN(sqm) ){
		logger.error('Line ' + record.index + ' field SQM is set invalid value: ' + record.sqm);
		return false;		
	}
	record.sqm = sqm;

	var accountId = Number(record.accountId);
	if( isNaN(accountId) ){
		logger.error('Line ' + record.index + ' field AccountID is set invalid value: ' + record.accountId);
		return false;		
	}
	record.accountId = accountId;

	return true;
}