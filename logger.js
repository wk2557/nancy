var winston = require('winston')
  , fs = require('fs')
  , logdir = __dirname + '/logs'

if(!fs.existsSync(logdir)) fs.mkdirSync(logdir);

var logger = new (winston.Logger)({
	transports: [
		new (winston.transports.Console)({ level: 'info'}),
		new (winston.transports.File)({
			filename: logdir + '/out.log',
			maxFiles: 5,
			maxsize: 50 * 1024 * 1024,			// 50M
			json: false
		})
	]
});

module.exports = logger;