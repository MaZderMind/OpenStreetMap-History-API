var config = require('./config');
var reader = require('./reader');
var db = require('./db');

reader.on('node', function(node)
{
	console.log('node');
	db.nodeStream.writeRow([
		node.id, 
		node.version, 
		node.visible, 
		node.uid, 
		node.timestamp, 
		node.changeset, 
		node.tags, 
		node.lat, 
		node.lon
	]);
});

reader.once('way', function(way)
{
	console.log('committing nodeStream');
	
	reader.stop();
	db.nodeStream.commit(function()
	{
		reader.on('way', function(way)
		{
			console.log('way', way.id);
		});
		
		console.log('way', way.id);
		reader.resume();
	});
});

reader.on('relation', function(relation)
{
	console.log('relation', relation);
});

reader.on('user', function(uid, user)
{
	console.log('user', uid, user);
});


// connect & init the db clients
console.log('starting db');
db.start(config.con, function()
{
	// when the parser finishes	
	reader.once('end', function()
	{
		console.log('parser finished, finishing db');
		db.finish(function()
		{
			console.log('db finished');
		});
	});

	// start parsing from stdin
	console.log('starting reader');
	reader.start();
});

