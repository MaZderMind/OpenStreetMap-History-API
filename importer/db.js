var cp = require('child_process');
var pq = require('pg');
var util = require('./util');

function Db()
{
	this.nodeStreamCommands = [
		'BEGIN TRANSACTION', 
		
		'DROP TABLE IF EXISTS nodes', 
		
		'CREATE TABLE nodes ( \
			id BIGINT, \
			version INT, \
			visible BOOL, \
			user_id BIGINT, \
			timestamp TIMESTAMP WITHOUT TIME ZONE, \
			changeset BIGINT, \
			tags HSTORE, \
			lat FLOAT, \
			lon FLOAT \
		) WITHOUT OIDS', 
		
		'COPY nodes FROM STDIN', 
		
		'COMMIT', 
		
		'ALTER TABLE nodes ADD PRIMARY KEY (id, version)', 
		
		'VACUUM ANALYZE'
	]
	
	this.wayStreamCommands = [
		'BEGIN TRANSACTION', 
		
		'DROP TABLE IF EXISTS ways', 
		
		'CREATE TABLE ways ( \
			id BIGINT, \
			version INT, \
			visible BOOL, \
			user_id BIGINT, \
			timestamp TIMESTAMP WITHOUT TIME ZONE, \
			changeset BIGINT, \
			tags HSTORE \
		) WITHOUT OIDS', 
		
		'COPY ways FROM STDIN', 
		
		'COMMIT', 
		
		'ALTER TABLE ways ADD PRIMARY KEY (id, version)', 
		
		'VACUUM ANALYZE'
	]
	
	this.wayNodesStreamCommands = [
		'BEGIN TRANSACTION', 
		
		'DROP TABLE IF EXISTS waynodes', 
		
		'CREATE TABLE waynodes ( \
			wayid BIGINT, \
			wayversion INT, \
			nodeid BIGINT, \
			nodeversion INT \
		) WITHOUT OIDS', 
		
		'COPY waynodes FROM STDIN', 
		
		'COMMIT', 
		
		'ALTER TABLE ways ADD PRIMARY KEY (wayid, wayversion)', 
		
		'VACUUM ANALYZE'
	]
}

function writeRow(cols)
{
	var tRe = /\t/g, nRe = /\n/g, qRe = /"/g;
	
	var line = [];
	cols.forEach(function(col) {
		switch(typeof col)
		{
			case 'number':
				return line.push(col);
			
			case 'string':
				return line.push(col.replace(tRe, '\\t').replace(nRe, '\\n'));
			
			case 'boolean':
				return line.push(col ? '1' : '0');
			
			case 'undefined':
				return line.push('\\N');
			
			case 'object':
				switch(true)
				{
					case col == null:
						return line.push('\\N');
					
					case col instanceof Date:
						return line.push('1970-00-10');
					
					default:
						var hstore = [];
						for(k in col)
						{
							var hk = k.replace(qRe, '\\\\"');
							var hv = col[k].replace(qRe, '\\\\"');
							var item = '"'+hk+'"=>"'+hv+'"';
							
							hstore.push(item.replace(tRe, '\\t').replace(nRe, '\\n'));
						}
						if(hstore.length > 0)
							return line.push(hstore.join(','));
						else
							return line.push('\\N');
				}
			
			default:
				console.warn('cant write %s var to db', typeof col);
		}
	});
	
	var str = line.join('\t')+'\n';
	this.write(str);
}

Db.prototype.prepareStream = function(proc)
{
	proc.stdin.writeRow = writeRow;
	proc.stdin.committed = false;
	proc.stdin.commit = function(cb) {
		proc.once('exit', function()
		{
			proc.stdin.committed = true;
			cb();
		});
		
		proc.stdin.end();
	};
	
	proc.stderr.on('data', function(error)
	{
		//console.warn(error); // stderr of the nodes' psql command
	});
	proc.stderr.setEncoding('utf-8');
	proc.stderr.resume();
	
	return proc.stdin;
}

Db.prototype.start = function(con, cb)
{
	this.nodeProc = cp.spawn('psql', [con.db, '-c', this.nodeStreamCommands.join(';')]);
	this.nodeStream = this.prepareStream(this.nodeProc);
	
	this.wayProc = cp.spawn('psql', [con.db, '-c', this.wayStreamCommands.join(';')]);
	this.wayStream = this.prepareStream(this.wayProc);
	
	this.wayNodesProc = cp.spawn('psql', [con.db, '-c', this.wayNodesStreamCommands.join(';')]);
	this.wayNodesStream = this.prepareStream(this.wayNodesProc);
	
	cb();
}

Db.prototype.isFinished = function()
{
	return this.nodeStream.committed && this.wayStream.committed && this.wayNodesStream.committed;
}

Db.prototype.finish = function(cb)
{
	var db = this;
	
	if(db.isFinished())
		return cb();
	
	if(!this.nodeStream.committed) 
	{
		console.log('nodeStream not yet comitted, committing..');
		this.nodeStream.commit(function()
		{
			console.log('nodeStream finished');
			if(db.isFinished())
				return cb();
			
		});
	}
	
	if(!this.wayStream.committed)
	{
		console.log('wayStream not yet comitted, committing..');
		this.wayStream.commit(function()
		{
			console.log('wayStream finished');
			if(db.isFinished())
				return cb();
			
		});
	}
	
	if(!this.wayNodesStream.committed)
	{
		console.log('wayNodesStream not yet comitted, committing..');
		this.wayNodesStream.commit(function()
		{
			console.log('wayNodesStream finished');
			if(db.isFinished())
				return cb();
			
		});
	}
}

module.exports = new Db();

