var events = require("events");
var expat = require('node-expat');
var util = require('./util');

// the parser object
function Parser(stream)
{
	// reading stream
	this.stream = stream;
	var xml = this.xml = new expat.Parser();
	
	// the currently handled element
	this.el = {};
	
	// the usermap
	this.userMap = {};
	
	// init the EventEmitter
	events.EventEmitter.call(this);
	
	
	// data from the stream -> send to xml parser
	stream.on('data', function(chunk)
	{
		xml.parse(chunk);
	});
	
	// xml elements -> send to handler
	xml.on('startElement', util.createDelegate(this.startElement, this));
	xml.on('endElement', util.createDelegate(this.endElement, this));
}

// parser is also an EventEmitter
util.inherits(Parser, events.EventEmitter);

// start the parsing
Parser.prototype.start = function()
{
	this.stream.resume();
}

// resume the parsing
Parser.prototype.resume = function()
{
	this.xml.resume();
}

// pause the parsing
Parser.prototype.stop = function()
{
	this.xml.stop();
}

// a starting xml element
Parser.prototype.startElement = function(tag, attr)
{
	if(tag == 'tag')
	{
		var k = attr.k, v = attr.v;
		
		this.el.tags[k] = v;
		return;
	}
	
	if(tag == 'nd')
	{
		return this.el.nodes.push(attr.ref);
	}
	
	if(tag == 'member')
	{
		return this.el.members.push({
			type: attr.type, 
			ref: parseInt(attr.ref), 
			role: attr.role
		});
	}
	
	if(tag == 'node' || tag == 'way' || tag == 'relation')
	{
		this.el = {
			id: parseInt(attr.id), 
			user: attr.user, 
			uid: parseInt(attr.uid), 
			visible: attr.visible == 'true', 
			version: parseInt(attr.version), 
			changeset: parseInt(attr.changeset), 
			timestamp: attr.timestamp, 
			visible: attr.visible != 'false', 
			tags: {}
		};
		
		this.userMap[this.el.uid] = this.el.user;
		
		if(tag == 'node')
		{
			this.el.lat = parseFloat(attr.lat);
			this.el.lon = parseFloat(attr.lon);
		}
		
		else if(tag == 'way')
		{
			this.el.nodes = [];
		}

		else if(tag == 'relation')
		{
			this.el.members = [];
		}
	}
}

// a ending xml element
Parser.prototype.endElement = function(tag)
{
	if(tag == 'node')
	{
		return this.emit('node', this.el);
	}

	if(tag == 'way')
	{
		return this.emit('way', this.el);
	}

	if(tag == 'relation')
	{
		return this.emit('relation', this.el);
	}
	
	if(tag == 'osm')
	{
		for(uid in this.userMap)
			this.emit('user', uid, this.userMap[uid]);
		
		return this.emit('end');
	}
}

// default parser uses stdin
module.exports = new Parser(process.stdin);

