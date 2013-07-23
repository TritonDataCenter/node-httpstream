/*
 * examples/basic.js: basic HttpStream example
 */

var mod_restify = require('restify');
var mod_bunyan = require('bunyan');

var HttpStream = require('../lib/httpstream');

var log = new mod_bunyan({
    'name': 'example',
    'level': 'warn',
    'serializers': {}
});

var client = mod_restify.createClient({
    'url': 'https://us-east.manta.joyent.com',
    'log': log,
});

var stream = new HttpStream({
    'client': client,
    'path': '/manta/public/sdks/joyent-node-latest.pkg',
    'log': log,
    'highWaterMark': 10 * 1024 * 1024
});

var sum = 0;

console.log('fetching ... ');
stream.on('data', function (chunk) { sum += chunk.length; });
stream.on('end', function () {
	console.log('fetched %d bytes', sum);
	client.close();
});
