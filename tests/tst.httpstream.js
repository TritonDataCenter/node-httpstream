/*
 * tst.httpstream.js: exercise both common and edge cases of the httpstream.
 */

var mod_assert = require('assert');
var mod_crypto = require('crypto');
var mod_http = require('http');
var mod_path = require('path');

var mod_bunyan = require('bunyan');
var mod_vasync = require('vasync');
var mod_restify = require('restify');
var mod_verror = require('verror');

var VError = mod_verror.VError;

var mod_httpstream = require('../lib/httpstream');

/*
 * This test works by creating a custom HTTP server that we can cause to serve
 * data normally, interrupt transfers prematurely, or behave otherwise badly
 * (e.g., changing the underlying etag, or sending the wrong md5 checksum).
 * Each test case describes what the server should do in order to test the
 * desired behavior, as well as what the client should do (see the "error"
 * property):
 *
 *	chunks		Array of chunk sizes.  The server will emit data chunks
 *			of the given size and then close the TCP connection.  If
 *			there's more than one chunk, the server will expect the
 *			client to fetch the next chunk.
 *
 *			The special value 'change_etag' is also supported, which
 *			indicates that the etag value should be changed between
 *			chunks.
 *
 *	[md5]		Override value of the content-md5 header that the server
 *			will use.  (Used to test md5 mismatch.)
 *
 *	[error]		If present, the test case expects the client to emit an
 *			error matching the given regular expression.  If not,
 *			the client is expected to read the correct number of
 *			bytes and validate the md5sum.
 */
var test_cases = {
    'zero': {
	/* zero-byte resource */
	'chunks': [ 0 ]
    },
    'small': {
	/* small resource */
	'chunks': [ 137 ]
    },
    'no-md5': {
    	/* no md5 value given by server */
	'md5': null,
	'chunks': [ 5 * 1024 ]
    },
    'large': {
	/* resource that fills up small buffers */
	'chunks': [ 128 * 1024 * 1024 ]
    },
    'interrupted': {
	/*
	 * Resource where the server interrupts the transfer several times,
	 * including a few interrupted immediately.
	 */
	'chunks': [ 137, 1024, 10 * 1024 * 1024, 47, 0, 36, 0, 10 ]
    },
    'bad_md5': {
	'md5': 'deadbeef',
	'chunks': [ 5 * 1024 ],
	'error': /md5 mismatch: expected 'deadbeef', got/
    },
    'changed_etag': {
	'chunks': [
	    10 * 1024,
	    'change_etag',
	    1024
	],
	'error': /object changed while fetching \(etag mismatch\)/
    },
    'transient_500': {
	'chunks': [
	    128, 'error_500', 256, 'error_503', 128, 'error_500', 1024,
	        'error_503', 57
	]
    },
    'persistent_500': {
	'chunks': [
	    128, 'error_500', 'error_503', 'error_503', 517
	],
	'error': /ServiceUnavailableError/
    },
    'error_400': {
	'chunks': [ 'error_400', 128 ],
	'error': /BadRequestError/
    }
};
var test_state = {};	/* server-side state, indexed by test case name */
var log, server, address, client;

mod_vasync.pipeline({
    'funcs': [
	/* Set up global objects */
	function (_, callback) {
		log = new mod_bunyan({
		    'name': 'tst.httpstream.js',
		    'level': process.env['LOG_LEVEL'] || 'debug',
		    'serializers': {}
		});
		callback();
	},

	/* Start our mock HTTP server. */
	function (_, callback) {
		server = mod_http.createServer(handleRequest);
		server.listen(function () {
			address = server.address();
			log.info('server listening at %s:%d',
			    address['address'], address['port']);
			callback();
		});
	},

	/* Create a restify client for the server we just started. */
	function (_, callback) {
		client = mod_restify.createClient({
		    'url': 'http://' + address['address'] + ':' +
		        address['port'],
		    'log': log
		});
		callback();
	},

	/* Run the test cases in sequence. */
	function (_, callback) {
		var tests_to_run = process.argv.slice(2);
		if (tests_to_run.length === 0)
			tests_to_run = Object.keys(test_cases);
		var funcs = tests_to_run.map(function (k) {
			if (!test_cases.hasOwnProperty(k))
				throw (new VError(
				    'unknown test name: "%s"', k));
			return (runTestCase.bind(null, k, test_cases[k]));
		});
		mod_vasync.pipeline({ 'funcs': funcs }, callback);
	},

	/* Cleanup so that we can exit gracefully. */
	function (_, callback) {
		server.close();
		client.close();
		callback();
	}
    ]
}, function (err) {
	if (err) {
		log.fatal('TEST FAILED: %s', err);
		process.exit(1);
	}

	log.info('TEST PASSED');
});

/*
 * Runs a single test case "t" called "name".  See the global definition of
 * test_cases above.
 */
function runTestCase(name, t, _, callback)
{
	var stream, str, nbytes;

	stream = new mod_httpstream({
	    'client': client,
	    'path': '/' + name,
	    'log': log,
	    'highWaterMark': 1024 * 1024,
	    'retryPolicy': {
		'retries': 2,
		'minTimeout': 500,
		'maxTimeout': 2000
	    }
	});

	str = '';
	nbytes = 0;

	log.info('test "%s": start', name);

	/*
	 * Accumulate all the data in "str".  If there's an error, make sure it
	 * matches what we expect.  If there wasn't, make sure there wasn't
	 * supposed to be, and also make sure the data we received matched the
	 * expected md5sum.
	 */
	stream.on('data', function (chunk) {
		str += chunk.toString('utf8');
		nbytes += chunk.length;
		if (nbytes === chunk.length)
			log.debug('got chunk of %d bytes (%d total)',
			    chunk.length, nbytes);
	});

	stream.on('error', function (err) {
		if (t['error'] && t['error'].test(
		    err.name + ': ' + err.message)) {
			log.debug(err, 'test "%s": found expected error', name);
			callback();
			return;
		}

		if (t['error'])
			log.error(err, 'test "%s": found error, but not ' +
			    'the expected one (which was /%s/)', name,
			    t['error'].source);
		else
			log.error(err, 'test "%s": unexpected error', name);

		callback(new VError(err, 'unexpected error'));
	});

	stream.on('end', function () {
		if (t['error']) {
			log.error('test "%s": expected error, but none found',
			    name);
			callback(new VError('expected error'));
			return;
		}

		mod_assert.equal(str,
		    test_state[name]['raw'].toString('utf8'));
		log.info('test "%s": done: data matched up', name);
		callback();
	});
}

/*
 * Server-side function to handle requests.  The case under test is identified
 * by the URL.  The server roughly simulates what an HTTP server would do, with
 * additional functionality configured by the "chunks" and "md5" properties in
 * the test case definitions above.
 */
function handleRequest(req, res)
{
	var name, t;
	var expected_length, expected_range, i, clow, chigh;
	var buf, hasher;

	name = req.url.substr(1);
	mod_assert.ok(test_cases.hasOwnProperty(name));
	t = test_cases[name];

	log.debug('test server: request start', req.url, req.headers);

	/*
	 * Although in the real world the server wouldn't keep state about what
	 * requests the client is expected to make next, this testing server
	 * does so in order to test that the client is behaving as we expect.
	 * We identify the start of each client stream by the request that has
	 * no "Range" header.  The state is indexed by the test case name, which
	 * means we don't support multiple client streams for the same resource.
	 */
	if (!req.headers['range']) {
		/*
		 * This is the first request for this resource for this client
		 * stream.  Generate the data for this resource and compute the
		 * md5sum.
		 */
		mod_assert.ok(!test_state.hasOwnProperty(name));
		expected_length = 0;
		t['chunks'].forEach(function (c) {
			if (typeof (c) == 'number')
				expected_length += c;
		});

		log.debug('test server: building buffer');
		buf = new Buffer(expected_length);
		clow = 'a'.charCodeAt(0);
		chigh = 'z'.charCodeAt(0);
		for (i = 0; i < expected_length; i++)
			buf[i] = clow + (i % (chigh - clow));

		hasher = mod_crypto.createHash('md5');
		hasher.update(buf);
		test_state[name] = {
		    'etag': 'etag0',			/* resource etag */
		    'raw': buf,				/* raw resource data */
		    'md5sum': hasher.digest('base64'),	/* resource md5sum */
		    'nbytesread': 0,			/* client bytes read */
		    'nbytestotal': expected_length,	/* total bytes */
		    'next_chunk': 0			/* expected entry in */
							/* t['chunks'] */
		};
		log.debug('test server: buffer ready');
	} else {
		/*
		 * Again, this is overly rigid, but we're testing that our own
		 * client works the way we expect, so we don't need to handle
		 * all valid forms of the "Range" header.
		 */
		expected_range = 'bytes=' +
		    test_state[name]['nbytesread'] + '-';
		if (req.headers['range'] != expected_range) {
			log.error('test "%s": expected range header "%s", ' +
			    'but got "%s"', name, expected_range,
			    req.headers['range']);
			res.writeHead(400);
			res.end('client made the wrong "Range" request');
			return;
		}
	}

	fetchNext(req, res, t, test_state[name],
	    test_state[name]['nbytesread']);
}

/*
 * For test case "t", send the next chunk of the resource, which starts at
 * offset "offset".  See comments above on how the server is expected to behave.
 * This function implements the funky behavior under test, including sending
 * incomplete responses, bogus md5sums, and changed etags.
 */
function fetchNext(req, res, t, state, offset)
{
	var i, chunk, code, headers;

	i = state['next_chunk']++;
	mod_assert.ok(i < t['chunks'].length,
	    'client requested past the end of the object');

	chunk = t['chunks'][i];
	if (typeof (chunk) == 'string' && chunk.substr(0, 6) == 'error_') {
		code = parseInt(chunk.substr(6), 10);
		res.writeHead(code);
		res.end();
		return;
	}
	if (chunk == 'change_etag') {
		if (state['etag'] == 'etag0')
			state['etag'] = 'etag1';
		else
			state['etag'] = 'etag0';

		fetchNext(req, res, t, state, offset);
		return;
	}

	headers = {};
	mod_assert.equal(typeof (chunk), 'number');
	if (offset === 0) {
		code = 206;
		if (t['md5'])
			headers['content-md5'] = t['md5'];
		else
			headers['content-md5'] = state['md5sum'];
	} else {
		code = 200;
	}

	headers['content-length'] = state['nbytestotal'] - state['nbytesread'];
	headers['etag'] = state['etag'];
	res.writeHead(code, headers);
	res.end(state['raw'].slice(state['nbytesread'],
	    state['nbytesread'] + chunk));
	state['nbytesread'] += chunk;
	log.debug('test server: request completed', chunk, code, headers);
	req.socket.setTimeout(3000);
}
