#!/usr/bin/env node

/*
 * stress-abort.js: use HttpStream to fetch a URL in a loop.  With --abort,
 * aborts the stream when requested.  This is intended to exercise
 * stream.abort() of a stream in various states.
 */

var mod_assert = require('assert');
var mod_bunyan = require('bunyan');
var mod_cmdutil = require('cmdutil');
var mod_extsprintf = require('extsprintf');
var mod_fdleakcheck = require('fdleakcheck');
var mod_getopt = require('posix-getopt');
var mod_http = require('http');
var mod_restify = require('restify');
var mod_url = require('url');
var HttpStream = require('../lib/httpstream');
var VError = require('verror');
var printf = mod_extsprintf.printf;
var sprintf = mod_extsprintf.sprintf;

var frLog;
var frUrlRaw;
var frUrl;
var frClient;
var frStream;
var frByteCount = 0;
var frInfoSig = 'SIGUSR2';
var frAbortOnCreate = false;
var frAbortAfterEnd = false;
var frAbortAfterError = false;
var frAbortOnData = false;
var frAbortRoundRobin = false;
var frNumIterations = 300;
var frNumWarmup = 20;
var frNDone = 0;
var frMemoryHistory = [];
var frFdsInitial;

/*
 * main() parses command-line arguments, sets up persistent objects (like the
 * logger and HTTP client), and kicks off the first iteration.
 */
function main()
{
	var parser, option;

	mod_cmdutil.configure({
	    'usageMessage': 'Fetch a remote URL using the fetcher stream.  ' +
	        'The contents are ignored.',
	    'synopses': [ '[-a | --abort WHEN] URL' ]
	});

	parser = new mod_getopt.BasicParser('a:(abort)', process.argv);
	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
		case 'a':
			if (option.optarg == frInfoSig) {
				process.on(frInfoSig, function () {
					abortStream('caught ' + frInfoSig);
				});
			} else if (option.optarg == 'end') {
				frAbortAfterEnd = true;
			} else if (option.optarg == 'error') {
				frAbortAfterError = true;
			} else if (option.optarg == 'create') {
				frAbortOnCreate = true;
			} else if (option.optarg == 'data') {
				frAbortOnData = true;
			} else if (option.optarg == 'rotate') {
				frAbortRoundRobin = true;
			} else {
				/* XXX option for when response received? */
				mod_cmdutil.usage('options for --abort: ' +
				    [ 'end', 'error', 'create',
				    frInfoSig ].join(', '));
			}
			break;

		default:
			/* error message emitted by getopt */
			mod_cmdutil.usage();
			break;
		}
	}

	if (parser.optind() != process.argv.length - 1) {
		mod_cmdutil.usage('expected one URL');
	}

	mod_http.globalAgent.maxSockets = frNumIterations;
	frUrlRaw = process.argv[parser.optind()];
	frUrl = mod_url.parse(frUrlRaw);
	if (frUrl['protocol'] != 'http:' && frUrl['protocol'] != 'https:') {
		mod_cmdutil.usage('expected "http" or "https" protocol');
	}

	frLog = new mod_bunyan({
	    'name': 'stress-abort',
	    'src': true,
	    'level': process.env['LOG_LEVEL'] || 'warn'
	});

	/*
	 * It's somewhat incomplete to test with no HTTP agent, but this is
	 * necessitated by issue ceejbot/keep-alive-stream#2.
	 */
	frClient = mod_restify.createClient({
	    'agent': false,
	    'log': frLog,
	    'rejectUnauthorized': false,
	    'url': mod_url.format({
	        'protocol': frUrl['protocol'],
		'host': frUrl['host']
	    })
	});

	mod_assert.ok(frNumWarmup < frNumIterations,
	    '# of warmup iterations is configured too high');

	startDownloadIteration();
}

/*
 * Invoked like printf(), but prepends an ISO standard date string.  This is
 * separate from the bunyan logger because that's primarily intended to debug
 * production systems, whereas this tool is aimed at ad-hoc interactive use,
 * which is why we opt for the simpler plaintext format.
 */
function log()
{
	var args = Array.prototype.slice.call(arguments);
	var msg = sprintf.apply(null, args);
	printf('%s: %s\n', new Date().toISOString(), msg);
}

/*
 * Instantiates a new HttpStream and counts the bytes read.  If abort has been
 * requested on error or end, this function will trigger abortStream() to be
 * called at the appropriate time.  Either way, it ends up calling restart() to
 * start another iteration.
 */
function startDownloadIteration()
{
	var stream, which, aborted;

	if (frAbortRoundRobin) {
		frAbortAfterEnd = false;
		frAbortAfterError = false;
		frAbortOnCreate = false;
		frAbortOnData = false;

		which = frNDone % 5;
		switch (which) {
		case 0:	frAbortAfterEnd = true;
			log('next abort: after end');
			break;
		case 1: frAbortAfterError = true;
			log('next abort: after error');
			break;
		case 2: frAbortOnCreate = true;
			log('next abort: after create');
			break;
		case 3: frAbortOnData = true;
			log('next abort: on data');
			break;
		default:
			log('next abort: skipped');
			break;
		}
	}

	frByteCount = 0;
	aborted = false;

	frStream = stream = new HttpStream({
	    'client': frClient,
	    'path': frUrl['path'],
	    'log': frLog,
	    'highWaterMark': 10 * 1024 * 1024
	});

	frStream.on('data', function (chunk) {
		if (stream != frStream)
			return;
		frByteCount += chunk.length;
		if (frAbortOnData && !aborted) {
			aborted = true;
			abortStream('after data');
		}
	});

	frStream.on('end', function () {
		if (stream != frStream)
			return;
		log('event: end (read %d bytes)', frByteCount);
		if (frAbortAfterEnd) {
			setImmediate(abortStream, 'after end');
		} else {
			restart();
		}
	});

	frStream.on('error', function (err) {
		if (stream != frStream)
			return;
		log('event: error: %s', err.message);
		if (frAbortAfterError) {
			setImmediate(abortStream, 'after error');
		}
	});

	frStream.on('close', function () {
		if (stream != frStream)
			return;
		log('event: close');
	});

	log('stream created');

	if (frAbortOnCreate) {
		abortStream('on create');
	}
}

/*
 * If we're in the middle of an iteration, abort the stream and start another
 * iteration.  Otherwise, do nothing.
 */
function abortStream(when)
{
	if (frStream === undefined) {
		log('would abort (%s), but not running', when);
		return;
	}

	log('aborting (%s, read %d bytes so far)', when, frByteCount);
	frStream.abort();
	restart();
}

/*
 * Records a sample of memory usage and then starts another iteration.
 */
function restart()
{
	var usage;

	frStream = undefined;
	usage = process.memoryUsage();
	log('sample %d: %s', ++frNDone, JSON.stringify(usage));
	frMemoryHistory.push(usage);

	if (frNDone == 1) {
		/*
		 * We defer the first fd listing to after the first iteration so
		 * that we catch lazily-opened file descriptors (like the nscd
		 * door).
		 */
		mod_fdleakcheck.snapshot(function (err, snapshot) {
			if (err)
				mod_cmdutil.fail(err);

			frFdsInitial = snapshot;
			startDownloadIteration();
		});
	} else if (frNDone == frNumIterations) {
		log('completed %d iterations: running final checks', frNDone);
		finalFdLeakCheck();
	} else {
		log('starting again in 50ms');
		setTimeout(startDownloadIteration, 50);
	}
}

function finalFdLeakCheck()
{
	log('starting fd leak check');
	frClient.close();
	mod_fdleakcheck.snapshot(function (err, snapshot) {
		if (err)
			mod_cmdutil.fail(err);

		if (frFdsInitial.differs(snapshot)) {
			log('fds leaked!');
			log('fds before:');
			printf('%s\n', frFdsInitial.openFdsAsString());
			log('fds after:');
			printf('%s\n', snapshot.openFdsAsString());
		} else {
			log('no fd leaks detected');
		}

		finish();
	});
}

/*
 * Print a final summary of memory usage.
 */
function finish()
{
	var rss_high, total_high, used_high;
	var rss_high_which, total_high_which, used_high_which;
	var rss_low, total_low, used_low;
	var rss_low_which, total_low_which, used_low_which;

	mod_assert.equal(frMemoryHistory.length, frNDone);
	mod_assert.ok(frMemoryHistory.length > frNumWarmup);

	rss_low_which = rss_high_which = used_low_which =
	    used_high_which = total_low_which = total_high_which = frNumWarmup;
	rss_low = rss_high = frMemoryHistory[frNumWarmup].rss;
	used_low = used_high = frMemoryHistory[frNumWarmup].heapUsed;
	total_low = total_high = frMemoryHistory[frNumWarmup].heapTotal;

	frMemoryHistory.forEach(function (mem, i) {
		if (i < frNumWarmup)
			return;

		if (mem.rss < rss_low) {
			rss_low = mem.rss;
			rss_low_which = i;
		} else if (mem.rss > rss_high) {
			rss_high = mem.rss;
			rss_high_which = i;
		}

		if (mem.heapUsed < used_low) {
			used_low = mem.heapUsed;
			used_low_which = i;
		} else if (mem.used > used_high) {
			used_high = mem.heapUsed;
			used_high_which = i;
		}

		if (mem.heapTotal < total_low) {
			total_low = mem.heapTotal;
			total_low_which = i;
		} else if (mem.total > total_high) {
			total_high = mem.heapTotal;
			total_high_which = i;
		}
	});

	log('all memory samples');
	printf('%3s  %10s  %10s  %10s\n', '#', 'RSS', 'HEAP USED',
	    'HEAP TOTAL');
	frMemoryHistory.forEach(function (mem, i) {
		printf('%3d  %10d  %10d  %10d\n', i, mem.rss, mem.heapUsed,
		    mem.heapTotal);
	});

	log('memory watermarks (ignoring first %d samples)', frNumWarmup);
	printf('%-11s  %-27s  %-27s\n', 'METRIC', 'LOW', 'HIGH');
	printf('%-11s  %9d bytes (sample %2d)  %9d bytes (sample %2d)\n',
	    'RSS:', rss_low, rss_low_which + 1, rss_high, rss_high_which + 1);
	printf('%-11s  %9d bytes (sample %2d)  %9d bytes (sample %2d)\n',
	    'HEAP USED:', used_low, used_low_which + 1, used_high,
	    used_high_which + 1);
	printf('%-11s  %9d bytes (sample %2d)  %9d bytes (sample %2d)\n',
	    'HEAP TOTAL:', total_low, total_low_which + 1, total_high,
	    total_high_which + 1);
}

main();
