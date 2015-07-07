/*
 * httpstream.js: Readable stream for HTTP resources that retries on transient
 * upstream failures.
 */

var mod_assert = require('assert');
var mod_crypto = require('crypto');
var mod_util = require('util');
var mod_stream = require('stream');
var mod_retry = require('retry');

/*
 * For 0.8, require the special readable-stream module for Readable.
 */
if (!mod_stream.Transform)
	mod_stream = require('readable-stream');

var mod_verror = require('verror');
var VError = mod_verror.VError;

/* Public interface */
module.exports = ReliableHttpStream;

/*
 * A ReliableHttpStream is a readable stream for a given HTTP resource that
 * abstracts over transient failures of the underlying connection (including
 * those resulting from unexpected upstream connection closes) by issuing
 * subsequent requests for the same resource from where we left off.
 *
 * Arguments include:
 *
 *     client		restify HTTP client
 *
 *     path		resource to fetch from upstream
 *
 *     log		bunyan-style logger
 *
 *     highWaterMark	See Node API docs
 *
 *     retryPolicy	If specified, this should specify a node-retry-like
 *     			retry policy, which will be used for retrying connection
 *     			failures and 500 errors.  If unspecified, such failures
 *     			will be retried a few times, for up to a few seconds
 *     			(which is deliberately vague).  Non-transient errors
 *     			(e.g., 400 errors) will never be retried, and premature
 *     			closes from the server will always be retried
 *     			immediately.
 */
function ReliableHttpStream(args)
{
	mod_assert.equal('object', typeof (args['client']),
	    '"client" arg must be a restify client');
	mod_assert.equal('string', typeof (args['path']),
	    '"path" arg must be a string');
	mod_assert.equal('object', typeof (args['log']),
	    '"log" arg must be a bunyan log');
	mod_assert.equal('number', typeof (args['highWaterMark']),
	    '"highWaterMark" arg must be a number');

	this.rs_log = args['log'];
	this.rs_path = args['path'];
	this.rs_client = args['client'];

	/* runtime state */
	this.rs_nbytesread = 0;		/* number of bytes consumed */
	this.rs_nresumes = 0;		/* number of resume attempts */
	this.rs_md5 = mod_crypto.createHash('md5');
	this.rs_request = null;
	this.rs_response = null;
	this.rs_source = null;
	this.rs_aborted = null;
	this.rs_request_pending = false;
	this.rs_error = null;
	this.rs_reading = false;	/* read in progress */
	this.rs_retrypolicy = args['retryPolicy'] || {
	    'retries': 3,
	    'minTimeout': 1000,
	    'maxTimeout': 10000
	};
	this.rs_retry = null;

	/* populated from first response headers */
	this.rs_exp_md5 = null;
	this.rs_exp_len = null;
	this.rs_exp_etag = null;

	mod_stream.Readable.call(this,
	    { 'highWaterMark': args['highWaterMark'] });
}

mod_util.inherits(ReliableHttpStream, mod_stream.Readable);

ReliableHttpStream.prototype._read = function ()
{
	var s = this;

	if (this.rs_aborted !== null) {
		this.rs_log.warn('ignoring _read() called after aborted');
		return;
	}

	if (this.rs_error !== null) {
		this.rs_log.warn('ignoring _read() called after error');
		return;
	}

	if (this.rs_reading) {
		this.rs_log.warn('_read(): already reading');
		return;
	}

	this.rs_reading = true;

	if (this.rs_response !== null) {
		this.pump();
		return;
	}

	if (this.rs_request_pending)
		/* There's already a request pending. */
		return;

	this.rs_retry = new mod_retry.operation(this.rs_retrypolicy);
	this.rs_retry.attempt(function () { s.makeRequest(); });
};

ReliableHttpStream.prototype.makeRequest = function ()
{
	var s = this;
	var rqoptions;

	mod_assert.ok(this.rs_request_pending === false);

	if (this.rs_aborted !== null) {
		this.rs_log.debug('read: bailing out due to abort');
		this.rs_reading = false;
		return;
	}

	rqoptions = {
	    'path': this.rs_path,
	    'retry': this.rs_retrypolicy
	};

	/*
	 * We don't supply the "Range" header for the first request because the
	 * server won't try to serve us the content-md5 value if Range is
	 * specified.
	 */
	if (this.rs_nbytesread > 0) {
		rqoptions['headers'] = {
		    'range': 'bytes=' + this.rs_nbytesread + '-'
		};
	}

	this.rs_log.debug('read: initiating request', rqoptions);
	this.rs_request_pending = true;
	this.rs_client.get(rqoptions, function (err, req) {
		if (err) {
			/*
			 * Retries are automatically handled by the restify
			 * client according to the configured policy, so this
			 * indicates an error that's fatal to the stream.
			 */
			s.rs_reading = false;
			s.internalError(err);
			return;
		}

		s.rs_log.debug('read: request start');
		s.rs_request = req;
		s.rs_request_pending = false;

		if (s.rs_aborted !== null) {
			s.rs_log.debug('read: aborting request that was ' +
			    'pending when abort() called');
			s.stopAndCleanUp();
			return;
		}

		req.on('result', function (err2, res) {
			if (err2 &&
			    (err2.statusCode === undefined ||
			    err2.statusCode >= 500) &&
			    s.rs_retry.retry(err2)) {
				s.rs_log.warn(err2, 'found error, will retry');
				s.rs_request = null;
				return;
			}

			if (err2) {
				s.rs_reading = false;
				s.internalError(err2);
				return;
			}

			s.rs_response = res;
			if (!res.read) {
				/* pre-0.10 stream */
				s.rs_source = new mod_stream.Readable();
				s.rs_source.wrap(res);
			} else {
				s.rs_source = res;
			}

			s.rs_log.debug({
			    'statusCode': res.statusCode,
			    'x-server-name': res.headers['x-server-name'],
			    'x-request-id': res.headers['x-request-id']
			}, 'response');

			if (s.rs_exp_etag !== null &&
			    s.rs_exp_etag !== res.headers['etag']) {
				s.rs_reading = false;
				s.internalError(new VError(
				    'object changed while fetching ' +
				    '(etag mismatch)'));
				return;
			}

			if (s.rs_exp_len === null) {
				s.rs_exp_md5 = res.headers['content-md5'] ||
				    null;
				s.rs_exp_len = parseInt(
				    res.headers['content-length'], 10) || 0;
				s.rs_exp_etag = res.headers['etag'] || null;
				s.rs_log.debug({
				    'content-length': s.rs_exp_len,
				    'etag': s.rs_exp_etag,
				    'md5': s.rs_exp_md5
				}, 'response details');
			}

			s.rs_source.on('end', function () {
				mod_assert.ok(s.rs_response === res);
				s.responseEnd();
			});

			s.pump();
		});
	});
};

ReliableHttpStream.prototype.responseEnd = function ()
{
	this.rs_log.debug('read "end" after %d bytes', this.rs_nbytesread);

	if (this.rs_exp_len > this.rs_nbytesread) {
		this.rs_log.debug('bytes read (%d) is less than expected (%d)' +
		    ' (initiating retry)', this.rs_nbytesread, this.rs_exp_len);
		this.rs_source.removeAllListeners('readable');
		this.rs_request = null;
		this.rs_response = null;
		this.rs_source = null;
		this.rs_reading = false;
		this._read();
		return;
	}

	var calcmd5 = this.rs_md5.digest('base64');
	if (this.rs_exp_md5 !== null) {
		if (this.rs_exp_md5 != calcmd5) {
			this.internalError(new VError('md5 mismatch: ' +
			    'expected %j, got %j', this.rs_exp_md5, calcmd5));
			return;
		}

		this.rs_log.debug('md5 matched');
	}

	this.push(null);
};

ReliableHttpStream.prototype.pump = function ()
{
	var s = this;
	var source = this.rs_source;
	var buf;

	/*
	 * The Node docs suggest that if push() returns true, we could keep
	 * reading, but the push() itself calls _read() back, so we don't
	 * bother.
	 */
	buf = source.read();
	if (buf === null) {
		this.rs_log.trace('read null; waiting for more data');
		source.once('readable', function () {
			s.rs_log.trace('source readable');
			s.pump();
		});
		return;
	}

	this.rs_log.trace('read %d bytes from source', buf.length);
	this.rs_nbytesread += buf.length;
	this.rs_md5.update(buf);
	this.rs_reading = false;
	this.push(buf);
};

ReliableHttpStream.prototype.internalError = function (err)
{
	this.rs_error = err;
	this.rs_log.error(err);
	this.emit('error', this.rs_error);
	this.stopAndCleanUp();
};

ReliableHttpStream.prototype.stopAndCleanUp = function ()
{
	mod_assert.ok(this.rs_error || this.rs_aborted);
	if (this.rs_request)
		this.rs_request.abort();
	if (this.rs_response)
		this.rs_response.destroy();
};

ReliableHttpStream.prototype.abort = function ()
{
	if (this.rs_aborted !== null)
	        return;

	this.rs_aborted = new Date();
	this.rs_log.info('aborted');
	this.stopAndCleanUp();
};
