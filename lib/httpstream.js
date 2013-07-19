/*
 * httpstream.js: Readable stream for HTTP resources that retries on transient
 * upstream failures.
 */

var mod_assert = require('assert');
var mod_crypto = require('crypto');
var mod_util = require('util');
var mod_readable = require('readable-stream');

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
 */
function ReliableHttpStream(args)
{
	mod_assert.equal('object', typeof (args['client']),
	    '"client" arg must be a restify client');
	mod_assert.equal('string', typeof (args['path']),
	    '"path" arg must be a string');
	mod_assert.equal('object', typeof (args['log']),
	    '"log" arg must be a restify log');
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
	this.rs_request_pending = false;
	this.rs_error = null;
	this.rs_reading = false;	/* read in progress */

	/* populated from first response headers */
	this.rs_exp_md5 = null;
	this.rs_exp_len = null;
	this.rs_exp_etag = null;

	mod_readable.Readable.call(this,
	    { 'highWaterMark': args['highWaterMark'] });
}

mod_util.inherits(ReliableHttpStream, mod_readable.Readable);

ReliableHttpStream.prototype._read = function ()
{
	var s = this;
	var rqoptions;

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

	rqoptions = {
	    'path': this.rs_path
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
			s.rs_reading = false;
			s.internalError(err);
			return;
		}

		s.rs_log.debug('read: request start');
		s.rs_request = req;
		s.rs_request_pending = false;

		req.on('result', function (err2, res) {
			if (err2) {
				/* XXX backoff and retry on 50x */
				s.rs_reading = false;
				s.internalError(err2);
				return;
			}

			s.rs_response = res;
			s.rs_log.debug('response: code = %d, ' +
			    'x-server-name=%s, req_id=%s', res.statusCode,
			    res.headers['x-server-name'] || '<unknown>',
			    res.headers['x-request-id'] || '<unknown>');

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
				s.rs_exp_len = res.headers['content-length'];
				s.rs_exp_etag = res.headers['etag'];
				s.rs_log.debug('content-length = %d, etag = %s',
				    s.rs_exp_len, s.rs_exp_etag);
			}

			s.rs_response.on('end', function () {
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

	if (this.rs_exp_len != this.rs_nbytesread) {
		this.rs_log.debug('bytes read (%d) is less than expected (%d)' +
		    ' (initiating retry)', this.rs_nbytesread, this.rs_exp_len);
		this.rs_response.removeAllListeners('readable');
		this.rs_request = null;
		this.rs_response = null;
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

	this.emit('end');
};

ReliableHttpStream.prototype.pump = function ()
{
	var s = this;
	var source = this.rs_response;
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

	if (this.rs_request)
		this.rs_request.abort();

	if (this.rs_response)
		this.rs_response.destroy();
};
