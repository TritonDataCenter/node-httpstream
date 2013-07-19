# httpstream: Reliable Readable stream for HTTP resources

httpstream provides a Readable stream interface (for Node 0.10 and later) that
abstracts over transient upstream failures by retrying requests from where it
left off.

httpstream requires Node 0.10 or later and the node-restify module.  restify is
not technically a dependency because the module doesn't "require" it directly.
Rather, callers pass in a constructed restify client.

## Example

    $ cat examples/basic.js
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

    $ node examples/basic.js 
    fetching ... 
    fetched 17562751 bytes

In classic "systems demo" fashion, the output's not interesting, but the fact
that it worked.


## Contributions

Pull requests should be "make prepush" clean.
