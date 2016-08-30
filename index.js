var fs = require('fs'),
    path = require('path'),
    elasticsearch = require('elasticsearch'),
    q = require('q'),
    log4js = require('log4js'),
    log = log4js.getLogger('main'),
    argv = require('minimist')(process.argv.slice(2)),
    readdir = q.denodeify(fs.readdir),
    readfile = q.denodeify(fs.readFile),
    stat = q.denodeify(fs.stat);

const BUFFER_MAX_SIZE = 50;

var getNumCacheHist = function(entries) {
    var hits = entries.filter(function(e) {
        var headers = e.response.headers;
        var hits = headers.filter(function(h) {
            return (h.name === "X-Cache" && h.value.split(" ")[0].substr(-3) === "HIT") ||
                (h.name === "CF-Cache-Status" && h.value === "HIT");
        });
        return hits.length > 0;
    });
    return hits.length;
};

var parseHar = function(route, data) {
    var numhits = getNumCacheHist(data.har.log.entries);
    return {
        status: data.status,
        route: route,
        date: data.har.log.pages[0].startedDateTime,
        onload: data.har.log.pages[0].pageTimings.onLoad,
        numhits: numhits
    };
};

var processFiles = function(dirpath, files) {
    log.info("processing " + files.length + " files");
    var done = q();
    for(var file of files) {
        done = done.then(function(file) {
            return readfile(dirpath + path.sep + file, 'utf8')
            .then(function(data) {
                var doc = parseHar(argv.route, JSON.parse(data));
                return pushDocument(doc);
            });
        }.bind(this, file));
    }
    return done;
};

function pad(n) { return n < 10 ? '0' + n : n; }
var docBuffer = [];
var pushDocument = function(doc) {
    docBuffer.push(doc);
    if(docBuffer.length < BUFFER_MAX_SIZE) {
        return;
    }
    return flushDocumentBuffer();
};

var flushDocumentBuffer = function() {
    if(docBuffer.length === 0) {
        return;
    }
    var tmp = docBuffer;
    docBuffer = [];
    log.info("flushing " + tmp.length + " documents");

    var body = [];
    for(var d of tmp) {
        var date = new Date(Date.parse(d.date));
        d.date = date;
        var index = "pwt-" + date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate());
        body.push({ index: {_index: index, _type: "pageload" }});
        body.push(d);
    }
    //log.debug(body);
    return q.ninvoke(es, "bulk", { body: body });
};

var printUsage = function(e) {
    log.info("Usage: node " + __filename + " --route <route-name> <directory>");
    log.info(" route-name  This string is added to each document create in ES from har files");
    log.info("  directory  Directory is scanned recursively for har files");
    if(e) {
        process.exit(e === true ? 0 : e);
    }
};

if(typeof argv.route === "undefined") {
    log.error("Argument 'route' required!");
    printUsage(1);
}

if(argv._.length != 1) {
    log.error("Provide one directory as argument!");
    printUsage(1);
}

var es = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

var readrecursive = function(dirname) {
    log.debug("reading " + dirname);
    return readdir(dirname)
    .then(function(files) {
        var fstats = {};
        var done = q();
        for(var fn of files) {
            done = done.then(function(fn) {
                return stat(dirname + path.sep + fn)
                .then(function(statres) {
                    fstats[fn] = statres;
                });
            }.bind(this, fn));
        }
        return done.then(function() {
            return fstats;
        });
    }, function(err) {
        log.error("failed to read directory " + dirname, err);
    })
    .then(function(fstats) {
        var dirs = [];
        var files = [];

        for(var fn in fstats) {
            if(fstats[fn].isDirectory()) {
                dirs.push(fn);
            } else if(fstats[fn].isFile() && fn.substr(-4) == ".har") {
                files.push(fn);
            }
        }

        var done = []
        for(var dir of dirs) {
            done.push(readrecursive(dirname + path.sep + dir));
        }
        done.push(processFiles(dirname, files));
        return q.all(done);
    }, function(err) {
        log.error("failed to stat file", err);
    });
};

readrecursive(argv._[0])
.then(flushDocumentBuffer)
.then(function() {
    log.info("all processing finished");
})
.done();
