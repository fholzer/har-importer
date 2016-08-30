var fs = require('fs'),
    path = require('path'),
    q = require('q'),
    log4js = require('log4js'),
    log = log4js.getLogger('index'),
    argv = require('minimist')(process.argv.slice(2)),
    readdir = q.denodeify(fs.readdir),
    readfile = q.denodeify(fs.readFile),
    stat = q.denodeify(fs.stat);

const BUFFER_MAX_SIZE = 50;

var processFiles = function(dirpath, files) {
    log.info("processing " + files.length + " files");
    var done = q();
    for(var file of files) {
        done = done.then(function(file) {
            return readfile(dirpath + path.sep + file, 'utf8')
            .then(function(data) {
                var docs = parseHar(argv.source, argv.route, JSON.parse(data));
                return es.pushMultiple(doc);
            });
        }.bind(this, file));
    }
    return done;
};

var printUsage = function(e) {
    log.info("Usage: node " + __filename + " --source <source-name> --route <route-name> <directory>");
    log.info(" source-name This string is added to each document create in ES from har files");
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

if(typeof argv.source === "undefined") {
    log.error("Argument 'source' required!");
    printUsage(1);
}

if(argv._.length != 1) {
    log.error("Provide one directory as argument!");
    printUsage(1);
}

var es = new EsSender({
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

        var done = [];
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
