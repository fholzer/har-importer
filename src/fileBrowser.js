var log = require('log4js').getLogger('index'),
    q = require('q'),
    path = require('path'),
    fs = require('fs'),
    readdir = q.denodeify(fs.readdir),
    readfile = q.denodeify(fs.readFile),
    stat = q.denodeify(fs.stat);

var FileBrowser = function(harParser, es, source, route) {
    this.harParser = harParser;
    this.es = es;
    this.source = source;
    this.route = route;
};

FileBrowser.prototype.processFiles = function(dirpath, files) {
    var self = this;

    log.info("processing " + files.length + " files");

    var done = q();
    for(var file of files) {
        done = done.then(function(file) {
            return readfile(dirpath + path.sep + file, 'utf8')
            .then(function(data) {
                var docs = self.harParser.parse(self.source, self.route, JSON.parse(data));
                return self.es.pushMultiple(doc);
            });
        }.bind(this, file));
    }
    return done;
};

FileBrowser.prototype.readRecursive = function(dirname) {
    var self = this;

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
            done.push(self.readRecursive(dirname + path.sep + dir));
        }
        done.push(self.processFiles(dirname, files));
        return q.all(done);
    }, function(err) {
        log.error("failed to stat file", err);
    });
};

module.exports = FileBrowser;
