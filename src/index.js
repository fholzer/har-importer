var q = require('q'),
    log = require('log4js').getLogger('index'),
    argv = require('minimist')(process.argv.slice(2)),
    EsSender = require('./esSender'),
    FileBrowser = require('./fileBrowser'),
    HarParser = require('./harParser');

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

var parser = new HarParser();

var browser = new FileBrowser(parser, es, argv.source, argv.route);

browser.readRecursive(argv._[0])
.then(function() {
    return es.flush();
})
.then(function() {
    log.info("all processing finished");
})
.done();
