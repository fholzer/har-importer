var elasticsearch = require('elasticsearch'),
    log = require('log4js').getLogger('EsSender'),
    q = require('q');

const BUFFER_MAX_SIZE = 50;

function pad(n) {
    return n < 10 ? '0' + n : n;
}

var esSender = function(options) {
    this.es = new elasticsearch.Client(options);
    this.docBuffer = [];
};

esSender.prototype.pushMultiple = function(docs) {
    var done = q();
    for(var d of docs) {
        done.then(this.push(d));
    }
    return done;
};

esSender.prototype.push = function(doc) {
    if(doc.constructor === Array) {
        return this.pushMultiple(doc);
    }
    this.docBuffer.push(doc);
    if(this.docBuffer.length < BUFFER_MAX_SIZE) {
        return;
    }
    return this.flush();
};

esSender.prototype.flush = function() {
    if(this.docBuffer.length === 0) {
        return;
    }
    var tmp = this.docBuffer;
    this.docBuffer = [];
    log.info("flushing " + tmp.length + " documents");

    var body = [];
    for(var d of tmp) {
        var date = new Date(Date.parse(d.date));
        d.date = date;
        var index = "pwt-" + date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate());
        var type = d.type,
            id = d.id;
        delete d.type;
        delete d.id;
        body.push({ index: {_index: index, _type: type }});
        /*log.info("DEBUG");
        log.info(d);
        process.exit(0);*/
        body.push(d);
    }
    //log.debug(body);
    return q.ninvoke(this.es, "bulk", { body: body });
};

module.exports = esSender;
