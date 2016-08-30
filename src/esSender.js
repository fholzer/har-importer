var elasticsearch = require('elasticsearch'),
    q = require('q');

function pad(n) {
    return n < 10 ? '0' + n : n;
};

var esSender = function(options) {
    this.es = new elasticsearch.Client(options);
    this.docBuffer = [];
}

esSender.prototype.pushArray = function(docs) {
    var done = q();
    for(var d of docs) {
        done.then(this.push(d));
    }
    return done;
}

esSender.prototype.push = function(doc) {
    if(typeof doc === "array") {
        return pushArray(doc);
    }
    this.docBuffer.push(doc);
    if(this.docBuffer.length < BUFFER_MAX_SIZE) {
        return;
    }
    return flushDocumentBuffer();
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
        var type = d.type;
        delete d.type;
        body.push({ index: {_index: index, _type: type }});
        body.push(d);
    }
    //log.debug(body);
    return q.ninvoke(es, "bulk", { body: body });
};
