var HarParser = function() {

};

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

HarParser.prototype.parseHar = function(source route, data) {
    var numhits = getNumCacheHist(data.har.log.entries);
    var docs = [{
        type = pageload,
        status: data.status,
        route: route,
        source: source
        date: data.har.log.pages[0].startedDateTime,
        onload: data.har.log.pages[0].pageTimings.onLoad,
        numhits: numhits
    }];
};

module.exports = HarParser;
