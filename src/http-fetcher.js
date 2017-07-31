"use strict";
var Transform = require("stream")
    .Transform;
var util = require("util");
var RateLimiter = require("limiter")
    .RateLimiter;
var Error = require("@petitchevalroux/error");
var path = require("path");
var logger = require(path.join(__dirname, "logger"));
var retry = require("retry");

function HttpFetcherStream(options) {
    if (!(this instanceof HttpFetcherStream)) {
        return new HttpFetcherStream(options);
    }
    this.options = Object.assign({
        "timeout": 5000,
        "retries": 10,
        "rateCount": 5,
        "rateWindow": 10000
    }, options || {});
    Transform.call(this, {
        "objectMode": true
    });
    this.limiter = new RateLimiter(this.options.rateCount, this.options.rateWindow);
}
util.inherits(HttpFetcherStream, Transform);

HttpFetcherStream.prototype._transform = function(chunk, encoding, callback) {
    var self = this;
    var operation = retry.operation({
        "minTimeout": self.options.timeout,
        "retries": self.options.retries
    });
    operation.attempt(function(attempt) {
        logger.debug("downloading (url: %s, attempt: %d)", chunk.url,
            attempt);
        self.get(chunk, function(err, response) {
            if (operation.retry(err)) {
                return;
            }
            callback(err ? operation.mainError() : null,
                response);
        });
    });
};

HttpFetcherStream.prototype.get = function(chunk, callback) {
    var self = this;
    self.limiter.removeTokens(1, function(err) {
        if (err) {
            callback(err);
            return;
        }
        self.options.httpClient.get(chunk.url, function(err,
            response, body) {
            if (err) {
                err.url = chunk.url;
                self.emit("http:error", err);
                callback(new Error(
                    "Unable to download (chunk: %j)",
                    chunk, err));
                return;
            }
            self.emit("http:response", response);
            if (response.statusCode !== 200) {
                callback(new Error(
                    "Non 200 http status (response: %j)",
                    response));
            } else {
                callback(null, {
                    "url": chunk.url,
                    "context": chunk.context,
                    "body": body,
                    "headers": response.headers
                });
            }
        });
    });
};

module.exports = HttpFetcherStream;
