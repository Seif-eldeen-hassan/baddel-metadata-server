'use strict';

const { randomUUID } = require('crypto');
const log = require('../config/logger');

/**
 * Attach a unique request ID to each request and response.
 * Also logs request start/end with duration.
 */
function requestId(req, res, next) {
    const id = req.headers['x-request-id'] || randomUUID();
    req.id = id;
    res.setHeader('X-Request-ID', id);

    const start = Date.now();
    res.on('finish', () => {
        log.info({
            reqId:  id,
            method: req.method,
            path:   req.path,
            status: res.statusCode,
            ms:     Date.now() - start,
        }, 'request');
    });

    next();
}

module.exports = { requestId };
