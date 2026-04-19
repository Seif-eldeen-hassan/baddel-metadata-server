'use strict';

/**
 * middleware/errorHandler.js
 *
 * Centralized Express error handler.
 * Logs the full error server-side, returns safe message to client.
 * Never leaks stack traces or internal details to callers.
 */

const log = require('../config/logger');

function errorHandler(err, req, res, _next) {
    const reqId = req.id || 'unknown';

    log.error({
        reqId,
        path:   req.path,
        method: req.method,
        err:    { message: err.message, stack: err.stack, code: err.code },
    }, 'Unhandled error');

    // Don't leak internal details
    const status  = err.status || err.statusCode || 500;
    const message = status < 500 ? err.message : 'Internal server error';

    res.status(status).json({ status: 'error', message, reqId });
}

module.exports = { errorHandler };
