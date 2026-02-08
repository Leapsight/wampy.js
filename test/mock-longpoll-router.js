/**
 * Simple Mock WAMP Longpoll Router for testing
 *
 * Implements the WAMP HTTP Longpoll transport endpoints:
 * - POST /open          - Create transport session
 * - POST /{id}/receive  - Long-poll for messages
 * - POST /{id}/send     - Send messages to router
 * - POST /{id}/close    - Close transport session
 */

import { createServer } from 'http';
import { WAMP_MSG_SPEC } from '../src/constants.js';

export class MockLongpollRouter {
    constructor(port = 9000) {
        this.port = port;
        this.server = null;
        this.transports = new Map(); // transportId -> { queue, pendingReceive, closed }
        this.transportCounter = 0;

        // Configurable handlers
        this.onMessage = null; // (transportId, message) => void
        this.onOpen = null;    // (transportId) => void
        this.onClose = null;   // (transportId) => void

        // Default: auto-respond to HELLO with WELCOME
        this.autoWelcome = true;
        this.realm = 'realm1';
        this.sessionCounter = 1000;
    }

    start() {
        return new Promise((resolve, reject) => {
            this.server = createServer((req, res) => this._handleRequest(req, res));
            this.server.on('error', reject);
            this.server.listen(this.port, () => {
                resolve();
            });
        });
    }

    stop() {
        return new Promise((resolve) => {
            if (!this.server) {
                resolve();
                return;
            }

            // Close all pending receive requests
            for (const [, transport] of this.transports) {
                if (transport.pendingReceive) {
                    transport.pendingReceive.res.end('[]');
                    clearTimeout(transport.pendingReceive.timeout);
                }
            }
            this.transports.clear();

            this.server.close(() => {
                this.server = null;
                resolve();
            });
        });
    }

    /**
     * Queue a WAMP message to be sent to the client on next receive
     */
    queueMessage(transportId, message) {
        const transport = this.transports.get(transportId);
        if (!transport || transport.closed) {
            return false;
        }

        transport.queue.push(message);
        this._flushQueue(transportId);
        return true;
    }

    /**
     * Get all active transport IDs
     */
    getTransportIds() {
        return Array.from(this.transports.keys());
    }

    _handleRequest(req, res) {
        // Enable CORS for browser testing
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }

        const url = new URL(req.url, `http://localhost:${this.port}`);
        const path = url.pathname;

        // Collect request body
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', () => {
            this._routeRequest(path, body, res);
        });
    }

    _routeRequest(path, body, res) {
        // POST /open
        if (path === '/open') {
            this._handleOpen(body, res);
            return;
        }

        // Extract transport ID from path: /{id}/receive, /{id}/send, /{id}/close
        const match = path.match(/^\/([^/]+)\/(receive|send|close)$/);
        if (!match) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Not found' }));
            return;
        }

        const transportId = match[1];
        const action = match[2];

        if (!this.transports.has(transportId)) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Transport not found' }));
            return;
        }

        switch (action) {
            case 'receive':
                this._handleReceive(transportId, res);
                break;
            case 'send':
                this._handleSend(transportId, body, res);
                break;
            case 'close':
                this._handleClose(transportId, res);
                break;
        }
    }

    _handleOpen(body, res) {
        let protocols;
        try {
            const data = JSON.parse(body);
            protocols = data.protocols || [];
        } catch {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid JSON' }));
            return;
        }

        // Select first supported protocol (we only support JSON for now)
        const supportedProtocol = protocols.find(p =>
            p === 'wamp.2.json' || p === 'wamp.2.json.batched'
        );

        if (!supportedProtocol) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'No supported protocol' }));
            return;
        }

        const transportId = `t${++this.transportCounter}`;
        this.transports.set(transportId, {
            queue: [],
            pendingReceive: null,
            closed: false,
            protocol: supportedProtocol,
            sessionId: null
        });

        if (this.onOpen) {
            this.onOpen(transportId);
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            protocol: supportedProtocol,
            transport: transportId
        }));
    }

    _handleReceive(transportId, res) {
        const transport = this.transports.get(transportId);

        if (transport.closed) {
            res.writeHead(410, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Transport closed' }));
            return;
        }

        // If there are queued messages, send them immediately
        if (transport.queue.length > 0) {
            const message = transport.queue.shift();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(message));
            return;
        }

        // Otherwise, hold the request (long-poll)
        // Cancel any existing pending receive
        if (transport.pendingReceive) {
            transport.pendingReceive.res.writeHead(200, { 'Content-Type': 'application/json' });
            transport.pendingReceive.res.end('[]');
            clearTimeout(transport.pendingReceive.timeout);
        }

        // Set timeout for long-poll (30 seconds)
        const timeout = setTimeout(() => {
            if (transport.pendingReceive?.res === res) {
                transport.pendingReceive = null;
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end('[]'); // Empty response on timeout
            }
        }, 30000);

        transport.pendingReceive = { res, timeout };
    }

    _handleSend(transportId, body, res) {
        const transport = this.transports.get(transportId);

        if (transport.closed) {
            res.writeHead(410, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Transport closed' }));
            return;
        }

        let message;
        try {
            message = JSON.parse(body);
        } catch {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid JSON' }));
            return;
        }

        // Handle message
        const messageType = message[0];

        // Auto-respond to HELLO with WELCOME if enabled
        if (this.autoWelcome && messageType === WAMP_MSG_SPEC.HELLO) {
            const sessionId = ++this.sessionCounter;
            transport.sessionId = sessionId;

            const welcomeMsg = [
                WAMP_MSG_SPEC.WELCOME,
                sessionId,
                {
                    roles: {
                        broker: { features: {} },
                        dealer: { features: {} }
                    },
                    realm: this.realm
                }
            ];
            this.queueMessage(transportId, welcomeMsg);
        }

        // Auto-respond to GOODBYE with GOODBYE
        if (messageType === WAMP_MSG_SPEC.GOODBYE) {
            const goodbyeMsg = [
                WAMP_MSG_SPEC.GOODBYE,
                {},
                'wamp.close.goodbye_and_out'
            ];
            this.queueMessage(transportId, goodbyeMsg);
        }

        // Call custom message handler
        if (this.onMessage) {
            this.onMessage(transportId, message);
        }

        res.writeHead(202);
        res.end();
    }

    _handleClose(transportId, res) {
        const transport = this.transports.get(transportId);

        transport.closed = true;

        // Cancel pending receive
        if (transport.pendingReceive) {
            transport.pendingReceive.res.writeHead(200, { 'Content-Type': 'application/json' });
            transport.pendingReceive.res.end('[]');
            clearTimeout(transport.pendingReceive.timeout);
            transport.pendingReceive = null;
        }

        if (this.onClose) {
            this.onClose(transportId);
        }

        // Clean up after a delay
        setTimeout(() => {
            this.transports.delete(transportId);
        }, 1000);

        res.writeHead(202);
        res.end();
    }

    _flushQueue(transportId) {
        const transport = this.transports.get(transportId);
        if (!transport || !transport.pendingReceive || transport.queue.length === 0) {
            return;
        }

        const message = transport.queue.shift();
        const { res, timeout } = transport.pendingReceive;

        clearTimeout(timeout);
        transport.pendingReceive = null;

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(message));
    }
}

export default MockLongpollRouter;
