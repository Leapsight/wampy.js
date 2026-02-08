/**
 * Simple Mock WAMP SSE Router for testing
 *
 * Implements the WAMP HTTP-SSE transport endpoints:
 * - POST /open              - Create transport session
 * - GET  /{id}/receive      - SSE stream for downstream messages
 * - POST /{id}/send         - Send messages to router
 * - POST /{id}/close        - Close transport session
 */

import { createServer } from 'http';
import { WAMP_MSG_SPEC } from '../src/constants.js';

export class MockSSERouter {
    constructor(port = 9000) {
        this.port = port;
        this.server = null;
        this.transports = new Map(); // transportId -> { sseResponse, queue, closed, eventId }
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

            // Close all SSE connections
            for (const [, transport] of this.transports) {
                if (transport.sseResponse) {
                    transport.sseResponse.end();
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
     * Queue a WAMP message to be sent to the client via SSE
     */
    queueMessage(transportId, message) {
        const transport = this.transports.get(transportId);
        if (!transport || transport.closed) {
            return false;
        }

        // If SSE connection is active, send immediately
        if (transport.sseResponse) {
            this._sendSSEEvent(transport, message);
        } else {
            // Otherwise queue for when SSE connects
            transport.queue.push(message);
        }
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
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Accept, Last-Event-ID');

        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }

        const url = new URL(req.url, `http://localhost:${this.port}`);
        const path = url.pathname;

        // POST /open
        if (path === '/open' && req.method === 'POST') {
            this._collectBody(req, (body) => this._handleOpen(body, res));
            return;
        }

        // Extract transport ID from path
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
                if (req.method === 'GET') {
                    this._handleReceiveSSE(transportId, req, res);
                } else {
                    res.writeHead(405, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'Method not allowed' }));
                }
                break;
            case 'send':
                if (req.method === 'POST') {
                    this._collectBody(req, (body) => this._handleSend(transportId, body, res));
                } else {
                    res.writeHead(405, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'Method not allowed' }));
                }
                break;
            case 'close':
                if (req.method === 'POST') {
                    this._handleClose(transportId, res);
                } else {
                    res.writeHead(405, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'Method not allowed' }));
                }
                break;
        }
    }

    _collectBody(req, callback) {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', () => callback(body));
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

        // Select first supported SSE protocol
        const supportedProtocol = protocols.find(p =>
            p === 'wamp.2.json.sse' ||
            p === 'wamp.2.json.batched.sse' ||
            p === 'wamp.2.json' // Also accept non-sse for flexibility
        );

        if (!supportedProtocol) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'No supported protocol' }));
            return;
        }

        const transportId = `t${++this.transportCounter}`;
        this.transports.set(transportId, {
            queue: [],
            sseResponse: null,
            closed: false,
            protocol: supportedProtocol,
            sessionId: null,
            eventId: 0
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

    _handleReceiveSSE(transportId, req, res) {
        const transport = this.transports.get(transportId);

        if (transport.closed) {
            res.writeHead(410, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Transport closed' }));
            return;
        }

        // Close any existing SSE connection for this transport
        if (transport.sseResponse) {
            transport.sseResponse.end();
        }

        // Set up SSE headers
        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no' // Disable nginx buffering
        });

        // Send initial comment to flush headers and establish connection
        res.write(': connected\n\n');

        // Store the response for sending events
        transport.sseResponse = res;

        // Handle client disconnect
        req.on('close', () => {
            if (transport.sseResponse === res) {
                transport.sseResponse = null;
            }
        });

        // Check for Last-Event-ID for resumption (optional)
        const lastEventId = req.headers['last-event-id'];
        if (lastEventId) {
            // For simplicity, we don't implement replay in the mock
            // Real implementation would replay events after lastEventId
        }

        // Send any queued messages
        while (transport.queue.length > 0) {
            const message = transport.queue.shift();
            this._sendSSEEvent(transport, message);
        }

        // Start keepalive
        transport.keepaliveInterval = setInterval(() => {
            if (transport.sseResponse) {
                transport.sseResponse.write(': keepalive\n\n');
            }
        }, 15000);
    }

    _sendSSEEvent(transport, message) {
        if (!transport.sseResponse) {
            transport.queue.push(message);
            return;
        }

        transport.eventId++;
        const data = JSON.stringify(message);

        let event = '';
        event += `id: ${transport.eventId}\n`;
        event += `event: wamp\n`;
        event += `data: ${data}\n`;
        event += '\n';

        transport.sseResponse.write(event);
    }

    _sendSSEError(transport, code, message) {
        if (!transport.sseResponse) {
            return;
        }

        const data = JSON.stringify({ code, message });

        let event = '';
        event += `event: transport_error\n`;
        event += `data: ${data}\n`;
        event += '\n';

        transport.sseResponse.write(event);
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

        // Clear keepalive
        if (transport.keepaliveInterval) {
            clearInterval(transport.keepaliveInterval);
        }

        // Close SSE connection
        if (transport.sseResponse) {
            transport.sseResponse.end();
            transport.sseResponse = null;
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
}

export default MockSSERouter;
