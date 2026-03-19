/**
 * HTTP SSE Transport for WAMP
 *
 * Implements the WAMP HTTP-SSE transport specification.
 * Uses Server-Sent Events (SSE) for downstream messages and HTTP POST for upstream.
 * Provides a WebSocket-like API surface for compatibility with Wampy.
 */

/**
 * Simple SSE client for Node.js environments
 * Parses Server-Sent Events from an HTTP response stream
 */
class NodeSSEClient {
    constructor(url, options = {}) {
        this._url = url;
        this._headers = options.headers || {};
        this._withCredentials = options.withCredentials || false;
        this._request = null;
        this._readyState = 0; // CONNECTING

        // Event handlers
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;

        // Event listeners by type
        this._listeners = new Map();

        // SSE parsing state
        this._buffer = '';
        this._eventType = 'message';
        this._eventData = '';
        this._lastEventId = '';

        // Defer connection to next tick to allow event handlers to be set
        setImmediate(() => this._connect());
    }

    get readyState() {
        return this._readyState;
    }

    static get CONNECTING() { return 0; }
    static get OPEN() { return 1; }
    static get CLOSED() { return 2; }

    addEventListener(type, listener) {
        if (!this._listeners.has(type)) {
            this._listeners.set(type, []);
        }
        this._listeners.get(type).push(listener);
    }

    removeEventListener(type, listener) {
        const listeners = this._listeners.get(type);
        if (listeners) {
            const index = listeners.indexOf(listener);
            if (index !== -1) {
                listeners.splice(index, 1);
            }
        }
    }

    close() {
        this._readyState = NodeSSEClient.CLOSED;
        if (this._request) {
            this._request.destroy();
            this._request = null;
        }
    }

    async _connect() {
        // Dynamically import Node.js modules (avoids top-level await for browserify compat)
        let httpMod = null;
        let httpsMod = null;
        let zlibMod = null;
        try {
            httpMod = await import('http');
            httpsMod = await import('https');
            zlibMod = await import('zlib');
        } catch {
            this._readyState = NodeSSEClient.CLOSED;
            this._dispatchError(new Error('http/https modules not available'));
            return;
        }

        // Select http/https module based on URL protocol
        const urlObj = new URL(this._url);
        const isHttps = urlObj.protocol === 'https:';
        const mod = isHttps ? httpsMod : httpMod;

        const requestOptions = {
            method: 'GET',
            headers: {
                'Accept': 'text/event-stream',
                'Cache-Control': 'no-cache',
                ...(zlibMod ? { 'Accept-Encoding': 'gzip, deflate' } : {}),
                ...this._headers
            }
        };

        if (this._lastEventId) {
            requestOptions.headers['Last-Event-ID'] = this._lastEventId;
        }

        try {
            this._request = mod.request(this._url, requestOptions, (res) => {
                if (res.statusCode !== 200) {
                    this._readyState = NodeSSEClient.CLOSED;
                    this._dispatchError(new Error(`HTTP ${res.statusCode}`));
                    return;
                }

                this._readyState = NodeSSEClient.OPEN;

                // Call onopen handler
                if (this.onopen) {
                    try {
                        this.onopen({ target: this });
                    } catch (e) {
                        // Ignore errors in onopen handler
                    }
                }

                // Also dispatch to 'open' event listeners
                const openListeners = this._listeners.get('open');
                if (openListeners) {
                    for (const listener of openListeners) {
                        try {
                            listener({ target: this });
                        } catch (e) {
                            // Ignore errors in listeners
                        }
                    }
                }

                // Decompress if server sent gzip/deflate
                let stream = res;
                const encoding = (res.headers['content-encoding'] || '').toLowerCase();
                if (zlibMod && encoding === 'gzip') {
                    stream = res.pipe(zlibMod.createGunzip());
                } else if (zlibMod && encoding === 'deflate') {
                    stream = res.pipe(zlibMod.createInflate());
                }

                stream.setEncoding('utf8');
                stream.on('data', (chunk) => this._parseChunk(chunk));
                stream.on('end', () => {
                    this._readyState = NodeSSEClient.CLOSED;
                    this._dispatchError(new Error('Connection closed'));
                });
                stream.on('error', (err) => {
                    this._readyState = NodeSSEClient.CLOSED;
                    this._dispatchError(err);
                });
            });

            this._request.on('error', (err) => {
                this._readyState = NodeSSEClient.CLOSED;
                this._dispatchError(err);
            });

            this._request.end();
        } catch (err) {
            this._readyState = NodeSSEClient.CLOSED;
            this._dispatchError(err);
        }
    }

    _parseChunk(chunk) {
        this._buffer += chunk;
        const lines = this._buffer.split('\n');

        // Keep the last incomplete line in the buffer
        this._buffer = lines.pop() || '';

        for (const line of lines) {
            this._parseLine(line);
        }
    }

    _parseLine(line) {
        // Empty line dispatches the event
        if (line === '' || line === '\r') {
            this._dispatchEvent();
            return;
        }

        // Comment line (starts with :)
        if (line.startsWith(':')) {
            return;
        }

        // Parse field: value
        const colonIndex = line.indexOf(':');
        let field, value;

        if (colonIndex === -1) {
            field = line;
            value = '';
        } else {
            field = line.slice(0, colonIndex);
            value = line.slice(colonIndex + 1);
            // Remove leading space from value if present
            if (value.startsWith(' ')) {
                value = value.slice(1);
            }
        }

        // Remove trailing \r if present
        value = value.replace(/\r$/, '');

        switch (field) {
            case 'event':
                this._eventType = value;
                break;
            case 'data':
                this._eventData += (this._eventData ? '\n' : '') + value;
                break;
            case 'id':
                this._lastEventId = value;
                break;
            case 'retry':
                // Reconnection time - not implemented in this simple version
                break;
        }
    }

    _dispatchEvent() {
        if (!this._eventData) {
            this._eventType = 'message';
            return;
        }

        const event = {
            type: this._eventType,
            data: this._eventData,
            lastEventId: this._lastEventId,
            target: this
        };

        // Dispatch to specific event type listeners
        const listeners = this._listeners.get(this._eventType);
        if (listeners) {
            for (const listener of listeners) {
                listener(event);
            }
        }

        // Also dispatch to generic onmessage for 'message' type
        if (this._eventType === 'message' && this.onmessage) {
            this.onmessage(event);
        }

        // Reset for next event
        this._eventType = 'message';
        this._eventData = '';
    }

    _dispatchError(error) {
        const event = { error, target: this };

        const listeners = this._listeners.get('error');
        if (listeners) {
            for (const listener of listeners) {
                listener(event);
            }
        }

        if (this.onerror) {
            this.onerror(event);
        }
    }
}

export class SSETransport {
    // WebSocket-compatible ready states
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    constructor(options = {}) {
        this._baseUrl = null;
        this._transportId = null;
        this._protocol = null;
        this._readyState = SSETransport.CLOSED;
        this._eventSource = null;
        this._lastEventId = null;

        // Callbacks (WebSocket-compatible)
        this._onopen = null;
        this._onclose = null;
        this._onmessage = null;
        this._onerror = null;

        // Options
        this._protocols = options.protocols || ['wamp.2.json.sse', 'wamp.2.json'];
        this._headers = options.headers || {};
        this._fetchImpl = options.fetch || globalThis.fetch?.bind(globalThis);
        this._EventSourceImpl = options.EventSource || this._getEventSource();
        this._withCredentials = options.withCredentials || false;
        this._isBinary = false;
    }

    // WebSocket-compatible properties
    get protocol() { return this._protocol; }
    get readyState() { return this._readyState; }

    set onopen(fn) { this._onopen = fn; }
    get onopen() { return this._onopen; }

    set onclose(fn) { this._onclose = fn; }
    get onclose() { return this._onclose; }

    set onmessage(fn) { this._onmessage = fn; }
    get onmessage() { return this._onmessage; }

    set onerror(fn) { this._onerror = fn; }
    get onerror() { return this._onerror; }

    set binaryType(value) {
        this._isBinary = value === 'arraybuffer';
    }

    get binaryType() {
        return this._isBinary ? 'arraybuffer' : 'blob';
    }

    /**
     * Get EventSource implementation based on environment
     */
    _getEventSource() {
        // Browser environment
        if (typeof globalThis.EventSource !== 'undefined') {
            return globalThis.EventSource;
        }
        // Node.js environment - use our simple implementation
        return NodeSSEClient;
    }

    /**
     * Connect to the SSE endpoint
     * @param {string} url - Base URL for SSE endpoints
     */
    async connect(url) {
        if (this._readyState !== SSETransport.CLOSED) {
            throw new Error('Transport already connected or connecting');
        }

        this._baseUrl = url.replace(/\/$/, '');
        this._readyState = SSETransport.CONNECTING;

        try {
            // 1. Open transport session
            const openResponse = await this._fetch(`${this._baseUrl}/open`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...this._headers
                },
                body: JSON.stringify({ protocols: this._protocols })
            });

            if (!openResponse.ok) {
                const error = new Error(`Open failed: ${openResponse.status}`);
                this._readyState = SSETransport.CLOSED;
                this._emitError(error);
                throw error;
            }

            const { protocol, transport } = await openResponse.json();
            this._protocol = protocol;
            this._transportId = transport;

            // 2. Open SSE stream
            await this._openEventSource();

        } catch (error) {
            this._readyState = SSETransport.CLOSED;
            this._emitError(error);
            throw error;
        }
    }

    /**
     * Open SSE connection for receiving messages
     */
    async _openEventSource() {
        return new Promise((resolve, reject) => {
            const url = `${this._baseUrl}/${this._transportId}/receive`;

            const options = {
                headers: this._headers,
                withCredentials: this._withCredentials
            };

            // Create EventSource
            this._eventSource = new this._EventSourceImpl(url, options);

            // Handle connection open
            this._eventSource.onopen = () => {
                this._readyState = SSETransport.OPEN;
                if (this._onopen) {
                    this._onopen({ target: this });
                }
                resolve();
            };

            // Handle WAMP messages
            this._eventSource.addEventListener('wamp', (event) => {
                this._lastEventId = event.lastEventId;

                let data = event.data;

                // Decode base64url for binary protocols
                if (this._isBinary && this._isBinaryProtocol()) {
                    data = this._base64urlToArrayBuffer(data);
                }

                if (this._onmessage) {
                    this._onmessage({ data, target: this });
                }
            });

            // Handle transport errors
            this._eventSource.addEventListener('transport_error', (event) => {
                try {
                    const errorData = JSON.parse(event.data);
                    this._emitError(new Error(errorData.message || 'Transport error'));
                } catch {
                    this._emitError(new Error('Transport error'));
                }
            });

            // Handle connection errors
            this._eventSource.addEventListener('error', (event) => {
                if (!this._eventSource) return;
                if (this._eventSource.readyState === 2) { // CLOSED
                    this._handleSSEClosed();
                    reject(new Error('SSE connection failed'));
                } else {
                    this._emitError(new Error('SSE connection error'));
                }
            });

            // For NodeSSEClient, also handle generic errors
            this._eventSource.onerror = (event) => {
                if (this._readyState === SSETransport.CONNECTING) {
                    this._readyState = SSETransport.CLOSED;
                    reject(event.error || new Error('SSE connection failed'));
                } else if (this._eventSource && this._eventSource.readyState === 2) {
                    this._handleSSEClosed();
                }
            };
        });
    }

    /**
     * Send data to the server
     * @param {string|ArrayBuffer} data - Encoded WAMP message
     */
    async send(data) {
        if (this._readyState !== SSETransport.OPEN) {
            throw new Error('Transport not open');
        }

        // For SSE transport, binary data goes as application/octet-stream upstream
        const contentType = this._isBinary
            ? 'application/octet-stream'
            : 'application/json';

        try {
            const response = await this._fetch(
                `${this._baseUrl}/${this._transportId}/send`,
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': contentType,
                        ...this._headers
                    },
                    body: data
                }
            );

            if (!response.ok) {
                if (response.status === 404 || response.status === 410) {
                    this._handleTransportExpired();
                    return;
                }
                throw new Error(`Send failed: ${response.status}`);
            }
        } catch (error) {
            this._emitError(error);
            throw error;
        }
    }

    /**
     * Close the transport connection
     */
    async close() {
        if (this._readyState === SSETransport.CLOSED) {
            return;
        }

        this._readyState = SSETransport.CLOSING;

        // Close SSE stream
        if (this._eventSource) {
            this._eventSource.close();
            this._eventSource = null;
        }

        if (this._transportId) {
            try {
                await this._fetch(
                    `${this._baseUrl}/${this._transportId}/close`,
                    {
                        method: 'POST',
                        headers: this._headers
                    }
                );
            } catch {
                // Ignore close errors
            }
        }

        this._readyState = SSETransport.CLOSED;
        if (this._onclose) {
            this._onclose({ code: 1000, reason: 'Normal closure', target: this });
        }
    }

    _handleSSEClosed() {
        this._readyState = SSETransport.CLOSED;
        if (this._onclose) {
            this._onclose({ code: 1006, reason: 'SSE connection closed', target: this });
        }
    }

    _handleTransportExpired() {
        this._readyState = SSETransport.CLOSED;
        if (this._eventSource) {
            this._eventSource.close();
            this._eventSource = null;
        }
        if (this._onclose) {
            this._onclose({ code: 1006, reason: 'Transport session expired', target: this });
        }
    }

    _emitError(error) {
        if (this._onerror) {
            this._onerror({ error, target: this });
        }
    }

    _isBinaryProtocol() {
        return this._protocol &&
               (this._protocol.includes('msgpack') || this._protocol.includes('cbor'));
    }

    /**
     * Decode base64url to ArrayBuffer (RFC 4648 section 5)
     */
    _base64urlToArrayBuffer(base64url) {
        // Convert base64url to base64
        let base64 = base64url
            .replace(/-/g, '+')
            .replace(/_/g, '/');

        // Add padding if needed
        while (base64.length % 4) {
            base64 += '=';
        }

        // Decode
        if (typeof atob !== 'undefined') {
            // Browser
            const binary = atob(base64);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {
                bytes[i] = binary.charCodeAt(i);
            }
            return bytes.buffer;
        } else {
            // Node.js
            const buffer = Buffer.from(base64, 'base64');
            return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
        }
    }

    async _fetch(url, options) {
        if (!this._fetchImpl) {
            throw new Error('fetch is not available. Provide a fetch implementation via options.');
        }
        if (this._withCredentials) {
            options.credentials = 'include';
        }
        return this._fetchImpl(url, options);
    }
}

export default SSETransport;
