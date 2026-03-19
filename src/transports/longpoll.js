/**
 * HTTP Longpoll Transport for WAMP
 *
 * Implements the WAMP HTTP Longpoll transport specification.
 * Provides a WebSocket-like API surface for compatibility with Wampy.
 */

export class LongpollTransport {
    // WebSocket-compatible ready states
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    constructor(options = {}) {
        this._baseUrl = null;
        this._transportId = null;
        this._protocol = null;
        this._readyState = LongpollTransport.CLOSED;
        this._receiveAbortController = null;
        this._isReceiving = false;

        // Callbacks (WebSocket-compatible)
        this._onopen = null;
        this._onclose = null;
        this._onmessage = null;
        this._onerror = null;

        // Options
        this._protocols = options.protocols || ['wamp.2.json'];
        this._headers = options.headers || {};
        this._fetchImpl = options.fetch || globalThis.fetch?.bind(globalThis);
        this._withCredentials = options.withCredentials || false;
        this._receiveTimeout = options.receiveTimeout || 30000;
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
     * Connect to the longpoll endpoint
     * @param {string} url - Base URL for longpoll endpoints
     */
    async connect(url) {
        if (this._readyState !== LongpollTransport.CLOSED) {
            throw new Error('Transport already connected or connecting');
        }

        this._baseUrl = url.replace(/\/$/, '');
        this._readyState = LongpollTransport.CONNECTING;

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
                this._readyState = LongpollTransport.CLOSED;
                this._emitError(error);
                throw error;
            }

            const { protocol, transport } = await openResponse.json();
            this._protocol = protocol;
            this._transportId = transport;
            this._readyState = LongpollTransport.OPEN;

            // 2. Start receive loop
            this._startReceiveLoop();

            // 3. Notify connection open
            if (this._onopen) {
                this._onopen({ target: this });
            }
        } catch (error) {
            this._readyState = LongpollTransport.CLOSED;
            this._emitError(error);
            throw error;
        }
    }

    /**
     * Send data to the server
     * @param {string|ArrayBuffer} data - Encoded WAMP message
     */
    async send(data) {
        if (this._readyState !== LongpollTransport.OPEN) {
            throw new Error('Transport not open');
        }

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
        if (this._readyState === LongpollTransport.CLOSED) {
            return;
        }

        this._readyState = LongpollTransport.CLOSING;
        this._isReceiving = false;

        // Abort ongoing receive request
        if (this._receiveAbortController) {
            this._receiveAbortController.abort();
            this._receiveAbortController = null;
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

        this._readyState = LongpollTransport.CLOSED;
        if (this._onclose) {
            this._onclose({ code: 1000, reason: 'Normal closure', target: this });
        }
    }

    /**
     * Start the receive polling loop
     */
    async _startReceiveLoop() {
        this._isReceiving = true;

        while (this._readyState === LongpollTransport.OPEN && this._isReceiving) {
            try {
                this._receiveAbortController = new AbortController();

                // Add cache-busting parameter
                const url = `${this._baseUrl}/${this._transportId}/receive?x=${Date.now()}`;

                const response = await this._fetch(url, {
                    method: 'POST',
                    headers: this._headers,
                    signal: this._receiveAbortController.signal
                });

                if (!response.ok) {
                    if (response.status === 404 || response.status === 410) {
                        this._handleTransportExpired();
                        break;
                    }
                    // Brief delay before retry on error
                    await this._delay(1000);
                    continue;
                }

                let data;
                if (this._isBinary) {
                    data = await response.arrayBuffer();
                    if (data.byteLength === 0) continue;
                } else {
                    data = await response.text();
                    if (!data || data === '[]') continue;
                }

                if (this._onmessage) {
                    this._onmessage({ data, target: this });
                }
            } catch (error) {
                if (error.name === 'AbortError') {
                    break; // Normal abort during close
                }

                this._emitError(error);

                // Brief delay before retry on error
                await this._delay(1000);
            }
        }
    }

    _handleTransportExpired() {
        this._isReceiving = false;
        this._readyState = LongpollTransport.CLOSED;
        if (this._onclose) {
            this._onclose({ code: 1006, reason: 'Transport session expired', target: this });
        }
    }

    _emitError(error) {
        if (this._onerror) {
            this._onerror({ error, target: this });
        }
    }

    _delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
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

export default LongpollTransport;
