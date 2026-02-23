/**
 * Integration tests for wampy.js HTTP transports against a real Bondy router.
 *
 * Requires a running Bondy instance with:
 *   - HTTP longpoll listener on port 18080 at /wamp/longpoll
 *   - HTTP SSE listener on port 18080 at /wamp/sse
 *   - Realm 'com.leapsight.longpoll' with anonymous auth
 *
 * Run:
 *   npm run test:bondy
 */

import { expect } from 'chai';
import { Wampy } from '../src/wampy.js';
import { LongpollTransport } from '../src/transports/longpoll.js';

const LONGPOLL_URL = process.env.BONDY_LONGPOLL_URL || 'http://localhost:18080/wamp/longpoll';
const SSE_URL      = process.env.BONDY_SSE_URL || 'http://localhost:18080/wamp/sse';
const REALM        = process.env.BONDY_REALM || 'com.leapsight.test';

// Generate unique names per test run to avoid stale registrations
const SUFFIX = Date.now() + '.' + Math.random().toString(36).slice(2, 8);

function uniqueName (base) {
    return `${base}.${SUFFIX}.${Math.random().toString(36).slice(2, 8)}`;
}

function sleep (ms) {
    return new Promise(r => setTimeout(r, ms));
}

// Run the same test suite for each transport
for (const [transportName, url] of [['longpoll', LONGPOLL_URL], ['sse', SSE_URL]]) {

    describe(`Wampy.js with Bondy — ${transportName} transport`, function () {
        this.timeout(15000);

        // -------------------------------------------------------------------
        // Connection Lifecycle
        // -------------------------------------------------------------------
        describe('Connection Lifecycle', function () {

            it('should connect and obtain a session ID', async function () {
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();
                expect(wampy.getSessionId()).to.be.a('number');
                expect(wampy.getSessionId()).to.be.greaterThan(0);

                await wampy.disconnect();
            });

            it('should disconnect gracefully', async function () {
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();
                expect(wampy.getSessionId()).to.be.a('number');

                await wampy.disconnect();
                expect(wampy.getSessionId()).to.not.be.ok;
            });

            it('should call onClose callback on disconnect', async function () {
                let closeCalled = false;

                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                    onClose: () => { closeCalled = true; },
                });

                await wampy.connect();
                await wampy.disconnect();

                expect(closeCalled).to.be.true;
            });

            it('should handle connection failure gracefully', async function () {
                const wampy = new Wampy('http://localhost:59999', {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                try {
                    await wampy.connect();
                    expect.fail('Should have thrown');
                } catch (e) {
                    expect(e).to.be.an('error');
                }
            });

            it('should throw when realm is not specified', async function () {
                const wampy = new Wampy(url, {
                    transport: transportName,
                    autoReconnect: false,
                });

                try {
                    await wampy.connect();
                    expect.fail('Should have thrown');
                } catch (e) {
                    expect(e.message).to.include('realm');
                }
            });
        });

        // -------------------------------------------------------------------
        // RPC Operations
        // -------------------------------------------------------------------
        describe('RPC Operations', function () {

            it('should register and unregister a procedure', async function () {
                const proc = uniqueName('com.test.rpc.register');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                const reg = await wampy.register(proc, () => {
                    return { argsList: ['ok'] };
                });
                expect(reg).to.be.an('object');

                await wampy.unregister(proc);
                await wampy.disconnect();
            });

            it('should call a self-registered procedure', async function () {
                const proc = uniqueName('com.test.rpc.selfcall');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                await wampy.register(proc, (invocation) => {
                    const [a, b] = invocation.argsList;
                    return { argsList: [a + b] };
                });

                const result = await wampy.call(proc, { argsList: [17, 25] });
                expect(result).to.be.an('object');
                expect(result.argsList).to.deep.equal([42]);

                await wampy.unregister(proc);
                await wampy.disconnect();
            });

            it('should call a procedure registered by another client', async function () {
                const proc = uniqueName('com.test.rpc.crosscall');

                const server = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });
                const client = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await server.connect();
                await client.connect();

                await server.register(proc, (invocation) => {
                    return { argsList: [invocation.argsList[0].toUpperCase()] };
                });

                const result = await client.call(proc, { argsList: ['hello'] });
                expect(result.argsList).to.deep.equal(['HELLO']);

                await server.unregister(proc);
                await server.disconnect();
                await client.disconnect();
            });

            it('should receive argsDict in RPC invocation and result', async function () {
                const proc = uniqueName('com.test.rpc.kwargs');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                await wampy.register(proc, (invocation) => {
                    return {
                        argsList: [invocation.argsDict.x + invocation.argsDict.y],
                        argsDict: { op: 'add' },
                    };
                });

                const result = await wampy.call(proc, {
                    argsList: [],
                    argsDict: { x: 3, y: 7 },
                });
                expect(result.argsList).to.deep.equal([10]);
                expect(result.argsDict).to.deep.equal({ op: 'add' });

                await wampy.unregister(proc);
                await wampy.disconnect();
            });

            it('should handle RPC errors from callee', async function () {
                const proc = uniqueName('com.test.rpc.error');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                await wampy.register(proc, () => {
                    throw new Error('Something went wrong');
                });

                try {
                    await wampy.call(proc, { argsList: [] });
                    expect.fail('Should have thrown');
                } catch (e) {
                    expect(e).to.be.an('error');
                }

                await wampy.unregister(proc);
                await wampy.disconnect();
            });
        });

        // -------------------------------------------------------------------
        // Pub/Sub Operations
        // -------------------------------------------------------------------
        describe('Pub/Sub Operations', function () {

            it('should subscribe and unsubscribe from a topic', async function () {
                const topic = uniqueName('com.test.pubsub.sub');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                const sub = await wampy.subscribe(topic, () => {});
                expect(sub).to.be.an('object');
                expect(sub.subscriptionId).to.be.a('number');

                await wampy.unsubscribe(sub.subscriptionKey);
                await wampy.disconnect();
            });

            it('should receive events on subscribed topic (exclude_me: false)', async function () {
                const topic = uniqueName('com.test.pubsub.selfpub');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                const receivedEvents = [];

                await wampy.subscribe(topic, (eventData) => {
                    receivedEvents.push(eventData);
                });

                await wampy.publish(topic, { argsList: ['hello'] }, { exclude_me: false });

                // Give the router time to deliver the event
                await sleep(1000);

                expect(receivedEvents).to.have.length(1);
                expect(receivedEvents[0].argsList).to.deep.equal(['hello']);

                await wampy.disconnect();
            });

            it('should receive events published by another client', async function () {
                const topic = uniqueName('com.test.pubsub.crosspub');

                const subscriber = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });
                const publisher = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await subscriber.connect();
                await publisher.connect();

                const received = new Promise((resolve) => {
                    subscriber.subscribe(topic, (eventData) => {
                        resolve(eventData);
                    });
                });

                // Small delay to ensure subscription is established
                await sleep(500);

                await publisher.publish(topic, { argsList: ['cross-pub'] });

                const eventData = await Promise.race([
                    received,
                    sleep(5000).then(() => { throw new Error('Timeout waiting for event'); }),
                ]);

                expect(eventData.argsList).to.deep.equal(['cross-pub']);

                await subscriber.disconnect();
                await publisher.disconnect();
            });

            it('should receive multiple events in sequence', async function () {
                const topic = uniqueName('com.test.pubsub.multi');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                const receivedEvents = [];
                let resolveAll;
                const allReceived = new Promise((resolve) => { resolveAll = resolve; });

                await wampy.subscribe(topic, (eventData) => {
                    receivedEvents.push(eventData.argsList[0]);
                    if (receivedEvents.length === 3) {
                        resolveAll();
                    }
                });

                for (let i = 1; i <= 3; i++) {
                    await wampy.publish(topic, { argsList: [`event-${i}`] }, { exclude_me: false });
                }

                await Promise.race([
                    allReceived,
                    sleep(5000).then(() => { throw new Error('Timeout waiting for events'); }),
                ]);

                expect(receivedEvents).to.deep.equal(['event-1', 'event-2', 'event-3']);

                await wampy.disconnect();
            });

            it('should receive argsDict in published events', async function () {
                const topic = uniqueName('com.test.pubsub.kwargs');
                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                let resolveReceived;
                const received = new Promise((resolve) => { resolveReceived = resolve; });

                await wampy.subscribe(topic, (eventData) => {
                    resolveReceived(eventData);
                });

                await wampy.publish(
                    topic,
                    { argsList: ['val'], argsDict: { key: 'test' } },
                    { exclude_me: false }
                );

                const eventData = await Promise.race([
                    received,
                    sleep(5000).then(() => { throw new Error('Timeout waiting for event'); }),
                ]);

                expect(eventData.argsList).to.deep.equal(['val']);
                expect(eventData.argsDict).to.deep.equal({ key: 'test' });

                await wampy.disconnect();
            });
        });

        // -------------------------------------------------------------------
        // Combined RPC + Pub/Sub
        // -------------------------------------------------------------------
        describe('Combined Operations', function () {

            it('should handle RPC and pub/sub on the same session', async function () {
                const proc = uniqueName('com.test.combined.rpc');
                const topic = uniqueName('com.test.combined.topic');

                const wampy = new Wampy(url, {
                    realm: REALM,
                    transport: transportName,
                    autoReconnect: false,
                });

                await wampy.connect();

                // Register RPC
                await wampy.register(proc, (invocation) => {
                    return { argsList: [invocation.argsList[0] * 2] };
                });

                // Subscribe to topic
                const receivedEvents = [];
                await wampy.subscribe(topic, (eventData) => {
                    receivedEvents.push(eventData.argsList[0]);
                });

                // Call RPC
                const rpcResult = await wampy.call(proc, { argsList: [21] });
                expect(rpcResult.argsList).to.deep.equal([42]);

                // Publish event
                await wampy.publish(topic, { argsList: ['combo'] }, { exclude_me: false });
                await sleep(1000);

                expect(receivedEvents).to.deep.equal(['combo']);

                await wampy.unregister(proc);
                await wampy.disconnect();
            });
        });
    });
}

// ---------------------------------------------------------------------------
// Longpoll-specific: message queuing during receive gaps
// ---------------------------------------------------------------------------
describe('Wampy.js with Bondy — longpoll message queuing', function () {
    this.timeout(30000);

    const WAMP_HELLO     = 1;
    const WAMP_WELCOME   = 2;
    const WAMP_SUBSCRIBE = 32;
    const WAMP_SUBSCRIBED = 33;
    const WAMP_EVENT     = 36;

    /**
     * Low-level helper: open a longpoll transport, complete the WAMP
     * handshake, and subscribe to a topic — all with manual control over
     * when receive is called.
     */
    async function manualLongpollSession (topic) {
        const base = LONGPOLL_URL;

        // 1. Open transport
        const openResp = await fetch(`${base}/open`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ protocols: ['wamp.2.json'] }),
        });
        const { transport: tid } = await openResp.json();

        const send = (msg) => fetch(`${base}/${tid}/send`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(msg),
        });

        const receive = async (timeoutMs = 10000) => {
            const ctrl = new AbortController();
            const timer = setTimeout(() => ctrl.abort(), timeoutMs);
            try {
                const resp = await fetch(`${base}/${tid}/receive`, {
                    method: 'POST',
                    signal: ctrl.signal,
                });
                const text = await resp.text();
                return text ? JSON.parse(text) : null;
            } finally {
                clearTimeout(timer);
            }
        };

        const close = () => fetch(`${base}/${tid}/close`, { method: 'POST' }).catch(() => {});

        // 2. HELLO / WELCOME
        await send([WAMP_HELLO, REALM, {
            roles: { caller: {}, callee: {}, publisher: {}, subscriber: {} },
        }]);
        const welcome = await receive();
        expect(welcome[0]).to.equal(WAMP_WELCOME);

        // 3. SUBSCRIBE
        const subReqId = 1;
        await send([WAMP_SUBSCRIBE, subReqId, {}, topic]);
        const subscribed = await receive();
        expect(subscribed[0]).to.equal(WAMP_SUBSCRIBED);
        expect(subscribed[1]).to.equal(subReqId);
        const subscriptionId = subscribed[2];

        return { tid, send, receive, close, subscriptionId };
    }

    it('should deliver messages that were queued while no receive was pending', async function () {
        const topic = uniqueName('com.test.longpoll.queue');
        const messageCount = 5;

        // Set up a manual longpoll subscriber — we control when receive is called
        const session = await manualLongpollSession(topic);

        // Set up a publisher via normal Wampy (on SSE for independence)
        const publisher = new Wampy(SSE_URL, {
            realm: REALM,
            transport: 'sse',
            autoReconnect: false,
        });
        await publisher.connect();

        // At this point, the subscriber has NO pending receive request.
        // Publish several messages — they must be queued by Bondy.
        for (let i = 1; i <= messageCount; i++) {
            await publisher.publish(
                topic,
                { argsList: [`queued-${i}`] },
                { exclude_me: false }
            );
        }

        // Small delay to ensure all publishes are processed by Bondy
        await sleep(500);

        // NOW start receiving — all queued messages should be delivered
        const received = [];
        for (let i = 0; i < messageCount; i++) {
            try {
                const msg = await session.receive(5000);
                if (msg && msg[0] === WAMP_EVENT) {
                    received.push(msg[4][0]); // argsList[0]
                }
            } catch {
                break; // AbortError = timeout, stop trying
            }
        }

        expect(received).to.have.length(messageCount);
        for (let i = 1; i <= messageCount; i++) {
            expect(received).to.include(`queued-${i}`);
        }

        await session.close();
        await publisher.disconnect();
    });

    it('should deliver messages queued during a deliberate pause between receives', async function () {
        const topic = uniqueName('com.test.longpoll.gap');

        const session = await manualLongpollSession(topic);

        // Set up a publisher via SSE
        const publisher = new Wampy(SSE_URL, {
            realm: REALM,
            transport: 'sse',
            autoReconnect: false,
        });
        await publisher.connect();

        // Publish one message, receive it normally
        await publisher.publish(topic, { argsList: ['before-gap'] });
        const msg1 = await session.receive(5000);
        expect(msg1[0]).to.equal(WAMP_EVENT);
        expect(msg1[4]).to.deep.equal(['before-gap']);

        // ---- GAP: no receive pending for 2 seconds ----
        // Publish messages DURING the gap
        await sleep(500);
        await publisher.publish(topic, { argsList: ['during-gap-1'] });
        await publisher.publish(topic, { argsList: ['during-gap-2'] });
        await sleep(1500);
        // ---- END GAP ----

        // Now receive — both messages should have been queued by Bondy
        const received = [];
        for (let i = 0; i < 2; i++) {
            try {
                const msg = await session.receive(5000);
                if (msg && msg[0] === WAMP_EVENT) {
                    received.push(msg[4][0]);
                }
            } catch {
                break;
            }
        }

        expect(received).to.deep.equal(['during-gap-1', 'during-gap-2']);

        await session.close();
        await publisher.disconnect();
    });
});

// ---------------------------------------------------------------------------
// GZIP compression tests
// ---------------------------------------------------------------------------
describe('Wampy.js with Bondy — gzip compression', function () {
    this.timeout(15000);

    describe('longpoll transport', function () {

        it('should receive gzip-compressed responses from Bondy', async function () {
            const base = LONGPOLL_URL;

            // Open transport
            const { transport: tid } = await (await fetch(`${base}/open`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ protocols: ['wamp.2.json'] }),
            })).json();

            // Send HELLO
            await fetch(`${base}/${tid}/send`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify([1, REALM, { roles: { subscriber: {} } }]),
            });

            // Receive WELCOME — check response headers for gzip
            const recvResp = await fetch(`${base}/${tid}/receive`, { method: 'POST' });
            const encoding = recvResp.headers.get('content-encoding');
            const text = await recvResp.text();

            expect(encoding).to.equal('gzip');
            expect(text).to.be.a('string');
            // Verify the decompressed body is valid WAMP WELCOME
            const msg = JSON.parse(text);
            expect(msg[0]).to.equal(2); // WELCOME

            await fetch(`${base}/${tid}/close`, { method: 'POST' }).catch(() => {});
        });

        it('should perform full RPC round-trip over gzip-compressed longpoll', async function () {
            const proc = uniqueName('com.test.gzip.longpoll.rpc');
            const wampy = new Wampy(LONGPOLL_URL, {
                realm: REALM,
                transport: 'longpoll',
                autoReconnect: false,
            });

            await wampy.connect();

            await wampy.register(proc, (inv) => {
                return { argsList: [inv.argsList[0] + inv.argsList[1]] };
            });

            const result = await wampy.call(proc, { argsList: [100, 200] });
            expect(result.argsList).to.deep.equal([300]);

            await wampy.unregister(proc);
            await wampy.disconnect();
        });
    });

    describe('sse transport', function () {

        it('should receive gzip-compressed SSE stream from Bondy', async function () {
            const http = await import('http');
            const base = SSE_URL;

            // Open transport
            const { transport: tid } = await (await fetch(`${base}/open`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ protocols: ['wamp.2.json.sse'] }),
            })).json();

            // Check that SSE receive endpoint returns gzip when requested
            const recvUrl = new URL(`${base}/${tid}/receive`);
            const headers = await new Promise((resolve, reject) => {
                const req = http.default.request({
                    hostname: recvUrl.hostname,
                    port: recvUrl.port,
                    path: recvUrl.pathname,
                    headers: {
                        'Accept': 'text/event-stream',
                        'Accept-Encoding': 'gzip',
                    },
                }, (res) => {
                    resolve(res.headers);
                    res.destroy();
                });
                req.on('error', reject);
                req.end();
            });

            expect(headers['content-encoding']).to.equal('gzip');
            expect(headers['content-type']).to.include('text/event-stream');

            await fetch(`${base}/${tid}/close`, { method: 'POST' }).catch(() => {});
        });

        it('should perform full RPC round-trip over gzip-compressed SSE', async function () {
            const proc = uniqueName('com.test.gzip.sse.rpc');
            const wampy = new Wampy(SSE_URL, {
                realm: REALM,
                transport: 'sse',
                autoReconnect: false,
            });

            await wampy.connect();

            await wampy.register(proc, (inv) => {
                return { argsList: [inv.argsList[0] + inv.argsList[1]] };
            });

            const result = await wampy.call(proc, { argsList: [100, 200] });
            expect(result.argsList).to.deep.equal([300]);

            await wampy.unregister(proc);
            await wampy.disconnect();
        });

        it('should receive pub/sub events over gzip-compressed SSE stream', async function () {
            const topic = uniqueName('com.test.gzip.sse.pubsub');
            const wampy = new Wampy(SSE_URL, {
                realm: REALM,
                transport: 'sse',
                autoReconnect: false,
            });

            await wampy.connect();

            const receivedEvents = [];
            let resolveAll;
            const allReceived = new Promise((resolve) => { resolveAll = resolve; });

            await wampy.subscribe(topic, (eventData) => {
                receivedEvents.push(eventData.argsList[0]);
                if (receivedEvents.length === 3) {
                    resolveAll();
                }
            });

            for (let i = 1; i <= 3; i++) {
                await wampy.publish(topic, { argsList: [`gzip-event-${i}`] }, { exclude_me: false });
            }

            await Promise.race([
                allReceived,
                sleep(5000).then(() => { throw new Error('Timeout waiting for events'); }),
            ]);

            expect(receivedEvents).to.deep.equal(['gzip-event-1', 'gzip-event-2', 'gzip-event-3']);

            await wampy.disconnect();
        });
    });
});
