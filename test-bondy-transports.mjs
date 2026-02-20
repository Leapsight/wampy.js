/**
 * Test wampy.js HTTP Longpoll and HTTP Longpoll-SSE transports against Bondy.
 *
 * Usage:
 *   node test-bondy-transports.mjs [--url http://localhost:18080/wamp/longpoll] \
 *       [--sse-url http://localhost:18080/wamp/sse] [--realm com.example.realm]
 *
 * Defaults:
 *   url:     http://localhost:18080/wamp/longpoll
 *   sse-url: http://localhost:18080/wamp/sse
 *   realm:   com.leapsight.test
 */

import { Wampy, LongpollTransport, SSETransport } from './src/wampy.js';

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------
const args = process.argv.slice(2);
function flag(name, fallback) {
    const idx = args.indexOf(name);
    return idx !== -1 && idx + 1 < args.length ? args[idx + 1] : fallback;
}

const LONGPOLL_URL = flag('--url',     'http://localhost:18080/wamp/longpoll');
const SSE_URL      = flag('--sse-url', 'http://localhost:18080/wamp/sse');
const REALM        = flag('--realm',   'com.leapsight.test');

const SUFFIX = Date.now();
const TOPIC = `com.example.test.onhello.${SUFFIX}`;
const RPC   = `com.example.test.add2.${SUFFIX}`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function log(transport, ...msg) {
    const ts = new Date().toISOString().slice(11, 23);
    console.log(`[${ts}] [${transport}]`, ...msg);
}

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Test scenario (runs once per transport)
// ---------------------------------------------------------------------------
async function runTest(transportName) {
    const url = transportName === 'sse' ? SSE_URL : LONGPOLL_URL;
    log(transportName, `Connecting to ${url} realm=${REALM} ...`);

    const wampy = new Wampy(url, {
        realm: REALM,
        transport: transportName,
        autoReconnect: false,
        debug: false,
        onClose: () => log(transportName, 'Session closed'),
        onError: (err) => log(transportName, 'ERROR', err),
    });

    // 1. Connect
    await wampy.connect();
    log(transportName, `Connected! sessionId=${wampy.getSessionId()}`);

    // 2. Register an RPC
    let rpcCalled = false;
    await wampy.register(RPC, (invocation) => {
        rpcCalled = true;
        const [a, b] = invocation.argsList;
        log(transportName, `RPC invoked: ${a} + ${b} = ${a + b}`);
        return { argsList: [a + b] };
    });
    log(transportName, `Registered RPC "${RPC}"`);

    // 3. Subscribe to a topic
    let eventReceived = false;
    const sub = await wampy.subscribe(TOPIC, (eventArgs) => {
        eventReceived = true;
        log(transportName, `Event received:`, eventArgs);
    });
    log(transportName, `Subscribed to "${TOPIC}" (subscriptionId=${sub.subscriptionId})`);

    // 4. Publish an event
    await wampy.publish(TOPIC, { argsList: ['hello from ' + transportName] }, { exclude_me: false });
    log(transportName, `Published to "${TOPIC}"`);

    // Give Bondy a moment to deliver the event
    await sleep(1000);

    // 5. Call the RPC we registered
    const result = await wampy.call(RPC, { argsList: [17, 25] });
    log(transportName, `RPC result:`, result);

    // 6. Unsubscribe & unregister
    await wampy.unsubscribe(sub.subscriptionKey);
    log(transportName, `Unsubscribed from "${TOPIC}"`);

    await wampy.unregister(RPC);
    log(transportName, `Unregistered RPC "${RPC}"`);

    // 7. Disconnect
    await wampy.disconnect();
    log(transportName, 'Disconnected');

    // Summary
    console.log();
    log(transportName, '--- Results ---');
    log(transportName, `  RPC called:     ${rpcCalled ? 'YES' : 'NO'}`);
    log(transportName, `  Event received: ${eventReceived ? 'YES' : 'NO'}`);
    console.log();

    return { rpcCalled, eventReceived };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
    console.log('='.repeat(60));
    console.log('  wampy.js — Bondy HTTP Transport Test');
    console.log('  Longpoll URL: ', LONGPOLL_URL);
    console.log('  SSE URL:      ', SSE_URL);
    console.log('  Realm:        ', REALM);
    console.log('='.repeat(60));
    console.log();

    const results = {};

    for (const transport of ['longpoll', 'sse']) {
        try {
            results[transport] = await runTest(transport);
        } catch (err) {
            log(transport, 'FAILED:', err.message || err);
            if (err.stack) console.error(err.stack);
            results[transport] = { error: err.message || String(err) };
        }
        console.log('-'.repeat(60));
    }

    // Final summary
    console.log();
    console.log('='.repeat(60));
    console.log('  SUMMARY');
    console.log('='.repeat(60));
    for (const [t, r] of Object.entries(results)) {
        if (r.error) {
            console.log(`  ${t.padEnd(10)} FAIL  — ${r.error}`);
        } else {
            const ok = r.rpcCalled && r.eventReceived;
            console.log(`  ${t.padEnd(10)} ${ok ? 'PASS' : 'PARTIAL'}  rpc=${r.rpcCalled} event=${r.eventReceived}`);
        }
    }
    console.log();
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
