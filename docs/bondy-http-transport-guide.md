# Bondy HTTP Transport Client Guide

This guide covers the two HTTP-based WAMP transports supported by Bondy: **HTTP Longpoll** and **HTTP Longpoll-SSE** (Server-Sent Events). Both transports expose a REST-like API for establishing WAMP sessions over plain HTTP, without requiring WebSocket support.

---

## Table of Contents

1. [WAMP Message Reference](#1-wamp-message-reference)
2. [HTTP Longpoll Transport](#2-http-longpoll-transport)
   - [curl Examples](#21-curl-examples)
   - [wampy.js Examples](#22-wampyjs-examples)
   - [Message Queuing During Receive Gaps](#23-message-queuing-during-receive-gaps)
3. [HTTP Longpoll-SSE Transport](#3-http-longpoll-sse-transport)
   - [curl Examples](#31-curl-examples)
   - [wampy.js Examples](#32-wampyjs-examples)
4. [Compression (gzip)](#4-compression-gzip)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. WAMP Message Reference

| Code | Message        | Direction         | Description                            |
|------|----------------|-------------------|----------------------------------------|
| 1    | HELLO          | Client → Router   | Initiate a WAMP session               |
| 2    | WELCOME        | Router → Client   | Session established                    |
| 3    | ABORT          | Router → Client   | Session rejected or aborted            |
| 4    | CHALLENGE      | Router → Client   | Authentication challenge               |
| 5    | AUTHENTICATE   | Client → Router   | Authentication response                |
| 6    | GOODBYE        | Bidirectional     | Graceful session close                 |
| 8    | ERROR          | Router → Client   | Error response to a request            |
| 16   | PUBLISH        | Client → Router   | Publish an event to a topic            |
| 17   | PUBLISHED      | Router → Client   | Acknowledge publication                |
| 32   | SUBSCRIBE      | Client → Router   | Subscribe to a topic                   |
| 33   | SUBSCRIBED     | Router → Client   | Subscription confirmed                 |
| 34   | UNSUBSCRIBE    | Client → Router   | Unsubscribe from a topic              |
| 35   | UNSUBSCRIBED   | Router → Client   | Unsubscription confirmed               |
| 36   | EVENT          | Router → Client   | Deliver a published event              |
| 48   | CALL           | Client → Router   | Call a remote procedure                |
| 49   | CANCEL         | Client → Router   | Cancel a pending call                  |
| 50   | RESULT         | Router → Client   | Return RPC result                      |
| 64   | REGISTER       | Client → Router   | Register a procedure                   |
| 65   | REGISTERED     | Router → Client   | Registration confirmed                 |
| 66   | UNREGISTER     | Client → Router   | Unregister a procedure                 |
| 67   | UNREGISTERED   | Router → Client   | Unregistration confirmed               |
| 68   | INVOCATION     | Router → Client   | Invoke a registered procedure          |
| 69   | INTERRUPT      | Router → Client   | Interrupt a pending invocation         |
| 70   | YIELD          | Client → Router   | Return invocation result               |

---

## 2. HTTP Longpoll Transport

The longpoll transport uses HTTP POST requests for both upstream (client → router) and downstream (router → client) communication. The client polls for messages by repeatedly POSTing to the `/receive` endpoint; Bondy holds the request open until a message is available or the timeout expires.

**Base URL:** `http://<host>:<port>/wamp/longpoll`
**Protocol:** `wamp.2.json`

### 2.1 curl Examples

#### Step 1: Open a Transport Session

```bash
curl -s -X POST http://localhost:18080/wamp/longpoll/open \
  -H "Content-Type: application/json" \
  -d '{"protocols": ["wamp.2.json"]}'
```

Response:

```json
{"protocol": "wamp.2.json", "transport": "<TRANSPORT_ID>"}
```

Save the `transport` value — it is the Transport ID (`TID`) used in all subsequent requests.

```bash
TID=<TRANSPORT_ID>
```

#### Step 2: Send HELLO

```bash
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[1, "com.leapsight.test", {"roles": {"caller": {}, "callee": {}, "publisher": {}, "subscriber": {}}}]'
```

> **Note:** Include all four roles (`caller`, `callee`, `publisher`, `subscriber`) to enable full WAMP functionality.

#### Step 3: Receive WELCOME

```bash
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

Expected response (WELCOME, code 2):

```json
[2, <SESSION_ID>, {"roles": {"broker": {}, "dealer": {}}, ...}]
```

#### Step 4: Subscribe to a Topic

```bash
# Send SUBSCRIBE [32, RequestId, Options, Topic]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[32, 1, {}, "com.example.topic"]'

# Receive SUBSCRIBED [33, RequestId, SubscriptionId]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 5: Publish to the Topic

```bash
# Send PUBLISH [16, RequestId, Options, Topic, ArgsList]
# Use exclude_me=false to receive your own event
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[16, 2, {"acknowledge": true, "exclude_me": false}, "com.example.topic", ["hello"]]'

# Receive PUBLISHED [17, RequestId, PublicationId]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 6: Receive the EVENT

```bash
# Receive EVENT [36, SubscriptionId, PublicationId, Details, ArgsList]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 7: Register an RPC

```bash
# Send REGISTER [64, RequestId, Options, Procedure]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[64, 3, {}, "com.example.add2"]'

# Receive REGISTERED [65, RequestId, RegistrationId]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 8: Call an RPC (from another session or self-call)

```bash
# Send CALL [48, RequestId, Options, Procedure, ArgsList]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[48, 4, {}, "com.example.add2", [17, 25]]'

# Receive INVOCATION [68, RequestId, RegistrationId, Details, ArgsList]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 9: Yield the RPC Result

```bash
# Send YIELD [70, InvocationRequestId, Options, ArgsList]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[70, <INVOCATION_REQUEST_ID>, {}, [42]]'

# Receive RESULT [50, CallRequestId, Details, ArgsList]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive
```

#### Step 10: Graceful Disconnect (GOODBYE)

```bash
# Send GOODBYE [6, Details, Reason]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/send \
  -H "Content-Type: application/json" \
  -d '[6, {}, "wamp.close.normal"]'

# Receive GOODBYE [6, Details, Reason]
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive

# Close the transport
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/close
```

> **Important:** GOODBYE is message code **6**, not 3. Code 3 is ABORT, which the router sends to reject a session (e.g., during HELLO).

### 2.2 wampy.js Examples

#### Connect, Subscribe, Publish, Call, Disconnect

```javascript
import { Wampy } from 'wampy';

const wampy = new Wampy('http://localhost:18080/wamp/longpoll', {
    realm: 'com.leapsight.test',
    transport: 'longpoll',
    autoReconnect: false,
    onClose: () => console.log('Session closed'),
    onError: (err) => console.error('Error:', err),
});

// Connect
await wampy.connect();
console.log('Session ID:', wampy.getSessionId());

// Register an RPC
await wampy.register('com.example.add2', (invocation) => {
    const [a, b] = invocation.argsList;
    return { argsList: [a + b] };
});

// Subscribe to a topic
await wampy.subscribe('com.example.topic', (event) => {
    console.log('Event received:', event.argsList);
});

// Publish (exclude_me: false to receive own event)
await wampy.publish(
    'com.example.topic',
    { argsList: ['hello from longpoll'] },
    { exclude_me: false }
);

// Call an RPC
const result = await wampy.call('com.example.add2', { argsList: [17, 25] });
console.log('RPC result:', result.argsList); // [42]

// Disconnect
await wampy.disconnect();
```

> **Note:** The `register()` callback receives an invocation object with `{ argsList, argsDict, details }` — not a plain array.

### 2.3 Message Queuing During Receive Gaps

With the longpoll transport, there is necessarily a gap between when a `/receive` response is delivered and when the next `/receive` request is issued. During this gap, the router **queues** any messages destined for the client. The next `/receive` call will return one queued message. The client must continue polling until all queued messages have been consumed.

This is important when multiple events or invocations may arrive in quick succession — the client will receive them one at a time, one per `/receive` call.

**curl demonstration:**

```bash
# 1. Subscribe to a topic (as above)
# 2. Stop polling (don't call /receive)
# 3. From another session, publish 3 events to that topic
# 4. Resume polling:
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive  # → event 1
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive  # → event 2
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive  # → event 3
```

---

## 3. HTTP Longpoll-SSE Transport

The SSE transport uses **Server-Sent Events** for downstream (router → client) messages and **HTTP POST** for upstream (client → router) messages. This eliminates the need for polling — the router pushes messages to the client over a persistent SSE stream.

**Base URL:** `http://<host>:<port>/wamp/sse`
**Protocol:** `wamp.2.json.sse`

> **Warning:** The SSE transport uses a **different** base URL (`/wamp/sse`) and a **different** protocol identifier (`wamp.2.json.sse`) than the longpoll transport. Using `/wamp/longpoll` or `wamp.2.json` with SSE will result in a 400 error.

### 3.1 curl Examples

#### Step 1: Open a Transport Session

```bash
curl -s -X POST http://localhost:18080/wamp/sse/open \
  -H "Content-Type: application/json" \
  -d '{"protocols": ["wamp.2.json.sse"]}'
```

Response:

```json
{"protocol": "wamp.2.json.sse", "transport": "<TRANSPORT_ID>"}
```

```bash
TID=<TRANSPORT_ID>
```

#### Step 2: Open the SSE Stream (in a separate terminal)

```bash
curl -s -N -H "Accept: text/event-stream" \
  http://localhost:18080/wamp/sse/$TID/receive
```

This connection stays open. Messages arrive as SSE events with `event: wamp`:

```
event: wamp
data: [2, <SESSION_ID>, {"roles": {...}}]

event: wamp
data: [36, <SubscriptionId>, <PublicationId>, {}, ["hello"]]
```

#### Step 3: Send HELLO (in the original terminal)

```bash
curl -s -X POST http://localhost:18080/wamp/sse/$TID/send \
  -H "Content-Type: application/json" \
  -d '[1, "com.leapsight.test", {"roles": {"caller": {}, "callee": {}, "publisher": {}, "subscriber": {}}}]'
```

The WELCOME message will appear on the SSE stream (Step 2 terminal).

#### Step 4–10: Same WAMP Messages as Longpoll

All subsequent WAMP messages (SUBSCRIBE, PUBLISH, REGISTER, CALL, YIELD, GOODBYE) are sent the same way — POST to `/wamp/sse/$TID/send`. The only difference is that responses arrive on the SSE stream instead of via `/receive` polling.

#### Close

```bash
# Send GOODBYE
curl -s -X POST http://localhost:18080/wamp/sse/$TID/send \
  -H "Content-Type: application/json" \
  -d '[6, {}, "wamp.close.normal"]'

# Close the transport
curl -s -X POST http://localhost:18080/wamp/sse/$TID/close
```

### 3.2 wampy.js Examples

```javascript
import { Wampy } from 'wampy';

const wampy = new Wampy('http://localhost:18080/wamp/sse', {
    realm: 'com.leapsight.test',
    transport: 'sse',
    autoReconnect: false,
    onClose: () => console.log('Session closed'),
    onError: (err) => console.error('Error:', err),
});

// Connect
await wampy.connect();
console.log('Session ID:', wampy.getSessionId());

// Register an RPC
await wampy.register('com.example.add2', (invocation) => {
    const [a, b] = invocation.argsList;
    return { argsList: [a + b] };
});

// Subscribe to a topic
await wampy.subscribe('com.example.topic', (event) => {
    console.log('Event received:', event.argsList);
});

// Publish
await wampy.publish(
    'com.example.topic',
    { argsList: ['hello from sse'] },
    { exclude_me: false }
);

// Call an RPC
const result = await wampy.call('com.example.add2', { argsList: [17, 25] });
console.log('RPC result:', result.argsList); // [42]

// Disconnect
await wampy.disconnect();
```

> **Note:** The only difference from the longpoll example is the URL (`/wamp/sse`) and transport type (`'sse'`). Wampy.js automatically negotiates the correct protocol (`wamp.2.json.sse`).

---

## 4. Compression (gzip)

Both transports support gzip compression when negotiated via HTTP headers.

### Longpoll + gzip

For the longpoll transport, Node.js `fetch` handles `Accept-Encoding: gzip` and decompression transparently. No special client configuration is needed — simply ensure your HTTP client sends the `Accept-Encoding` header (most do by default).

```bash
# Verify gzip is working with curl
curl -s -X POST http://localhost:18080/wamp/longpoll/$TID/receive \
  -H "Accept-Encoding: gzip" \
  --compressed
```

### SSE + gzip

For the SSE transport, wampy.js's built-in `NodeSSEClient` sends `Accept-Encoding: gzip, deflate` and decompresses the SSE stream using Node.js `zlib`:

```javascript
// No special configuration needed — gzip is negotiated automatically
const wampy = new Wampy('http://localhost:18080/wamp/sse', {
    realm: 'com.leapsight.test',
    transport: 'sse',
});
await wampy.connect();
// The SSE stream is transparently decompressed if the server sends gzip
```

```bash
# Verify gzip on the SSE stream with curl
curl -s -N \
  -H "Accept: text/event-stream" \
  -H "Accept-Encoding: gzip" \
  --compressed \
  http://localhost:18080/wamp/sse/$TID/receive
```

---

## 5. Troubleshooting

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| **400 Bad Request** on SSE `/open` | Wrong URL or protocol | Use `/wamp/sse` (not `/wamp/longpoll`) with protocol `wamp.2.json.sse` |
| **`procedure_already_exists`** error | Stale registration from a previous crashed session | Use unique procedure names (e.g., append a timestamp suffix) or wait for the previous registration to expire |
| **Longpoll receive returns empty** | No messages pending; normal timeout | Continue polling — this is expected behavior |
| **SSE stream closes unexpectedly** | Transport session expired or server-side timeout | Reconnect: open a new transport session |
| **Events not received after subscribe** | Publish happened before SUBSCRIBED was confirmed | Always `await` the subscribe operation before publishing |
| **Only 1 of N queued messages received** | Single message per `/receive` call (longpoll) | Keep polling — each `/receive` returns one queued message |

### Key Differences: Longpoll vs SSE

| Feature | Longpoll | SSE |
|---------|----------|-----|
| Base URL | `/wamp/longpoll` | `/wamp/sse` |
| Protocol | `wamp.2.json` | `wamp.2.json.sse` |
| Downstream | POST `/receive` (polling) | GET `/receive` (SSE stream) |
| Upstream | POST `/send` | POST `/send` |
| Latency | Higher (polling interval) | Lower (server push) |
| Message delivery | One per `/receive` call | Pushed immediately on stream |
| Connection count | 1 per request | 1 persistent + 1 per send |

### WAMP Message Format Quick Reference

```
HELLO:        [1, "realm", {"roles": {...}}]
WELCOME:      [2, session_id, {"roles": {...}}]
ABORT:        [3, {"message": "..."}, "wamp.error.reason"]
GOODBYE:      [6, {}, "wamp.close.normal"]
SUBSCRIBE:    [32, request_id, {}, "topic.uri"]
SUBSCRIBED:   [33, request_id, subscription_id]
UNSUBSCRIBE:  [34, request_id, subscription_id]
PUBLISH:      [16, request_id, {"acknowledge": true}, "topic.uri", args_list]
PUBLISHED:    [17, request_id, publication_id]
EVENT:        [36, subscription_id, publication_id, {}, args_list]
REGISTER:     [64, request_id, {}, "procedure.uri"]
REGISTERED:   [65, request_id, registration_id]
CALL:         [48, request_id, {}, "procedure.uri", args_list]
INVOCATION:   [68, request_id, registration_id, {}, args_list]
YIELD:        [70, request_id, {}, args_list]
RESULT:       [50, request_id, {}, args_list]
```
