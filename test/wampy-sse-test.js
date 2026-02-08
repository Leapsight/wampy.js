/**
 * Tests for HTTP SSE Transport
 */

import { expect } from 'chai';
import { Wampy } from '../src/wampy.js';
import { MockSSERouter } from './mock-sse-router.js';
import { WAMP_MSG_SPEC } from '../src/constants.js';

describe('Wampy.js HTTP SSE Transport', function () {
    this.timeout(10000);

    const testPort = 9877;
    const testUrl = `http://localhost:${testPort}`;
    let router;

    before(async function () {
        router = new MockSSERouter(testPort);
        await router.start();
    });

    after(async function () {
        await router.stop();
    });

    afterEach(function () {
        // Reset router state between tests
        router.onMessage = null;
    });

    describe('Connection Lifecycle', function () {

        it('should connect to server via SSE transport', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            await wampy.connect();
            expect(wampy.getSessionId()).to.be.a('number');

            await wampy.disconnect();
        });

        it('should receive WELCOME message after HELLO', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            await wampy.connect();

            const sessionId = wampy.getSessionId();
            expect(sessionId).to.be.a('number');
            expect(sessionId).to.be.greaterThan(0);

            await wampy.disconnect();
        });

        it('should disconnect gracefully', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            await wampy.connect();
            expect(wampy.getSessionId()).to.be.a('number');

            await wampy.disconnect();
            // After disconnect, sessionId is undefined (cache is reset)
            expect(wampy.getSessionId()).to.not.be.ok;
        });

        it('should call onClose callback on disconnect', async function () {
            let closeCalled = false;

            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse',
                onClose: () => { closeCalled = true; }
            });

            await wampy.connect();
            await wampy.disconnect();

            expect(closeCalled).to.be.true;
        });

    });

    describe('Pub/Sub Operations', function () {

        it('should subscribe to a topic', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            // Set up message handler to respond to SUBSCRIBE
            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.SUBSCRIBE) {
                    const reqId = msg[1];
                    const subscriptionId = 12345;
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.SUBSCRIBED,
                        reqId,
                        subscriptionId
                    ]);
                }
            };

            await wampy.connect();

            const result = await wampy.subscribe('com.example.topic', () => {});
            expect(result).to.be.an('object');

            await wampy.disconnect();
        });

        it('should receive published events via SSE stream', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            let receivedEvent = null;
            const subscriptionId = 12345;

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.SUBSCRIBE) {
                    const reqId = msg[1];
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.SUBSCRIBED,
                        reqId,
                        subscriptionId
                    ]);

                    // After subscription confirmed, send an event via SSE
                    setTimeout(() => {
                        router.queueMessage(transportId, [
                            WAMP_MSG_SPEC.EVENT,
                            subscriptionId,
                            99999, // publication ID
                            {},    // details
                            ['sse', 'event'], // args
                            { source: 'sse' } // kwargs
                        ]);
                    }, 100);
                }
            };

            await wampy.connect();

            await new Promise((resolve) => {
                // Event callback receives { details, argsList, argsDict }
                wampy.subscribe('com.example.topic', (eventData) => {
                    receivedEvent = eventData;
                    resolve();
                });
            });

            expect(receivedEvent).to.not.be.null;
            expect(receivedEvent.argsList).to.deep.equal(['sse', 'event']);
            expect(receivedEvent.argsDict).to.deep.equal({ source: 'sse' });

            await wampy.disconnect();
        });

        it('should publish to a topic', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            let publishReceived = false;

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.PUBLISH) {
                    publishReceived = true;
                    const reqId = msg[1];
                    const publicationId = 77777;
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.PUBLISHED,
                        reqId,
                        publicationId
                    ]);
                }
            };

            await wampy.connect();

            await wampy.publish('com.example.topic', ['test data'], { acknowledge: true });
            expect(publishReceived).to.be.true;

            await wampy.disconnect();
        });

    });

    describe('RPC Operations', function () {

        it('should register a procedure', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.REGISTER) {
                    const reqId = msg[1];
                    const registrationId = 54321;
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.REGISTERED,
                        reqId,
                        registrationId
                    ]);
                }
            };

            await wampy.connect();

            const result = await wampy.register('com.example.procedure', () => {
                return 'result';
            });
            expect(result).to.be.an('object');

            await wampy.disconnect();
        });

        it('should call a remote procedure', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.CALL) {
                    const reqId = msg[1];
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.RESULT,
                        reqId,
                        {},
                        ['sse result'],
                        { status: 'ok' }
                    ]);
                }
            };

            await wampy.connect();

            const result = await wampy.call('com.example.procedure', ['arg1', 'arg2']);
            expect(result).to.be.an('object');
            expect(result.argsDict).to.deep.equal({ status: 'ok' });

            await wampy.disconnect();
        });

        it('should handle RPC invocation via SSE stream', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            const registrationId = 54321;

            const yieldReceived = new Promise((resolve) => {
                router.onMessage = (transportId, msg) => {
                    if (msg[0] === WAMP_MSG_SPEC.REGISTER) {
                        const reqId = msg[1];
                        router.queueMessage(transportId, [
                            WAMP_MSG_SPEC.REGISTERED,
                            reqId,
                            registrationId
                        ]);

                        // Send an invocation via SSE after registration
                        setTimeout(() => {
                            router.queueMessage(transportId, [
                                WAMP_MSG_SPEC.INVOCATION,
                                88888, // request ID
                                registrationId,
                                {},    // details
                                ['sse invocation']
                            ]);
                        }, 100);
                    }

                    if (msg[0] === WAMP_MSG_SPEC.YIELD) {
                        resolve(true);
                    }
                };
            });

            await wampy.connect();

            // Register receives { argsList, argsDict, details }
            await wampy.register('com.example.procedure', (data) => {
                return data.argsList[0].toUpperCase();
            });

            // Wait for yield with timeout
            const result = await Promise.race([
                yieldReceived,
                new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout waiting for YIELD')), 2000))
            ]);

            expect(result).to.be.true;

            await wampy.disconnect();
        });

    });

    describe('SSE-Specific Features', function () {

        it('should receive multiple events in sequence via SSE', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'sse'
            });

            const receivedEvents = [];
            const subscriptionId = 12345;

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.SUBSCRIBE) {
                    const reqId = msg[1];
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.SUBSCRIBED,
                        reqId,
                        subscriptionId
                    ]);

                    // Send multiple events in quick succession
                    setTimeout(() => {
                        for (let i = 1; i <= 3; i++) {
                            router.queueMessage(transportId, [
                                WAMP_MSG_SPEC.EVENT,
                                subscriptionId,
                                90000 + i,
                                {},
                                [`event-${i}`]
                            ]);
                        }
                    }, 50);
                }
            };

            await wampy.connect();

            await new Promise((resolve) => {
                wampy.subscribe('com.example.topic', (eventData) => {
                    receivedEvents.push(eventData.argsList[0]);
                    if (receivedEvents.length === 3) {
                        resolve();
                    }
                });
            });

            expect(receivedEvents).to.deep.equal(['event-1', 'event-2', 'event-3']);

            await wampy.disconnect();
        });

    });

    describe('Error Handling', function () {

        it('should throw when realm is not specified', async function () {
            const wampy = new Wampy(testUrl, {
                transport: 'sse'
            });

            try {
                await wampy.connect();
                expect.fail('Should have thrown');
            } catch (e) {
                expect(e.message).to.include('realm');
            }
        });

        it('should handle connection failure gracefully', async function () {
            const wampy = new Wampy('http://localhost:59998', {
                realm: 'realm1',
                transport: 'sse'
            });

            try {
                await wampy.connect();
                expect.fail('Should have thrown');
            } catch (e) {
                expect(e).to.be.an('error');
            }
        });

    });

});
