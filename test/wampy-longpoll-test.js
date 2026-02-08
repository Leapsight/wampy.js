/**
 * Tests for HTTP Longpoll Transport
 */

import { expect } from 'chai';
import { Wampy } from '../src/wampy.js';
import { MockLongpollRouter } from './mock-longpoll-router.js';
import { WAMP_MSG_SPEC } from '../src/constants.js';

describe('Wampy.js HTTP Longpoll Transport', function () {
    this.timeout(10000);

    const testPort = 9876;
    const testUrl = `http://localhost:${testPort}`;
    let router;

    before(async function () {
        router = new MockLongpollRouter(testPort);
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

        it('should connect to server via longpoll transport', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'longpoll'
            });

            await wampy.connect();
            expect(wampy.getSessionId()).to.be.a('number');

            await wampy.disconnect();
        });

        it('should receive WELCOME message after HELLO', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'longpoll'
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
                transport: 'longpoll'
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
                transport: 'longpoll',
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
                transport: 'longpoll'
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

        it('should receive published events', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'longpoll'
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

                    // After subscription confirmed, send an event
                    setTimeout(() => {
                        router.queueMessage(transportId, [
                            WAMP_MSG_SPEC.EVENT,
                            subscriptionId,
                            99999, // publication ID
                            {},    // details
                            ['hello', 'world'], // args
                            { key: 'value' }    // kwargs
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
            expect(receivedEvent.argsList).to.deep.equal(['hello', 'world']);
            expect(receivedEvent.argsDict).to.deep.equal({ key: 'value' });

            await wampy.disconnect();
        });

        it('should publish to a topic', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'longpoll'
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
                transport: 'longpoll'
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
                transport: 'longpoll'
            });

            router.onMessage = (transportId, msg) => {
                if (msg[0] === WAMP_MSG_SPEC.CALL) {
                    const reqId = msg[1];
                    router.queueMessage(transportId, [
                        WAMP_MSG_SPEC.RESULT,
                        reqId,
                        {},
                        ['call result'],
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

        it('should handle RPC invocation', async function () {
            const wampy = new Wampy(testUrl, {
                realm: 'realm1',
                transport: 'longpoll'
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

                        // Send an invocation after registration
                        setTimeout(() => {
                            router.queueMessage(transportId, [
                                WAMP_MSG_SPEC.INVOCATION,
                                88888, // request ID
                                registrationId,
                                {},    // details
                                ['invocation arg']
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

    describe('Error Handling', function () {

        it('should throw when realm is not specified', async function () {
            const wampy = new Wampy(testUrl, {
                transport: 'longpoll'
            });

            try {
                await wampy.connect();
                expect.fail('Should have thrown');
            } catch (e) {
                expect(e.message).to.include('realm');
            }
        });

        it('should handle connection failure gracefully', async function () {
            const wampy = new Wampy('http://localhost:59999', {
                realm: 'realm1',
                transport: 'longpoll'
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
