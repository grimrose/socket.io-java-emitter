package org.grimrose.socket.io

import redis.clients.jedis.BinaryJedisPubSub
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class EmitterAndRedisSpec extends Specification {

    @Shared
    JedisPool pool

    def setupSpec() {
        pool = new JedisPool(new JedisPoolConfig(), "localhost");
    }

    def cleanupSpec() {
        pool.destroy()
    }


    def "should be able to access localhost Redis"() {
        setup:
        def redis = pool.resource

        when:
        redis.set("test", "one")

        then:
        redis.get("test") == "one"

        cleanup:
        pool.returnResource(redis)
    }


    @Timeout(5)
    def "should be able to emit string messages to subscriber"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        def pub = pool.resource
        def sub = pool.resource

        def subscriber = new Subscriber()

        def service = Executors.newFixedThreadPool(2)
        [
                {
                    pubLatch.await()

                    def emitter = SpecHelper.emitter(pub, Emitter.DEFAULT_KEY)
                    emitter.emit("broadcast event", "broadcast payload")
                },
                {
                    sub.subscribe(subscriber, Emitter.DEFAULT_KEY)
                    latch.countDown()
                },
        ].each { service.execute(it as Runnable) }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('broadcast event')
        subscriber.result.toString().contains('broadcast payload')

        cleanup:
        service.shutdown()
        pool.returnResource(pub)
        pool.returnResource(sub)
    }


    @Timeout(5)
    def "should be able to emit binary messages to subscriber"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        def pub = pool.resource
        def sub = pool.resource

        def subscriber = new BinSubscriber()

        def service = Executors.newFixedThreadPool(2)

        [
                {
                    pubLatch.await()

                    def emitter = SpecHelper.emitter(pub, Emitter.DEFAULT_KEY)
                    emitter.emit("AB".bytes, "海老".bytes)
                },
                {
                    sub.subscribe(subscriber, Emitter.DEFAULT_KEY.bytes, Emitter.DEFAULT_KEY.bytes)
                    latch.countDown()
                }
        ].each { service.execute(it as Runnable) }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('AB')
        subscriber.result.toString().contains('海老')

        cleanup:
        service.shutdown()
        pool.returnResource(pub)
        pool.returnResource(sub)
    }


    static class BinSubscriber extends BinaryJedisPubSub {

        def result = []

        @Override
        void onMessage(byte[] channel, byte[] message) {
            String value = new String(message)
            println value
            result << value
        }

        @Override
        void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
        }

        @Override
        void onSubscribe(byte[] channel, int subscribedChannels) {
        }

        @Override
        void onUnsubscribe(byte[] channel, int subscribedChannels) {
        }

        @Override
        void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
        }

        @Override
        void onPSubscribe(byte[] pattern, int subscribedChannels) {
        }
    }

}

