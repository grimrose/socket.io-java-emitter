package org.grimrose.socket.io

import redis.clients.jedis.BinaryJedisPubSub
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class EmitterAndRedisSpec extends Specification {

    def "should be able to access localhost Redis"() {
        setup:
        def redis = new Jedis("localhost")

        when:
        redis.set("test", "one")

        then:
        redis.get("test") == "one"

        cleanup:
        redis?.quit()
    }


    @Timeout(5)
    def "should be able to emit string messages to subscriber"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        Thread.start {
            def redis = new Jedis("localhost")
            pubLatch.await()

            def emitter = Emitter.getInstance(publisherOf(redis), Emitter.DEFAULT_KEY)
            emitter.emit("broadcast event", "broadcast payload")
        }

        def subscriber = new Subscriber()
        Thread.start {
            def redis = new Jedis("localhost")
            redis.subscribe(subscriber, Emitter.DEFAULT_KEY)
            latch.countDown()
        }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('broadcast event')
        subscriber.result.toString().contains('broadcast payload')
    }


    @Timeout(5)
    def "should be able to emit binary messages to subscriber"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        Thread.start {
            def redis = new Jedis("localhost")
            pubLatch.await()

            def emitter = Emitter.getInstance(publisherOf(redis), Emitter.DEFAULT_KEY)
            emitter.emit("AB".bytes, "海老".bytes)
        }

        def subscriber = new BinSubscriber()
        Thread.start {
            def redis = new Jedis("localhost")
            redis.subscribe(subscriber, Emitter.DEFAULT_KEY.bytes, Emitter.DEFAULT_KEY.bytes)
            latch.countDown()
        }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('AB')
        subscriber.result.toString().contains('海老')
    }


    def publisherOf = { Jedis redis ->
        new RedisPublisher() {
            @Override
            Long publish(byte[] channel, byte[] message) {
                redis.publish(channel, message)
            }

            @Override
            Long publish(String channel, String message) {
                redis.publish(channel, message)
            }
        }
    }

    
    static class Subscriber extends JedisPubSub {

        def result = []

        @Override
        void onMessage(String channel, String message) {
            println message
            result << message
        }

        @Override
        void onPMessage(String pattern, String channel, String message) {
        }

        @Override
        void onSubscribe(String channel, int subscribedChannels) {
        }

        @Override
        void onUnsubscribe(String channel, int subscribedChannels) {
        }

        @Override
        void onPUnsubscribe(String pattern, int subscribedChannels) {
        }

        @Override
        void onPSubscribe(String pattern, int subscribedChannels) {
        }
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

