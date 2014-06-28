package org.grimrose.socket.io

import redis.clients.jedis.Jedis
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class EmitterAndNodeRedisSpec extends Specification {

    def setupSpec() {
        def process = ["node", "src/test/resources/node_redis_client.js"].execute()
        Runtime.runtime.addShutdownHook({
            process.destroy()
        })
    }

    @Timeout(5)
    def "should be able to emit messages to client"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        Thread.start {
            def redis = new Jedis("localhost")
            pubLatch.await()

            def emitter = SpecHelper.emitter(redis, Emitter.DEFAULT_KEY)
            emitter.emit("broadcast event", "broadcast payload")
        }

        def subscriber = new Subscriber()
        Thread.start {
            def redis = new Jedis("localhost")
            redis.subscribe(subscriber, "spock")
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
    def "should be able to emit messages to namespace"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        Thread.start {
            def redis = new Jedis("localhost")
            pubLatch.await()

            def emitter = SpecHelper.emitter(redis, Emitter.DEFAULT_KEY)
            emitter.of('/nsp').broadcast().emit('broadcast event', 'nsp broadcast payload')
        }

        def subscriber = new Subscriber()
        Thread.start {
            def redis = new Jedis("localhost")
            redis.subscribe(subscriber, "spock")
            latch.countDown()
        }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('nsp broadcast payload')
    }

}
