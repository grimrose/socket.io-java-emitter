package org.grimrose.socket.io

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class EmitterAndNodeRedisSpec extends Specification {

    @Shared
    JedisPool pool

    def setupSpec() {
        pool = new JedisPool(new JedisPoolConfig(), "localhost");

        def process = ["node", "src/test/resources/node_redis_client.js"].execute()
        Runtime.runtime.addShutdownHook({
            process.destroy()
        })
    }

    def cleanupSpec() {
        pool.destroy()
    }


    @Timeout(5)
    def "should be able to emit messages to client"() {
        given:
        def pubLatch = new CountDownLatch(1)
        def latch = new CountDownLatch(1)

        def subscriber = new Subscriber()

        def service = Executors.newFixedThreadPool(2)

        def pub = pool.resource
        def sub = pool.resource

        [
                {
                    pubLatch.await()

                    def emitter = SpecHelper.emitter(pub, Emitter.DEFAULT_KEY)
                    emitter.emit("broadcast event", "broadcast payload")
                },
                {
                    sub.subscribe(subscriber, "spock")
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
    def "should be able to emit messages to namespace"() {
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
                    emitter.of('/nsp').broadcast().emit('broadcast event', 'nsp broadcast payload')
                },
                {
                    sub.subscribe(subscriber, "spock")
                    latch.countDown()
                },
        ].each { service.execute(it as Runnable) }

        when:
        pubLatch.countDown()
        latch.await 3, TimeUnit.SECONDS
        subscriber.unsubscribe()

        then:
        subscriber.result.toString().contains('nsp broadcast payload')

        cleanup:
        service.shutdown()
        pool.returnResource(pub)
        pool.returnResource(sub)
    }

}
