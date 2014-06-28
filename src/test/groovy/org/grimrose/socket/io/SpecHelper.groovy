package org.grimrose.socket.io

import redis.clients.jedis.Jedis

class SpecHelper {

    static Emitter emitter(Jedis redis, String key) {
        def publisher = new RedisPublisher() {
            @Override
            Long publish(byte[] channel, byte[] message) {
                redis.publish(channel, message)
            }

            @Override
            Long publish(String channel, String message) {
                redis.publish(channel, message)
            }
        }
        Emitter.getInstance(publisher, key)
    }

}
