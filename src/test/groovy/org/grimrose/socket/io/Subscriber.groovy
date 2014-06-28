package org.grimrose.socket.io

import redis.clients.jedis.JedisPubSub

class Subscriber extends JedisPubSub {

    def result = []

    @Override
    void onMessage(String channel, String message) {
        println "channel: $channel\tmessage: $message"
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
