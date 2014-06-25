package org.grimrose.socket.io;

/**
 * Redis Client Publisher
 */
public interface RedisPublisher {

    /**
     * @param channel binary channel name
     * @param message binary message
     * @return publish process result
     */
    Long publish(byte[] channel, byte[] message);

    /**
     * @param channel channel name
     * @param message string message
     * @return publish process result
     */
    Long publish(String channel, String message);

}
