package org.grimrose.socket.io;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * socket.id-emitter
 */
public class Emitter {

    private static final Logger logger = Logger.getLogger(Emitter.class.getName());

    /**
     * #emitter
     */
    public static final String SUFFIX_KEY = "#emitter";

    static final String DEFAULT_KEY = "socket.io#emitter";


    /**
     * 2
     */
    static final int EVENT = 2;
    /**
     * 5
     */
    static final int BINARY_EVENT = 5;

    private RedisPublisher redis;

    private String key;

    private Flags flags = new Flags();
    private List<String> rooms = new ArrayList<>();


    /**
     * Socket.IO redis based emitter.
     *
     * @param redis redis client
     * @param key the name of the key to pub/sub events
     * @return emitter
     */
    public static Emitter getInstance(RedisPublisher redis, String key) {
        if (key == null || key.isEmpty()) {
            key = DEFAULT_KEY;
        }
        if (!key.endsWith(SUFFIX_KEY)) {
            key = key.concat(SUFFIX_KEY);
        }
        return new Emitter(redis, key);
    }

    Emitter(RedisPublisher redis, String key) {
        this.redis = redis;
        this.key = key;
    }

    /**
     * Flags.
     */
    private static enum Flag {
        JSON, VOLATILE, BROADCAST;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    private static class Flags {
        private String nsp;

        private Map<Flag, Boolean> flags = new HashMap<>();

        public void setFlag(Flag flag) {
            flags.put(flag, true);
        }

        public void set(String nsp) {
            this.nsp = nsp;
        }

        public boolean hasNsp() {
            return nsp != null && !nsp.isEmpty();
        }

        public String getNsp() {
            return nsp;
        }

        public void deleteNsp() {
            this.nsp = null;
        }

        public Map<String, Object> asMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("nsp", nsp);
            for (Map.Entry<Flag, Boolean> entry : flags.entrySet()) {
                map.put(entry.getKey().toString(), entry.getValue());
            }
            return map;
        }
    }

    /**
     * Apply flags from `Socket`.
     *
     * @return emitter
     */
    public Emitter json() {
        return get(Flag.JSON);
    }

    /**
     * Apply flags from `Socket`.
     *
     * @return emitter
     */
    public Emitter _volatile() {
        return get(Flag.VOLATILE);
    }

    /**
     * Apply flags from `Socket`.
     *
     * @return emitter
     */
    public Emitter broadcast() {
        return get(Flag.BROADCAST);
    }

    private Emitter get(Flag flag) {
        logger.fine(String.format("flag %s on", flag));
        flags.setFlag(flag);
        return this;
    }

    /**
     * Limit emission to a certain `room`.
     *
     * @param room room
     * @return emitter
     */
    public Emitter in(String room) {
        if (!rooms.contains(room)) {
            logger.fine(String.format("room %s", room));
            rooms.add(room);
        }
        return this;
    }

    /**
     * Limit emission to a certain `room`.
     *
     * @param room room
     * @return emitter
     */
    public Emitter to(String room) {
        return in(room);
    }

    /**
     * Limit emission to certain `namespace`.
     *
     * @param nsp namespace
     * @return emitter
     */
    public Emitter of(String nsp) {
        logger.fine(String.format("nsp set to %s", nsp));
        flags.set(nsp);
        return this;
    }

    /**
     * Send the packet.
     *
     * @param args arguments
     * @return emitter
     * @throws IOException
     */
    public Emitter emit(Object... args) throws IOException {
        byte[] channel = key.getBytes();
        byte[] message = toMessage(toPacket(args));

        // publish
        redis.publish(channel, message);

        // reset state
        rooms.clear();
        flags = new Flags();
        return this;
    }

    /**
     * Send the packet.
     *
     * @param args arguments
     * @return emitter
     * @throws IOException
     */
    public Emitter emit(String... args) throws IOException {
        String channel = key;
        String message = new String(toMessage(toPacket(args)), StandardCharsets.UTF_8);

        // publish
        redis.publish(channel, message);

        // reset state
        rooms.clear();
        flags = new Flags();
        return this;
    }

    /**
     * @param args arguments
     * @return packet
     * @throws IOException
     */
    Map<String, Object> toPacket(Object[] args) throws IOException {
        Map<String, Object> packet = new HashMap<>();

        int type = hasBinary(args) ? BINARY_EVENT : EVENT;
        packet.put("type", type);

        // set namespace to packet
        if (flags.hasNsp()) {
            packet.put("nsp", flags.getNsp());
            flags.deleteNsp();
        } else {
            packet.put("nsp", "/");
        }
        if (args instanceof String[]) {
            packet.put("data", args);
            return packet;
        }

        MessagePack messagePack = new MessagePack();
        BufferPacker packer = messagePack.createBufferPacker();
        for (Object arg : args) {
            packer.write(arg);
        }
        packet.put("data", packer.toByteArray());
        return packet;
    }

    /**
     * @param packet packet
     * @return message
     * @throws IOException
     */
    byte[] toMessage(Map<String, Object> packet) throws IOException {
        List<Object> objects = new ArrayList<>();
        objects.add(packet);

        Map<String, Object> map = new HashMap<>();
        map.put("rooms", rooms);
        map.put("flags", flags.asMap());

        objects.add(map);

        MessagePack messagePack = new MessagePack();
        BufferPacker packer = messagePack.createBufferPacker();
        for (Object o : objects) {
            packer.write(o);
        }
        return packer.toByteArray();
    }

    boolean hasBinary(Object o) {
        if (o == null) return false;
        if (o instanceof byte[]) return true;
        if (o instanceof Buffer) return true;
        if (o instanceof String) {
            CharsetEncoder encoder = StandardCharsets.US_ASCII.newEncoder();
            return !encoder.canEncode(o.toString());
        }
        // TODO detect JSON
        // TODO recursive Check For Binary
        return false;
    }

}
