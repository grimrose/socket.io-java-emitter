var redis = require('redis');

var subscriber = redis.createClient();
var publisher = redis.createClient();

subscriber.on("error", function (err) {
    console.error("Subscribe Error: " + err);
});

publisher.on("error", function (err) {
    console.error("Publish Error: " + err);
});

subscriber.on("message", function (channel, message) {
    console.log({
        channel: channel,
        message: message
    });
    publisher.publish("spock", message);
});

subscriber.subscribe("socket.io#emitter");
