package org.grimrose.socket.io


import akka.actor._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.redis._
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class EmitterAndScalaRedisSpec
  extends TestKit(ActorSystem("EmitterAndScalaRedisSpec", ConfigFactory.parseString(EmitterAndScalaRedisSpec.config)))
  with DefaultTimeout with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  import org.grimrose.socket.io.EmitterAndScalaRedisSpec._

  val sub = system.actorOf(Props(classOf[Sub], testActor))
  val pub = system.actorOf(Props[Pub])


  override def afterAll() = {
    shutdown()
  }

  "Localhost Redis" should {
    " be able to access" in {
      val redis = new RedisClient()
      try {
        redis.set("hello", "scala-redis")
        redis.get("hello") match {
          case Some(x) => x should be("scala-redis")
          case None => fail("should return scala-redis")
        }
      } finally {
        redis.disconnect
      }
    }
  }

  "Redis Subscriber" should {
    "be subscribed message" in {
      var messages = Seq[String]()
      within(6.second) {
        sub ! SubscribeCommand(List(Emitter.DEFAULT_KEY))
        awaitAssert(expectMsg(true), 1.second, 100.millisecond)
        pub ! PublishCommand(Emitter.DEFAULT_KEY, "hello")
        awaitAssert(expectMsg("hello"), 2.second, 100.millisecond)

        sub ! UnSubCommand(List(Emitter.DEFAULT_KEY))
        awaitAssert(expectMsg(true), 2.second, 100.millisecond)

        receiveWhile(6.second) {
          case msg: String => messages = msg +: messages
        }
      }
      messages.length should be(1)
      messages should be(Seq("hello"))

      pub ! ShutdownCommand
      sub ! ShutdownCommand
    }
  }

}

object EmitterAndScalaRedisSpec {

  val config = """
    akka {
      loglevel = "DEBUG"
    }
               """

  case class PublishCommand(channel: String, message: String)

  case class SubscribeCommand(channels: List[String])

  case class UnSubCommand(channels: List[String])

  case object ShutdownCommand


  class Pub extends Actor with ActorLogging {
    val redis = new RedisClient()

    val publisher = context.actorOf(Props(new Publisher(redis)), "publisher")

    def receive = {
      case PublishCommand(channel, message) =>
        publish(channel, message)
        sender ! message

      case ShutdownCommand =>
        redis.quit
        sender ! true

      case x =>
        log.debug("receive in Pub " + x)
        sender ! x
    }

    override def postStop() {
      redis.disconnect
    }

    def publish(channel: String, message: String) = {
      publisher ! Publish(channel, message)
    }
  }

  class Sub(next: ActorRef) extends Actor with ActorLogging {
    val redis = new RedisClient()

    val subscriber = context.actorOf(Props(new com.redis.Subscriber(redis)), "subscriber")
    subscriber ! Register(callback)

    def receive = {
      case SubscribeCommand(channels) =>
        subscribe(channels)
        sender ! true

      case UnSubCommand(channels) =>
        unsubscribe(channels)
        sender ! true

      case ShutdownCommand =>
        redis.quit
        sender ! true

      case x =>
        log.debug("receive in Sub " + x)
        sender ! x
    }

    override def postStop() {
      redis.disconnect
    }

    def subscribe(channels: List[String]) = {
      subscriber ! Subscribe(channels.toArray)
    }

    def unsubscribe(channels: List[String]) = {
      subscriber ! Unsubscribe(channels.toArray)
    }

    def callback(pubsub: PubSubMessage) = pubsub match {
      case E(exception) => exception.printStackTrace()
      case S(channel, count) => log.debug("sub   channel:" + channel + "\tcount: " + count)
      case U(channel, count) => log.debug("unsub channel:" + channel + "\tcount: " + count)
      case M(channel, msg) =>
        log.info("channel : " + channel + "\tmessage : " + msg)
        next ! msg
    }

  }

}
