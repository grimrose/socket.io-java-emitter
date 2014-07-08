package org.grimrose.socket.io


import java.lang.Long

import akka.actor._
import akka.testkit._
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
      val pub = system.actorOf(Props[Pub])
      val sub = system.actorOf(Props(classOf[Sub], testActor))

      var messages = Seq[String]()
      within(4.second) {
        sub ! SubscribeCommand(List(Emitter.DEFAULT_KEY))
        awaitAssert(expectMsg(true), 1.second)

        pub ! PublishCommand(Emitter.DEFAULT_KEY, "hello")
        awaitAssert(expectMsg(true), 2.second)

        receiveWhile(4.second) {
          case msg: String => messages = msg +: messages
        }
      }
      sub ! UnSubscribeCommand

      messages.length should be(1)
      messages should be(Seq("hello"))

      pub ! QuitCommand
    }
  }

  "Emitter" should {
    "be able to emit string messages to subscriber" in {
      val emit = system.actorOf(Props[Emit])

      val probe = TestProbe()
      val sub = system.actorOf(Props(classOf[Sub], probe.ref))

      within(5.second) {
        sub ! SubscribeCommand(List(Emitter.DEFAULT_KEY))
        awaitCond(expectMsg(true))

        emit ! EmitCommand
        awaitCond(expectMsg(true))

        val message = probe.receiveOne(1.second).toString
        message should include("broadcast event")
        message should include("broadcast payload")
      }

      sub ! UnSubscribeCommand
      emit ! QuitCommand
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

  case object EmitCommand

  case class SubscribeCommand(channels: List[String])

  case object UnSubscribeCommand

  case object QuitCommand


  class Pub extends Actor with ActorLogging {
    val redis = new RedisClient()

    val publisher = context.actorOf(Props(new Publisher(redis)))

    def receive = {
      case PublishCommand(channel, message) =>
        publisher ! Publish(channel, message)
        sender ! true

      case QuitCommand =>
        redis.quit
        sender ! true

      case x =>
        log.debug("receive in Pub: " + x)
        sender ! x
    }

    override def preStart() = {
      log.debug("start Pub.")
    }

    override def postStop() = {
      redis.quit
      log.debug("stop Pub.")
    }

  }

  class Emit extends Actor with ActorLogging with RedisPublisher {
    val redis = new RedisClient()

    val emitter = Emitter.getInstance(this, Emitter.DEFAULT_KEY)

    def receive = {
      case EmitCommand =>
        emitter.emit("broadcast event", "broadcast payload")
        log.debug("emit")
        sender ! true

      case QuitCommand =>
        redis.quit
        sender ! true

      case x =>
        log.debug("receive in Emit: " + x)
        sender ! x
    }

    override def publish(channel: Array[Byte], message: Array[Byte]): Long = {
      throw new UnsupportedOperationException("scala redis client does not support binary publishing.")
    }

    override def publish(channel: String, message: String): Long = {
      redis.publish(channel, message) match {
        case Some(x) => x
        case None => -1
      }
    }

    override def preStart() = {
      log.debug("start Emit.")
    }

    override def postStop() = {
      redis.quit
      log.debug("stop Emit.")
    }

  }

  class Sub(next: ActorRef) extends Actor with ActorLogging {
    val redis = new RedisClient()

    val subscriber = context.actorOf(Props(new com.redis.Subscriber(redis)))
    subscriber ! Register(callback)

    def receive = {
      case SubscribeCommand(channels) =>
        subscriber ! Subscribe(channels.toArray)
        sender ! true

      case UnSubscribeCommand =>
        subscriber ! UnsubscribeAll
        sender ! true

      case x =>
        log.debug("receive in Sub: " + x)
        sender ! x
    }

    def callback(pubsub: PubSubMessage) = pubsub match {
      case E(exception) => exception.printStackTrace()
      case S(channel, count) => log.debug("sub   channel:" + channel + "\tcount: " + count)
      case U(channel, count) => log.debug("unsub channel:" + channel + "\tcount: " + count)
      case M(channel, msg) =>
        log.info("channel : " + channel + "\tmessage : " + msg)
        next ! msg
    }

    override def preStart() = {
      log.debug("start Sub.")
    }

    override def postStop() = {
      redis.quit
      log.debug("stop Sub.")
    }
  }

}
