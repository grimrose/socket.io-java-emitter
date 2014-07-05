package org.grimrose.socket.io

import com.redis.RedisClient
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class EmitterAndScalaRedisSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val redis = new RedisClient()

  override def afterEach = {
    redis.flushdb
  }

  override def afterAll = {
    redis.disconnect
  }

  describe("access localhost Redis") {
    it("should be able to access") {
      redis.set("hello", "scala-redis")
      redis.get("hello") match {
        case Some(x) => x should be("scala-redis")
        case None => fail("should return scala-redis")
      }
    }
  }

}
