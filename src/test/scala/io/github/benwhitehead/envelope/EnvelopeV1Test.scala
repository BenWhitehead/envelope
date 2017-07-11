package io.github.benwhitehead.envelope

import com.twitter.bijection.Bijection._
import com.twitter.bijection.{Base64String, GZippedBase64String, Injection}
import io.circe.{CursorOp, DecodingFailure, Encoder}
import io.circe.Printer.noSpaces
import io.circe.generic.semiauto._
import io.circe.jawn._

import scala.util.Success

final case class Hello(hello: String)

class EnvelopeV1Test extends org.scalatest.FreeSpec {
  private implicit val printer = noSpaces
  private implicit val encodeHello: io.circe.Encoder[Hello] = deriveEncoder[Hello]
  private implicit val decodeHello: io.circe.Decoder[Hello] = deriveDecoder[Hello]

  private implicit val string2B64 = Injection.connect[String, Array[Byte], Base64String]
  private implicit val string2GzippedB64 = Injection.connect[String, Array[Byte], GZippedBase64String]

  "EnvelopeV1" - {

    "should be able to pack an object that" - {
      "does not benefit from compression" in {
        val hello = Hello("world")
        val Success(packed) = EnvelopeV1.pack(hello)
        val b64json = Injection[String, Base64String](s"""{"hello":"${hello.hello }"}""").str
        val expected = EnvelopeV1(Left(Base64String(b64json)))
        assert(packed === expected)
      }

      "does benefit from compression" in {
        val hello = Hello("worlddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
        val Success(packed) = EnvelopeV1.pack(hello)
        val b64json = Injection[String, GZippedBase64String](s"""{"hello":"${hello.hello }"}""").str
        val expected = EnvelopeV1(Right(GZippedBase64String(b64json)))
        assert(packed === expected)
      }
    }

    "support json when not gzipped" - {
      val payload = "eyJoZWxsbyI6ICJ3b3JsZCJ9Cg=="
      val json = s"""{"version":1,"headers":{},"payload":"$payload"}"""
      val envelope = EnvelopeV1(Left(Base64String(payload)))

      "decode" in {
        val Right(actual) = decode[EnvelopeV1](json)
        assert(actual === envelope)
      }

      "encode" in {
        val enc: Encoder[EnvelopeV1] = Encoder[EnvelopeV1]
        val actual = enc(envelope).pretty(printer)
        assert(actual === json)
      }
    }

    "support json when gzipped" - {
      val payload = "H4sIAAAAAAAAAKtWykjNyclXslIqzy/KSaEdUKoFALdH/ThrAAAA"
      val json = s"""{"version":1,"headers":{"Content-Encoding":"gzip"},"payload":"$payload"}"""
      val envelope = EnvelopeV1(Right(GZippedBase64String(payload)))

      "decode" in {
        val Right(actual) = decode[EnvelopeV1](json)
        assert(actual === envelope)
      }

      "encode" in {
        val enc: Encoder[EnvelopeV1] = Encoder[EnvelopeV1]
        val actual = enc(envelope).pretty(printer)
        assert(actual === json)
      }
    }

    "should be able to unpack and object that is" - {
      "not compressed" in {
        val hello = Hello("world")
        val payload = "eyJoZWxsbyI6ICJ3b3JsZCJ9Cg=="
        val json = s"""{"version":1,"headers":{},"payload":"$payload"}"""
        val Success(actual) = decode[EnvelopeV1](json).toTry.flatMap(EnvelopeV1.unpack[Hello])
        assert(actual === hello)
      }

      "compressed" in {
        val hello = Hello("worlddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
        val payload = "H4sIAAAAAAAAAKtWykjNyclXslIqzy/KSaEdUKoFALdH/ThrAAAA"
        val json = s"""{"version":1,"headers":{"Content-Encoding":"gzip"},"payload":"$payload"}"""
        val Success(actual) = decode[EnvelopeV1](json).toTry.flatMap(EnvelopeV1.unpack[Hello])
        assert(actual === hello)
      }
    }

    "json decoding should fail when" - {

      "version is not 1" in {
        val json = s"""{"version":2,"headers":{},"payload":"c29tZXRoaW5nCg=="}"""
        val Left(DecodingFailure((msg, cursor))) = decode[EnvelopeV1](json)
        assert(CursorOp.opsToPath(cursor) === ".version")
        assert(msg === "Expected .version to be 1 but was 2")
      }
      
      "payload is not base64" in {
        val json = s"""{"version":1,"headers":{},"payload":"!"}"""
        val Left(DecodingFailure((msg, cursor))) = decode[EnvelopeV1](json)
        assert(CursorOp.opsToPath(cursor) === ".payload")
        assert(msg === "Failed to invert: !")
      }
      
    }
  }
}
