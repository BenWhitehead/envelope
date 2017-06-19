package io.github.benwhitehead.envelope

import com.twitter.bijection.Base64String._
import com.twitter.bijection.Bijection._
import com.twitter.bijection.GZippedBase64String._
import com.twitter.bijection.StringCodec._
import com.twitter.bijection.{Base64String, GZippedBase64String, Injection}
import io.circe._
import io.circe.jawn._
import io.circe.syntax._

import scala.util.{Failure, Success, Try}

final case class EnvelopeV1 private[envelope](payload: Either[Base64String, GZippedBase64String])

object EnvelopeV1 {
  implicit val encodeEnvelopeV1: io.circe.Encoder[EnvelopeV1] = Encoder.instance { (e: EnvelopeV1) =>
    Json.obj(
      "version" -> 1.asJson,
      "headers" -> (e.payload match {
        case Left(_) => Map[String, String]()
        case Right(_) => Map("Content-Encoding" -> "gzip")
      }).asJson,
      "payload" -> (e.payload match {
        case Left(b64) => b64.str
        case Right(gb64) => gb64.str
      }).asJson
    )
  }
  implicit val decodeEnvelopeV1: io.circe.Decoder[EnvelopeV1] = Decoder.instance { (c: HCursor) =>
    c.downField("version").as[Int].flatMap { v =>
      if (v != 1) {
        Left(DecodingFailure(s"Expected .version to be 1 but was $v", c.history))
      } else {
        c.downField("headers").as[Map[String, String]].flatMap { h =>
          c.downField("payload").as[String].flatMap { p =>
            (h.get("Content-Encoding") match {
              case Some("gzip") =>
                Injection.invert[GZippedBase64String, String](p).map { d => EnvelopeV1(Right(d)) }
              case _            =>
                Injection.invert[Base64String, String](p).map { d => EnvelopeV1(Left(d)) }
            }) match {
              case Success(env) => Right(env)
              case Failure(err) => Left(DecodingFailure(err.getMessage, c.history))
            }
          }
        }
      }
    }
  }

  private implicit val string2B64: Injection[String, Base64String] = utf8 andThen bytes2Base64
  private implicit val string2GzippedB64: Injection[String, GZippedBase64String] = utf8 andThen bytes2GZippedBase64

  def pack[T](t: T)(implicit tEnc: Encoder[T], p: Printer): Try[EnvelopeV1] = {
    val tJsonString = tEnc(t).pretty(p)

    (Injection[String, Base64String](tJsonString), Injection[String, GZippedBase64String](tJsonString)) match {
      case (jsonStringBase64, jsonStringBase64Gzip) =>
        if (jsonStringBase64Gzip.str.length < jsonStringBase64.str.length) {
          Success(EnvelopeV1(Right(jsonStringBase64Gzip)))
        } else {
          Success(EnvelopeV1(Left(jsonStringBase64)))
        }
    }
  }

  def unpack[T](e: EnvelopeV1)(implicit tDec: Decoder[T]): Try[T] = {
    val t = e.payload match {
      case Right(b64Gzip) =>
        Injection.invert[String, GZippedBase64String](b64Gzip)
      case Left(b64) =>
        Injection.invert[String, Base64String](b64)
    }
    t.flatMap(decode[T](_).toTry)
  }
}
