package com.twitter.finagle.dispatch

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Flags, Record, SpanId, Trace, Tracer, TraceId}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Time, Local}
import java.security.cert.X509Certificate
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

import com.twitter.finagle.tracing._

@RunWith(classOf[JUnitRunner])
class SerialServerDispatcherTest extends FunSuite with MockitoSugar {
  trait Ctx {
    val trans = mock[Transport[String, String]]
    when(trans.peerCertificate).thenReturn(None)
    when(trans.onClose).thenReturn(Future.never)
    val readp = new Promise[String]
    when(trans.read()).thenReturn(readp)
    val writep = new Promise[Unit]
    when(trans.write(any[String])).thenReturn(writep)
  }

  trait Ictx {
    val onClose = new Promise[Throwable]
    val writep = new Promise[Unit]
    val trans = mock[Transport[String, String]]
    when(trans.onClose).thenReturn(onClose)
    when(trans.write(any[String])).thenReturn(writep)
    when(trans.peerCertificate).thenReturn(None)
    val service = mock[Service[String, String]]
    when(service.close(any[Time])).thenReturn(Future.Done)
    val replyp = new Promise[String] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    when(service("ok")).thenReturn(replyp)

    val readp = new Promise[String]
    when(trans.read()).thenReturn(readp)

    val disp = new SerialServerDispatcher(trans, service)
  }


  test("interrupt on hangup: while pending") (new Ictx {
    readp.setValue("ok")
    verify(service).apply("ok")
    assert(!replyp.interrupted.isDefined)
    onClose.setValue(new Exception)
    assert(replyp.interrupted.isDefined)
  })

  test("interrupt on hangup: while reading") (new Ictx {
    verify(trans).read()
    onClose.setValue(new Exception)
    assert(!replyp.interrupted.isDefined)
    verify(service, times(0)).apply(any[String])
    readp.setValue("ok")
    verify(service, times(0)).apply(any[String])
    // This falls through.
    verify(trans).close()
    verify(service).close(any[Time])
  })

  test("interrupt on hangup: while draining") (new Ictx {
    readp.setValue("ok")
    verify(service).apply("ok")
    replyp.setValue("yes")
    disp.close(Time.now)
    assert(!replyp.interrupted.isDefined)
    verify(trans).write("yes")
    onClose.setValue(new Exception)
    assert(!replyp.interrupted.isDefined)
  })

  trait Dctx {
    val onClose = new Promise[Throwable]
    val writep = new Promise[Unit]
    val trans = mock[Transport[String, String]]
    when(trans.onClose).thenReturn(onClose)
    when(trans.write(any[String])).thenReturn(writep)
    when(trans.peerCertificate).thenReturn(None)

    val service = mock[Service[String, String]]
    when(service.close(any[Time])).thenReturn(Future.Done)

    val readp = new Promise[String]
    when(trans.read()).thenReturn(readp)

    val disp = new SerialServerDispatcher(trans, service)
    verify(trans).read()
  }

  test("isClosing") ( new Ictx {
    assert(!disp.isClosing)
    disp.close(Time.now)
    assert(disp.isClosing)
  })

  test("drain: while reading") (new Dctx {
    disp.close(Time.now)
    verify(trans).close(any[Time])
    verify(service, times(0)).close(any[Time])

    readp.setException(new Exception("closed!"))
    onClose.setValue(new Exception("closed!"))
    verify(service).close(any[Time])
    verify(service, times(0)).apply(any[String])
    verify(trans, times(0)).write(any[String])
    verify(trans).read()
  })

  test("drain: while dispatching") (new Dctx {
    val servicep = new Promise[String]
    when(service(any[String])).thenReturn(servicep)
    readp.setValue("ok")
    verify(service).apply("ok")

    disp.close(Time.now)
    verify(service, times(0)).close(any[Time])
    verify(trans, times(0)).close()

    servicep.setValue("yes")
    verify(trans).write("yes")
    verify(service, times(0)).close(any[Time])
    verify(trans, times(0)).close()

    writep.setDone()
    verify(trans).close()
    onClose.setValue(new Exception("closed!"))
    verify(service).close(any[Time])
  })

  test("WireSend and WireRecv Annotations") (new Ctx {
    val tracer = mock[Tracer]
    Trace.letTracer(tracer) {
      val service = mock[Service[String, String]]
      when(service.close(any[Time])).thenReturn(Future.Done)
      val f = (s: String) => TraceId(None, None, SpanId(71L), None, Flags(Flags.Debug))
      val config = new ServerDispatcherConfig[String](f, true, true)
      val disp = new SerialServerDispatcher(trans, service, config)

      verify(trans).read()
      verify(trans, never()).write(any[String])
      verify(service, never()).apply(any[String])

      val servicep = new Promise[String]
      when(service(any[String])).thenReturn(servicep)

      readp.setValue("ok")
      verify(service).apply("ok")
      verify(trans, never()).write(any[String])

      servicep.setValue("ack")
      verify(trans).write("ack")

      verify(trans).read()

      when(trans.read()).thenReturn(new Promise[String]) // to short circuit
      writep.setDone()

      verify(trans, times(2)).read()
    }
    verify(tracer, times(2)).record(any[Record])
  })

  test("no tracing when disabled") (new Ctx {
    val tracer = mock[Tracer]
    Trace.letTracer(tracer) {
      val service = mock[Service[String, String]]
      when(service.close(any[Time])).thenReturn(Future.Done)
      val f = (s: String) => TraceId(None, None, SpanId(71L), None, Flags(Flags.Debug))
      val config = new ServerDispatcherConfig[String](f, false, true)
      val disp = new SerialServerDispatcher(trans, service, config)

      val servicep = new Promise[String]
      when(service(any[String])).thenReturn(servicep)

      readp.setValue("ok")
      verify(service).apply("ok")
      verify(trans, never()).write(any[String])

      when(trans.read()).thenReturn(new Promise[String]) // to short circuit
      writep.setDone()

      verify(trans, times(1)).read()
    }
    verify(tracer, times(0)).record(any[Record])
  })
}
