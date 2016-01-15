package com.twitter.finagle.dispatch

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, NoStacktrace, CancelledRequestException}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference

import com.twitter.finagle.tracing._

case class ServerDispatcherConfig[A](f: (A) => TraceId, traceEnabled: Boolean)

abstract class ServerDispatcher[Req,Rep,Out](config: ServerDispatcherConfig[Out]) extends Closable {
  private[this] val RecordWireSend: Unit => Unit = _ => Trace.record(Annotation.WireSend)

  /**
   * Dispatches a request. The first argument is the request. The second
   * argument `eos` (end-of-stream promise) must be fulfilled when the request
   * is complete.
   *
   * For non-streaming requests, `eos.setDone()` should be called immediately,
   * since the entire request is present. For streaming requests,
   * `eos.setDone()` must be called at the end of stream (in HTTP, this is on
   * receipt of last chunk). Refer to the implementation in
   * [[com.twitter.finagle.http.codec.HttpServerDispatcher]].
   */
  protected def dispatch(req: Out, eos: Promise[Unit]): Future[Rep]
  protected def handle(rep: Rep): Future[Unit]

  protected def wrappedDispatch(req: Out, eos: Promise[Unit]): Future[Rep] = 
    config.traceEnabled match {
      case true => {
        val traceId = config.f(req)
        Trace.letId(traceId) {
          Trace.record(Annotation.WireRecv)
          dispatch(req, eos)
        }
      }
      case false => dispatch(req, eos)
  }

  protected def wrappedHandle(rep: Rep): Future[Unit] = config.traceEnabled match {
    case true  => handle(rep).onSuccess { RecordWireSend }
    case false => handle(rep)
  }
}


object GenSerialServerDispatcher {
  private val Eof = Future.exception(new Exception("EOF") with NoStacktrace)
  // We don't use Future.never here, because object equality is important here
  private val Idle = new NoFuture
  private val Closed = new NoFuture

}

/**
 * A generic version of
 * [[com.twitter.finagle.dispatch.SerialServerDispatcher SerialServerDispatcher]],
 * allowing the implementor to furnish custom dispatchers & handlers.
 */
abstract class GenSerialServerDispatcher[Req, Rep, In, Out] (trans: Transport[In, Out],
  config: ServerDispatcherConfig[Out], beingTested: Boolean = false)
  extends ServerDispatcher[Req,Rep,Out](config) {

  import GenSerialServerDispatcher._

  private[this] val state = new AtomicReference[Future[_]](Idle)
  private[this] val cancelled = new CancelledRequestException

  private[this] def loop(): Future[Unit] = {
    state.set(Idle)
    trans.read() flatMap { req =>
      val p = new Promise[Rep]
      if (state.compareAndSet(Idle, p)) {
        val eos = new Promise[Unit]
        val save = Local.save()
        try trans.peerCertificate match {
          case None => p.become(wrappedDispatch(req, eos))
          case Some(cert) => Contexts.local.let(Transport.peerCertCtx, cert) {
            p.become(dispatch(req, eos))
          }
        } finally Local.restore(save)
        p map { res => (res, eos) }
      } else Eof
    } flatMap { case (rep, eos) =>
      Future.join(wrappedHandle(rep), eos).unit
    } respond {
      case Return(()) if state.get ne Closed =>
        loop()

      case _ =>
        trans.close()
    }
  }

  // Clear all locals to start the loop unless being tested; we want a clean slate.
  private[this] val looping = beingTested match {
    case false => Local.letClear { loop() }
    case true  => loop()
  }

  trans.onClose ensure {
    looping.raise(cancelled)
    state.getAndSet(Closed).raise(cancelled)
  }

  /** Exposed for testing */
  protected[dispatch] def isClosing: Boolean = state.get() eq Closed

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing TCP connection is
  // also possible.
  def close(deadline: Time) = {
    if (state.getAndSet(Closed) eq Idle)
      trans.close(deadline)
    trans.onClose.unit
  }
}

/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests.
 *
 * Transport errors are considered fatal; the service will be
 * released after any error.
 */
class SerialServerDispatcher[Req, Rep](
    trans: Transport[Rep, Req],
    service: Service[Req, Rep],
    config: ServerDispatcherConfig[Req] = 
      new ServerDispatcherConfig((r: Req) => { Trace.id }, false),
    beingTested: Boolean = false)
    extends GenSerialServerDispatcher[Req, Rep, Rep, Req](trans, config, beingTested) {

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(req: Req, eos: Promise[Unit]) = {
    println("Serial ServerDispatcher dispatch")
    service(req) ensure eos.setDone()
  }

  protected def handle(rep: Rep) = {
    println("Serial ServerDispatcher handle")
    trans.write(rep)
  }
}
