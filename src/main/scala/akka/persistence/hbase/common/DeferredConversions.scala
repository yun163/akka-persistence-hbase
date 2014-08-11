package akka.persistence.hbase.common

import akka.event.LoggingAdapter
import com.stumbleupon.async._
import scala.concurrent.{ ExecutionContext, Promise, Future }

trait DeferredConversions {

  implicit def typedFuture2unitFuture[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[Unit] =
    f map { _ => () }

  implicit def deferred2unitFuture[T <: AnyRef](deferred: Deferred[AnyRef])(implicit executionContext: ExecutionContext, logger: LoggingAdapter): Future[Unit] =
    deferred2future(deferred)

  implicit def deferred2future[T <: AnyRef](deferred: Deferred[T])(implicit logger: LoggingAdapter): Future[T] = {
    val p = Promise[T]()

    val onSuccess = new Callback[AnyRef, T] {
      def call(in: T) = p.success(in)
    }

    val onError = new Callback[Any, Exception] {
      def call(ex: Exception) = {
        if (logger != null) {
          logger.error("HBase client meet exception : " + ex.getStackTraceString)
        }
        p.failure(ex)
      }
    }

    deferred
      .addCallback(onSuccess)
      .addErrback(onError)

    p.future
  }

  implicit def fun2callback[T <: AnyRef, R <: AnyRef](fn: T => R): Callback[R, T] =
    new Callback[R, T] {
      def call(arg: T): R = fn(arg)
    }
}

object DeferredConversions extends DeferredConversions