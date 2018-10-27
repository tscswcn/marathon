package mesosphere.marathon
package core.task.tracker.impl

import java.time.Clock
import java.util.concurrent.TimeoutException

import akka.{Done, NotUsed}
import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.util.Timeout
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.instance.update.{InstanceChange, InstanceUpdateEffect, InstanceUpdateOperation}
import mesosphere.marathon.core.instance.{Goal, Instance}
import mesosphere.marathon.core.task.tracker.impl.InstanceTrackerActor.UpdateContext
import mesosphere.marathon.core.task.tracker.{InstanceTracker, InstanceTrackerConfig}
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.metrics.deprecated.ServiceMetric
import mesosphere.marathon.state.{PathId, Timestamp}
import org.apache.mesos

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

/**
  * Provides a [[InstanceTracker]] interface to [[InstanceTrackerActor]].
  *
  * This is used for the "global" InstanceTracker trait and it is also
  * is used internally in this package to communicate with the InstanceTracker.
  */
private[tracker] class InstanceTrackerDelegate(
    metrics: Metrics,
    clock: Clock,
    config: InstanceTrackerConfig,
    instanceTrackerRef: ActorRef)(implicit mat: Materializer) extends InstanceTracker {

  override def instancesBySpecSync: InstanceTracker.InstancesBySpec = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(instancesBySpec(), instanceTrackerQueryTimeout.duration)
  }

  override def instancesBySpec()(implicit ec: ExecutionContext): Future[InstanceTracker.InstancesBySpec] =
    oldTasksByAppTimeMetric {
      newTasksByAppTimeMetric {
        (instanceTrackerRef ? InstanceTrackerActor.List).mapTo[InstanceTracker.InstancesBySpec].recover {
          case e: AskTimeoutException =>
            throw new TimeoutException(
              "timeout while calling list. If you know what you are doing, you can adjust the timeout " +
                s"with --${config.internalTaskTrackerRequestTimeout.name}."
            )
        }
      }
    }

  // TODO(jdef) support pods when counting launched instances
  override def countActiveSpecInstances(appId: PathId): Future[Int] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    instancesBySpec().map(_.specInstances(appId).count(instance => instance.isActive || (instance.isReserved && !instance.isReservedTerminal)))
  }

  override def hasSpecInstancesSync(appId: PathId): Boolean = instancesBySpecSync.hasSpecInstances(appId)
  override def hasSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Boolean] =
    instancesBySpec().map(_.hasSpecInstances(appId))

  override def specInstancesSync(appId: PathId): Seq[Instance] =
    instancesBySpecSync.specInstances(appId)
  override def specInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]] =
    instancesBySpec().map(_.specInstances(appId))

  override def instance(taskId: Instance.Id): Future[Option[Instance]] =
    (instanceTrackerRef ? InstanceTrackerActor.Get(taskId)).mapTo[Option[Instance]]

  private[this] val oldTasksByAppTimeMetric = metrics.deprecatedTimer(ServiceMetric, getClass, "tasksByApp")
  private[this] val newTasksByAppTimeMetric =
    metrics.timer("debug.instance-tracker.resolve-tasks-by-app-duration")

  implicit val instanceTrackerQueryTimeout: Timeout = config.internalTaskTrackerRequestTimeout().milliseconds

  // -----------
  val maxParallelism: Int = 16
  val updateQueueSize: Int = 1024

  import scala.concurrent.ExecutionContext.Implicits.global

  case class QueuedUpdate(update: UpdateContext, promise: Promise[InstanceUpdateEffect])

  val queue = Source
    .queue[QueuedUpdate](updateQueueSize, OverflowStrategy.dropNew)
    .groupBy(maxParallelism, queued => MurmurHash3.stringHash(queued.update.instanceId.idString) % maxParallelism)
    .mapAsync(1){
      case QueuedUpdate(update, promise) =>
        val effectF = (instanceTrackerRef ? update).mapTo[InstanceUpdateEffect].recover{
          case NonFatal(ex) =>
            throw new RuntimeException(s"Timed out waiting for response for update $update", ex)
        }
        promise.completeWith(effectF)
        effectF
    }
    .map(u => logger.info(s"Completed processing instance update: $u"))
    .mergeSubstreams
    .toMat(Sink.ignore)(Keep.left)
    .run()

  override def process(stateOp: InstanceUpdateOperation): Future[InstanceUpdateEffect] = {
    val deadline = clock.now + instanceTrackerQueryTimeout.duration
    val update = InstanceTrackerActor.UpdateContext(deadline, stateOp)

    val promise = Promise[InstanceUpdateEffect]
    queue.offer(QueuedUpdate(update, promise)).map {
      case QueueOfferResult.Enqueued => logger.debug(s"Queued instance update operation $update")
      case QueueOfferResult.Dropped => throw new RuntimeException(s"Dropped instance update: $update")
      case QueueOfferResult.Failure(ex) => throw new RuntimeException(s"Failed to process instance update $update because", ex)
      case QueueOfferResult.QueueClosed => throw new RuntimeException(s"Failed to process instance update $update because the queue is closed")
    }
    promise.future
  }

  override def launchEphemeral(instance: Instance): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.LaunchEphemeral(instance)).map(_ => Done)
  }

  override def schedule(instance: Instance): Future[Done] = {
    require(
      instance.isScheduled,
      s"Instance ${instance.instanceId} was not in scheduled state but ${instance.state.condition}")

    import scala.concurrent.ExecutionContext.Implicits.global
    process(InstanceUpdateOperation.Schedule(instance)).map(_ => Done)
  }

  override def revert(instance: Instance): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.Revert(instance)).map(_ => Done)
  }

  override def forceExpunge(instanceId: Instance.Id): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.ForceExpunge(instanceId)).map(_ => Done)
  }

  override def updateStatus(instance: Instance, mesosStatus: mesos.Protos.TaskStatus, updateTime: Timestamp): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.MesosUpdate(instance, mesosStatus, updateTime)).map(_ => Done)
  }

  override def reservationTimeout(instanceId: Instance.Id): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.ReservationTimeout(instanceId)).map(_ => Done)
  }

  override def setGoal(instanceId: Instance.Id, goal: Goal): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    process(InstanceUpdateOperation.GoalChange(instanceId, goal)).map(_ => Done)
  }

  override val instanceUpdates: Source[InstanceChange, NotUsed] = {
    Source.actorRef(Int.MaxValue, OverflowStrategy.fail)
      .watchTermination()(Keep.both)
      .mapMaterializedValue {
        case (ref, done) =>
          done.onComplete { _ =>
            instanceTrackerRef.tell(InstanceTrackerActor.Unsubscribe, ref)
          }(ExecutionContexts.callerThread)
          instanceTrackerRef.tell(InstanceTrackerActor.Subscribe, ref)
          NotUsed
      }
  }
}
