package mesosphere.marathon
package scheduling

import akka.Done
import mesosphere.marathon.core.instance.{Goal, Instance}
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.state.PathId
import org.apache.mesos.Protos

import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}

case class LegacyScheduler(
    offerProcessor: OfferProcessor,
    instanceTracker: InstanceTracker,
    statusUpdateProcessor: TaskStatusUpdateProcessor) extends Scheduler {

  //override def create(runSpec: RunSpec): Future[Instance.Id]

  override def getInstances(runSpecId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]] = instanceTracker.specInstances(runSpecId)

  override def getInstance(instanceId: Instance.Id)(implicit ec: ExecutionContext): Future[Option[Instance]] = instanceTracker.get(instanceId)

  @SuppressWarnings(Array("all")) // async/await
  override def run(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Running)))
    await(work)
    Done
  }

  @SuppressWarnings(Array("all")) // async/await
  override def decommission(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Decommissioned)))
    await(work)
    Done
  }

  @SuppressWarnings(Array("all")) // async/await
  override def stop(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Stopped)))
    await(work)
    Done
  }

  override def processOffer(offer: Protos.Offer): Future[Done] = offerProcessor.processOffer(offer)

  override def processMesosUpdate(status: Protos.TaskStatus)(implicit ec: ExecutionContext): Future[Done] = statusUpdateProcessor.publish(status).map(_ => Done)
}
