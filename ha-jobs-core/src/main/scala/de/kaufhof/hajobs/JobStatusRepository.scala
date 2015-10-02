package de.kaufhof.hajobs

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import de.kaufhof.hajobs.JobState._
import de.kaufhof.hajobs.utils.CassandraUtils._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory.getLogger
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * Repository to manage job status in the database.
 * Write and Read Methods can be called with Consistency Level LOCAL_QUORUM
 * to get consistent Job data from Cassandra. This is necessary to prevent
 * the JobSupervisor from setting Finished Jobs to Dead Jobs
 */
class JobStatusRepository(session: Session,
                          ttl: FiniteDuration = 14.days,
                          jobTypes: JobTypes) {

  private val logger = getLogger(getClass)

  private val MetaTable = "job_status_meta"
  private val DataTable = "job_status_data"

  private val JobTypeColumn = "job_type"
  private val JobIdColumn = "job_id"
  private val JobStateColumn = "job_state"
  private val JobResultColumn = "job_result"
  private val TimestampColumn = "job_status_ts"
  private val ContentColumn = "content"
  private val TriggerIdColumn = "trigger_id"
  private val JobMonitoringStateColumn = "job_monitoring_state"

  private def insertMetaQuery(jobStatus: JobStatus) = {
    val stmt = insertInto(MetaTable)
      .value(JobTypeColumn, jobStatus.jobType.name)
      .value(JobIdColumn, jobStatus.jobId)
      .value(TimestampColumn, jobStatus.jobStatusTs.toDate)
      .value(JobStateColumn, jobStatus.jobState.toString)
      .value(JobResultColumn, jobStatus.jobResult.toString)
      .value(TriggerIdColumn, jobStatus.triggerId)
      .value(JobMonitoringStateColumn, jobStatus.jobMonitoringState.fold(JobMonitoringState.Undefined){state => state}.toString)
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt
  }

  private def insertDataQuery(jobStatus: JobStatus) = {
    val stmt = insertInto(DataTable)
      .value(JobTypeColumn, jobStatus.jobType.name)
      .value(JobIdColumn, jobStatus.jobId)
      .value(JobStateColumn, jobStatus.jobState.toString)
      .value(JobResultColumn, jobStatus.jobResult.toString)
      .value(TimestampColumn, jobStatus.jobStatusTs.toDate)
      .value(ContentColumn, jobStatus.content.map(_.toString()).orNull)
      .value(TriggerIdColumn, jobStatus.triggerId)
      .using(QueryBuilder.ttl(ttl.toSeconds.toInt))
    stmt.setConsistencyLevel(LOCAL_QUORUM)
    stmt
  }

  def save(jobStatus: JobStatus)(implicit ec: ExecutionContext): Future[JobStatus] = {
    val batchStmt = batch(insertMetaQuery(jobStatus), insertDataQuery(jobStatus))
    session.executeAsync(batchStmt).map(_ => jobStatus)
  }

  def updateJobState(jobStatus: JobStatus, newState: JobState)(implicit ec: ExecutionContext): Future[JobStatus] = {
    save(jobStatus.copy(jobState = newState, jobResult = JobStatus.stateToResult(newState), jobStatusTs = DateTime.now()))
  }

  /**
   * Finds the latest job status entries for all jobs.
   * Every JobState is loaded with a single select statement
   * It is recommended to use partition keys if possible and avoid
   * IN statement in WHERE clauses, therefore we prefer to execute
   * more than one select statement
   */
  def getLatestMetadata(readwithQuorum: Boolean = false)(implicit ec: ExecutionContext): Future[List[JobStatus]] = {

    def getLatestMetadata(jobType: JobType): Future[Option[JobStatus]] = {
      val selectMetadata = select().all().from(MetaTable).where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      if (readwithQuorum) {
        // setConsistencyLevel returns "this", we do not need to reassign
        selectMetadata.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      }
      session.executeAsync(selectMetadata).map(res =>
        Option(res.one).flatMap(result => rowToStatus(result, isMeta = true))
      )
    }

    val results = jobTypes.all.toList.map(getLatestMetadata)
    Future.sequence(results).map(_.flatten)
  }

  /**
   * Finds the latest job status entries for one job from a specified timestamp (24h default) up to now
   *
   * @param jobType
   * @param timeStamp
   * @param readwithQuorum
   * @param ec
   * @return
   */
  def getMetadataSinceTs(jobType: JobType, timeStamp: Option[DateTime] = None, readwithQuorum: Boolean = false)
                        (implicit ec: ExecutionContext): Future[Option[List[JobStatus]]] = {
    import scala.collection.JavaConversions._

    //default time we check if no timestamp is send = 24 hours
    val ts: DateTime = timeStamp.getOrElse(DateTime.now().minusHours(24))

    val selectMetadata = select().all().from(MetaTable).where(QueryBuilder.eq(JobTypeColumn, jobType.name))
    if (readwithQuorum) {
      // setConsistencyLevel returns "this", we do not need to reassign
      selectMetadata.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectMetadata)
    resultFuture.map( res =>
      Option(res.all.toList.flatMap( row =>
        rowToStatus(row, isMeta = false).filter(_.jobStatusTs.isAfter(ts)))
      ))
  }

  /**
   * Returns the history of saved JobStatus for a single job (each save/update for a job
   * is a separate entry).
   */
  def getJobHistory(jobType: JobType, jobId: UUID, withQuorum: Boolean = false)
                   (implicit ec: ExecutionContext): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._

    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map( res =>
      res.all.toList.flatMap( row =>
        rowToStatus(row, isMeta = false)
      ))
  }

  def list(jobType: JobType, limit: Int = 20, withQuorum: Boolean = false)
          (implicit ec: ExecutionContext): Future[List[JobStatus]] = {
    import scala.collection.JavaConversions._
    val selectStmt = select().all()
      .from(MetaTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .limit(limit)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }

    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map ( res =>
      res.all().toList.flatMap( result =>
        rowToStatus(result, isMeta = true)
      ))
  }

  def get(jobType: JobType, jobId: UUID, withQuorum: Boolean = false)
         (implicit ec: ExecutionContext): Future[Option[JobStatus]] = {
    val selectStmt = select().all()
      .from(DataTable)
      .where(QueryBuilder.eq(JobTypeColumn, jobType.name))
      .and(QueryBuilder.eq(JobIdColumn, jobId))
      .limit(1)

    if (withQuorum) {
      selectStmt.setConsistencyLevel(LOCAL_QUORUM)
    }


    val resultFuture: ResultSetFuture = session.executeAsync(selectStmt)
    resultFuture.map( res =>
      Option(res.one).flatMap( result =>
        rowToStatus(result, isMeta = false)
      ))
  }

  private def rowToStatus(row: Row, isMeta: Boolean): Option[JobStatus] = {
    try {
      val jobTypeName: String = row.getString(JobTypeColumn)
      jobTypes(jobTypeName) match {
        case Some(jobType) =>
          Some(JobStatus(
            row.getUUID(TriggerIdColumn),
            jobType,
            row.getUUID(JobIdColumn),
            JobState.withName(row.getString(JobStateColumn)),
            JobResult.withName(row.getString(JobResultColumn)),
            new DateTime(row.getDate(TimestampColumn).getTime),
            if (!isMeta) {
              readContent(row)
            } else {
              None
            }
          ))
        case None => logger.error(s"Could not find Jobtype for name: $jobTypeName")
          None
      }
    } catch {
      case NonFatal(e) =>
        logger.error("error mapping a JobStatus datarow to JobStatus object", e)
        None
    }
  }

  private def readContent(row: Row): Option[JsValue] = {
    Option(row.getString(ContentColumn)).flatMap(content =>
      try {
        Some(Json.parse(content))
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Could not parse content for ${row.getString(JobTypeColumn)} job ${row.getUUID(JobIdColumn)}: $e (content: $content).")
          None
      }
    )
  }

  def clear()(implicit ec: ExecutionContext): Future[Unit] = {
    val metaResFuture = session.executeAsync(truncate(MetaTable))
    val dataResFuture = session.executeAsync(truncate(DataTable))
    for(
      meta <- metaResFuture;
      data <- dataResFuture
    ) yield ()
  }
}
