package github

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, date_format, when}
import core.AnalyzeGithubData.spark.implicits._

class Users(df: DataFrame) {

  private def usersAggFunc(valueToFilter: String, columnToRename: String): DataFrame = {
    df.filter($"type" === valueToFilter)
      .groupBy("repo.id")
      .count()
      .withColumnRenamed("count", columnToRename)
  }

  private val watchEventUser: DataFrame = usersAggFunc("WatchEvent", "projects_starred")
  private val issueCountUser: DataFrame = usersAggFunc("IssuesEvent", "issues_created")
  private val pullRequestEventUser: DataFrame = usersAggFunc("PullRequestEvent", "pr_count")

  def generateUserDataset: DataFrame = {
    df.withColumn("date", when($"type" === "CreateEvent", date_format($"created_at", "yyyy-MM-dd hh:mm:ss")).otherwise("N/A"))
      .join(broadcast(watchEventUser), df("actor.id") === watchEventUser("id"), "left")
      .join(broadcast(issueCountUser), df("actor.id") === issueCountUser("id"), "left")
      .join(broadcast(pullRequestEventUser), df("actor.id") === pullRequestEventUser("id"), "left")
      .select($"date", $"actor.id" as "user_id", $"actor.login" as "user_login", $"projects_starred", $"issues_created", $"pr_count")
      .distinct()
      .na
      .fill(0)
  }
}
