package github

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, date_format, when}
import core.AnalyzeGithubData.spark.implicits._

class Repositories(df: DataFrame) {

  private def repositoryAggFunc(valueToFilter: String, columnToRename: String): DataFrame = {
    df.filter($"type" === valueToFilter)
      .groupBy("repo.id")
      .count()
      .withColumnRenamed("count", columnToRename)
  }

  private val watchEventRepository: DataFrame = repositoryAggFunc("WatchEvent", "users_starred")
  private val forkEventRepository: DataFrame = repositoryAggFunc("ForkEvent", "users_forked")
  private val issueCountRepository: DataFrame = repositoryAggFunc("IssuesEvent", "issue_count")
  private val pullRequestEventRepository: DataFrame = repositoryAggFunc("PullRequestEvent", "pr_count")

  def generateRepositoryDataset: DataFrame = {
    df.withColumn("date", when($"type" === "CreateEvent", date_format($"created_at", "yyyy-MM-dd hh:mm:ss")).otherwise("N/A"))
      .join(broadcast(watchEventRepository), df("repo.id") === watchEventRepository("id"), "left")
      .join(broadcast(forkEventRepository), df("repo.id") === forkEventRepository("id"), "left")
      .join(broadcast(issueCountRepository), df("repo.id") === issueCountRepository("id"), "left")
      .join(broadcast(pullRequestEventRepository), df("repo.id") === pullRequestEventRepository("id"), "left")
      .select($"date", $"repo.id" as "project_id", $"repo.name" as "project_name", $"users_starred", $"users_forked", $"issue_count", $"pr_count")
      .distinct()
      .na
      .fill(0)
  }
}