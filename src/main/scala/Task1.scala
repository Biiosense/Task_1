import org.apache.spark.sql.functions.{avg, count, max, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Task1 {

  def main(args: Array[String]): Unit = {


    /* Configure Spark */
    val spark = SparkSession.builder()
      .appName("Task1")
      .master("local")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")

    /* Paths */
    val path_in = "data/bgdata_small/"
    val path_out = "data/tmp/"


    /* SQL */
    var SQL_times = process_SQL(spark, path_in, path_out)


    /* Dataframe */
    var DF_times = process_DF(spark, path_in, path_out)


    System.out.println("| task |  spark dataframe api  |  spark sql api  |  best  |  ")
    for ( i <- 0 until (SQL_times.length - 1))
    {
      val task_index = i+1
      if (task_index == 7)
        System.out.print("| 7.A |")
      else if (task_index == 8)
        System.out.print("| 7.B |")
      else if (task_index == 9)
        System.out.print("|  8  |")
      else if (task_index == 10)
        System.out.print("|  9  |")
      else
        System.out.print("|  " + task_index + "  |")

      val DF_time = DF_times(i)
      if (DF_time != 0.0)
        System.out.print( "  " + DF_time + " sec  |")
      else
        System.out.print( "   -------  |")

      val SQL_time = SQL_times(i)
      if (SQL_time != 0.0)
        System.out.print( "  " + SQL_time + " sec  |")
      else
        System.out.print( "   -------  |")


      if (SQL_time != 0.0 && (DF_time == 0.0 || SQL_time < DF_time) )
        System.out.println( "  sql  |")
      else if (DF_time != 0.0 && (SQL_time == 0.0 || DF_time < SQL_time) )
        System.out.println( "  dataframe  |")
      else
        System.out.println( "  ----  |")
    }

  }

  def process_SQL(spark: SparkSession, path_in: String, path_out: String): Array[Double] =
  {

    /* Read tales into views */
    spark.read.parquet(path_in + "commonUserProfiles.parquet").limit(100000).createOrReplaceTempView("commonUserProfiles")
    spark.read.parquet(path_in + "followerProfiles.parquet").limit(100000).createOrReplaceTempView("followerProfiles")
    spark.read.parquet(path_in + "followers.parquet").limit(10000000).createOrReplaceTempView("followers")
    spark.read.parquet(path_in + "friends.parquet").limit(10000000).createOrReplaceTempView("friends")
    spark.read.parquet(path_in + "friendsProfiles.parquet").limit(100000).createOrReplaceTempView("friendsProfiles")
    spark.read.parquet(path_in + "groupsProfiles.parquet").limit(100000).createOrReplaceTempView("groupsProfiles")
    spark.read.parquet(path_in + "likes.parquet").limit(100000).createOrReplaceTempView("likes")
    spark.read.parquet(path_in + "traces.parquet").limit(10000).createOrReplaceTempView("traces")
    spark.read.parquet(path_in + "userGroupsSubs.parquet").limit(1000000).createOrReplaceTempView("userGroupsSubs")
    spark.read.parquet(path_in + "userWallComments.parquet").limit(100000).createOrReplaceTempView("userWallComments")
    spark.read.parquet(path_in + "userWallLikes.parquet").limit(100000).createOrReplaceTempView("userWallLikes")
    spark.read.parquet(path_in + "userWallPhotos.parquet").limit(1000000).createOrReplaceTempView("userWallPhotos")
    spark.read.parquet(path_in + "userWallPhotosLikes.parquet").limit(100000).createOrReplaceTempView("userWallPhotosLikes")
    spark.read.parquet(path_in + "userWallPosts.parquet").limit(100000).createOrReplaceTempView("userWallPosts")
    spark.read.parquet(path_in + "userWallProfiles.parquet").limit(100000).createOrReplaceTempView("userWallProfiles")


    val SQL_times = new Array[Double](10)


    val Q1SQL_time_start = System.currentTimeMillis()
    val Q1SQL_answer = Q1SQL(spark)
    Q1SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q1SQL")
    val Q1SQL_time_end = System.currentTimeMillis()
    SQL_times(0) = (Q1SQL_time_end - Q1SQL_time_start)/1000
    System.out.println("SQL - Q1 : " + (Q1SQL_time_end - Q1SQL_time_start)/1000 + "s.")

    /*
    val Q2SQL_time_start = System.currentTimeMillis()
    val Q2SQL_answer = Q2SQL(spark)
    Q2SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q2SQL")
    val Q2SQL_time_end = System.currentTimeMillis()
    SQL_times(1) = (Q2SQL_time_end - Q2SQL_time_start)/1000
    System.out.println("SQL - Q2 : " + (Q2SQL_time_end - Q2SQL_time_start)/1000 + "s.")
    */

    val Q3SQL_time_start = System.currentTimeMillis()
    val Q3SQL_answer = Q3SQL(spark)
    Q3SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q3SQL")
    val Q3SQL_time_end = System.currentTimeMillis()
    SQL_times(2) = (Q3SQL_time_end - Q3SQL_time_start)/1000
    System.out.println("SQL - Q3 : " + (Q3SQL_time_end - Q3SQL_time_start)/1000 + "s.")

    val Q4SQL_time_start = System.currentTimeMillis()
    val Q4SQL_answer = Q4SQL(spark)
    Q4SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q4SQL")
    val Q4SQL_time_end = System.currentTimeMillis()
    SQL_times(3) = (Q4SQL_time_end - Q4SQL_time_start)/1000
    System.out.println("SQL - Q4 : " + (Q4SQL_time_end - Q4SQL_time_start)/1000 + "s.")

    val Q5SQL_time_start = System.currentTimeMillis()
    val Q5SQL_answer = Q5SQL(spark)
    Q5SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q5SQL")
    val Q5SQL_time_end = System.currentTimeMillis()
    SQL_times(4) = (Q5SQL_time_end - Q5SQL_time_start)/1000
    System.out.println("SQL - Q5 : " + (Q5SQL_time_end - Q5SQL_time_start)/1000 + "s.")

    val Q6SQL_time_start = System.currentTimeMillis()
    val Q6SQL_answer = Q6SQL(spark)
    Q6SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q6SQL")
    val Q6SQL_time_end = System.currentTimeMillis()
    SQL_times(5) = (Q6SQL_time_end - Q6SQL_time_start)/1000
    System.out.println("SQL - Q6 : " + (Q6SQL_time_end - Q6SQL_time_start)/1000 + "s.")

    /*
    val Q7ASQL_time_start = System.currentTimeMillis()
    val Q7ASQL_answer = Q7ASQL(spark)
    Q7ASQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q7ASQL")
    val Q7ASQL_time_end = System.currentTimeMillis()
    SQL_times(6) = (Q7ASQL_time_end - Q7ASQL_time_start)/1000
    System.out.println("SQL - Q7.A : " + (Q7ASQL_time_end - Q7ASQL_time_start)/1000 + "s.")

    val Q7BSQL_time_start = System.currentTimeMillis()
    val Q7BSQL_answer = Q7BSQL(spark)
    Q7BSQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q7BSQL")
    val Q7BSQL_time_end = System.currentTimeMillis()
    SQL_times(7) = (Q7BSQL_time_end - Q7BSQL_time_start)/1000
    System.out.println("SQL - Q7.B : " + (Q7BSQL_time_end - Q7BSQL_time_start)/1000 + "s.")
    */

    /* Both questions 7 generated the following error while writing the DataFrame as a parquet file:
   "java.lang.NoSuchMethodError: sun.nio.ch.DirectBuffer.cleaner()Lsun/misc/Cleaner;"
    This error is caused by the too great amount of data written.
    */


    val Q9SQL_time_start = System.currentTimeMillis()
    val Q9SQL_answer = Q9SQL(spark, path_out)
    Q9SQL_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q9SQL")
    val Q9SQL_time_end = System.currentTimeMillis()
    SQL_times(9) = (Q9SQL_time_end - Q9SQL_time_start)/1000
    System.out.println("SQL - Q9 : " + (Q9SQL_time_end - Q9SQL_time_start)/1000 + "s.")


    SQL_times
  }



  def process_DF(spark: SparkSession, path_in: String, path_out: String): Array[Double] =
  {

    /* Read tables into variables */
    //val commonUserProfiles = spark.read.parquet(path_in + "commonUserProfiles.parquet").limit(100000).as("commonUserProfiles")
    val followerProfiles = spark.read.parquet(path_in + "followerProfiles.parquet").limit(100000).as("followerProfiles")
    val followers = spark.read.parquet(path_in + "followers.parquet").limit(10000000).as("followers")
    val friends = spark.read.parquet(path_in + "friends.parquet").limit(10000000).as("friends")
    val friendsProfiles = spark.read.parquet(path_in + "friendsProfiles.parquet").limit(100000).as("friendsProfiles")
    val groupsProfiles = spark.read.parquet(path_in + "groupsProfiles.parquet").limit(100000).as("groupsProfiles")
    //val likes = spark.read.parquet(path_in + "likes.parquet").limit(100000).as("likes")
    //val traces = spark.read.parquet(path_in + "traces.parquet").limit(10000).as("traces")
    val userGroupsSubs = spark.read.parquet(path_in + "userGroupsSubs.parquet").limit(1000000).as("userGroupsSubs")
    val userWallComments = spark.read.parquet(path_in + "userWallComments.parquet").limit(100000).as("userWallComments")
    val userWallLikes = spark.read.parquet(path_in + "userWallLikes.parquet").limit(100000).as("userWallLikes")
    val userWallPhotos = spark.read.parquet(path_in + "userWallPhotos.parquet").limit(1000000).as("userWallPhotos")
    //val userWallPhotosLikes = spark.read.parquet(path_in + "userWallPhotosLikes.parquet").limit(100000).as("userWallPhotosLikes")
    val userWallPosts = spark.read.parquet(path_in + "userWallPosts.parquet").limit(100000).as("userWallPosts")
    //val userWallProfiles = spark.read.parquet(path_in + "userWallProfiles.parquet").limit(100000).as("userWallProfiles")


    var DF_times = new Array[Double](10)

    val Q1DF_time_start = System.currentTimeMillis()
    val Q1DF_answer = Q1DF(spark, userWallPosts, userWallLikes, userWallComments)
    Q1DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q1DF")
    val Q1DF_time_end = System.currentTimeMillis()
    DF_times(0) = (Q1DF_time_end - Q1DF_time_start)/1000
    System.out.println("DataFrame - Q1 : " + (Q1DF_time_end - Q1DF_time_start)/1000 + "s.")


    val Q2DF_time_start = System.currentTimeMillis()
    val Q2DF_answer = Q2DF(spark, friends, followers, userGroupsSubs, userWallPhotos)
    Q2DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q2DF")
    val Q2DF_time_end = System.currentTimeMillis()
    DF_times(1) = (Q2DF_time_end - Q2DF_time_start)/1000
    System.out.println("DataFrame - Q2 : " + (Q2DF_time_end - Q2DF_time_start)/1000 + "s.")


    val Q3DF_time_start = System.currentTimeMillis()
    val Q3DF_answer = Q3DF(spark, userWallPosts, userWallComments)
    Q3DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q3DF")
    val Q3DF_time_end = System.currentTimeMillis()
    DF_times(2) = (Q3DF_time_end - Q3DF_time_start)/1000
    System.out.println("DataFrame - Q3 : " + (Q3DF_time_end - Q3DF_time_start)/1000 + "s.")


    val Q4DF_time_start = System.currentTimeMillis()
    val Q4DF_answer = Q4DF(spark, userWallPosts, userWallLikes)
    Q4DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q4DF")
    val Q4DF_time_end = System.currentTimeMillis()
    DF_times(3) = (Q4DF_time_end - Q4DF_time_start)/1000
    System.out.println("DataFrame - Q4 : " + (Q4DF_time_end - Q4DF_time_start)/1000 + "s.")


    val Q5DF_time_start = System.currentTimeMillis()
    val Q5DF_answer = Q5DF(spark, userGroupsSubs, groupsProfiles)
    Q5DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q5DF")
    val Q5DF_time_end = System.currentTimeMillis()
    DF_times(4) = (Q5DF_time_end - Q5DF_time_start)/1000
    System.out.println("DataFrame - Q5 : " + (Q5DF_time_end - Q5DF_time_start)/1000 + "s.")


    val Q6DF_time_start = System.currentTimeMillis()
    val Q6DF_answer = Q6DF(spark, friends, friendsProfiles, followers, followerProfiles)
    Q6DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q6DF")
    val Q6DF_time_end = System.currentTimeMillis()
    DF_times(5) = (Q6DF_time_end - Q6DF_time_start)/1000
    System.out.println("DataFrame - Q6 : " + (Q6DF_time_end - Q6DF_time_start)/1000 + "s.")


    /*
    val Q7ADF_time_start = System.currentTimeMillis()
    val Q7ADF_answer = Q7ADF(spark, userWallPosts, userWallComments, userWallLikes, friends)
    Q7ADF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q7ADF")
    val Q7ADF_time_end = System.currentTimeMillis()
    DF_times(6) = (Q7ADF_time_end - Q7ADF_time_start)/1000
    System.out.println("DataFrame - Q7.A : " + (Q7ADF_time_end - Q7ADF_time_start)/1000 + "s.")


    val Q7BDF_time_start = System.currentTimeMillis()
    val Q7BDF_answer = Q7BDF(spark, userWallPosts, userWallComments, userWallLikes, friends)
    Q7BDF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q7BDF")
    val Q7BDF_time_end = System.currentTimeMillis()
    DF_times(7) = (Q7BDF_time_end - Q7BDF_time_start)/1000
    System.out.println("DataFrame - Q7.B : " + (Q7BDF_time_end - Q7BDF_time_start)/1000 + "s.")
    */

    /* Botj questions 7 generate the following error while writing the DataFrame as a parquet file:
    "java.lang.NoSuchMethodError: sun.nio.ch.DirectBuffer.cleaner()Lsun/misc/Cleaner;"
    This error is caused by the too great amount of data written.
     */


    val Q9DF_time_start = System.currentTimeMillis()
    val Q9DF_answer = Q9DF(spark, path_out)
    Q9DF_answer.write.mode(SaveMode.Overwrite).parquet(path_out + "Q9DF")
    val Q9DF_time_end = System.currentTimeMillis()
    DF_times(9) = (Q9DF_time_end - Q9DF_time_start)/1000
    System.out.println("DataFrame - Q9 : " + (Q9DF_time_end - Q9DF_time_start)/1000 + "s.")

    DF_times
  }



  /* 1. count of posts, posts_comments and like made by user */
  def Q1SQL(spark: SparkSession): DataFrame =
  {
    val req1 = "SELECT post_view.user_id as user_id, post_view.count_posts, like_view.count_likes, comment_view.count_comments " +
      "FROM " +
          "(SELECT owner_id AS user_id, count(id) AS count_posts FROM userWallPosts GROUP BY owner_id) AS post_view, " +
          "(SELECT likerId AS user_id, count(itemId) AS count_likes FROM userWallLikes GROUP BY likerId) AS like_view, " +
          "(SELECT from_id AS user_id, count(id) AS count_comments FROM userWallComments GROUP BY from_id) AS comment_view " +
      "WHERE post_view.user_id = like_view.user_id AND post_view.user_id = comment_view.user_id"
    spark.sql(req1)
  }

  /* 2. count of friends, followers, users groups, videos, audios, photos, gifts */
  def Q2SQL(spark: SparkSession): DataFrame =
  {
    val req2 = "SELECT friends_view.user_id, friends_view.count_friends, " +
      "followers_view.count_followers, groups_view.count_groups, photos_view.count_photos " +
      "FROM  " +
          "(SELECT profile as user_id, count(follower) AS count_friends FROM friends GROUP BY profile) AS friends_view, " +
          "(SELECT profile as user_id, count(follower) AS count_followers FROM followers GROUP BY profile) AS followers_view, " +
          "(SELECT user as user_id, count(group) AS count_groups FROM userGroupsSubs GROUP BY user) AS groups_view, " +
          "(SELECT owner_id as user_id, count(id) AS count_photos FROM userWallPhotos GROUP BY owner_id) AS photos_view " +
      "WHERE friends_view.user_id == followers_view.user_id AND followers_view.user_id == groups_view.user_id " +
      "AND groups_view.user_id == photos_view.user_id "

    spark.sql(req2)
  }

  /* 3. count of "incoming" (made by other users) comments, max and mean "incoming" comments per post */
  def Q3SQL(spark: SparkSession): DataFrame =
  {
    val req3 = "SELECT userWallPosts.owner_id as user_id, sum(counts.post_count) as total_incoming_comments, " +
      "max(counts.post_count) as max_incoming_comments_per_post, avg(counts.post_count) as mean_incoming_comments_per_post " +
      "FROM userWallPosts, " +
          "(SELECT userWallPosts.id AS post_id, count(userWallComments.from_id) as post_count  " +
          "FROM userWallPosts " +
          "LEFT JOIN userWallComments ON userWallPosts.id == userWallComments.post_id " +
          "WHERE userWallPosts.owner_id != userWallComments.from_id " +
          "GROUP BY userWallPosts.id) AS counts " +
      "WHERE userWallPosts.id == counts.post_id " +
      "GROUP BY userWallPosts.owner_id " +
      "ORDER BY userWallPosts.owner_id ASC"

    spark.sql(req3)
  }

  /* 4. count of "incoming" likes, max and mean "incoming" likes per post */
  def Q4SQL(spark: SparkSession): DataFrame =
  {
    val req4 = "SELECT userWallPosts.owner_id as user_id, sum(counts.post_count) as total_incoming_likes, " +
      "max(counts.post_count) as max_incoming_likes_per_post, avg(counts.post_count) as mean_incoming_likes_per_post " +
      "FROM userWallPosts, " +
      "(SELECT userWallPosts.id AS post_id, count(userWallLikes.likerId) as post_count  " +
          "FROM userWallPosts " +
          "LEFT JOIN userWallLikes ON userWallPosts.id == userWallLikes.itemId " +
          "WHERE userWallPosts.owner_id != userWallLikes.likerId " +
          "GROUP BY userWallPosts.id) AS counts " +
      "WHERE userWallPosts.id == counts.post_id " +
      "GROUP BY userWallPosts.owner_id " +
      "ORDER BY userWallPosts.owner_id ASC"

    spark.sql(req4)
  }

  /* 5. count of open/closed groups a user participates in */
  def Q5SQL(spark: SparkSession): DataFrame =
  {
    val req5 = "SELECT userGroupsSubs.user as user_id, count(opened_group) as count_groups_opened, count(closed_group) as count_groups_closed " +
      "FROM userGroupsSubs " +
      "LEFT JOIN  " +
      "(SELECT groupsProfiles.key as group_id, 1 as opened_group " +
          "FROM groupsProfiles " +
          "WHERE groupsProfiles.is_closed == 0) AS opened_groups " +
      "ON userGroupsSubs.group == opened_groups.group_id " +
      "LEFT JOIN " +
      "(SELECT groupsProfiles.key as group_id, 1 as closed_group " +
          "FROM groupsProfiles " +
          "WHERE groupsProfiles.is_closed == 1) AS closed_groups " +
      "ON userGroupsSubs.group == closed_groups.group_id " +
      "GROUP BY userGroupsSubs.user ";

    spark.sql(req5)
  }

  /* 6. count of deleted users in friends and followers */
  def Q6SQL(spark: SparkSession): DataFrame =
  {
    val req6 = "SELECT deleted_friends_view.user_id, deleted_friends_view.count_deleted_friends, deleted_followers_view.count_deleted_followers " +
      "FROM " +
      "(SELECT friends.profile as user_id, count(friends.follower) as count_deleted_friends " +
          "FROM friends, friendsProfiles " +
          "WHERE friends.follower == friendsProfiles.id AND friendsProfiles.deactivated == 'deleted' " +
          "GROUP BY friends.profile) AS deleted_friends_view, " +
      "(SELECT followers.profile as user_id, count(followers.follower) as count_deleted_followers " +
          "FROM followers, followerProfiles  " +
          "WHERE followers.follower == followerProfiles.id AND followerProfiles.deactivated == 'deleted' " +
          "GROUP BY followers.profile) AS deleted_followers_view " +
      "WHERE deleted_friends_view.user_id == deleted_followers_view.user_id ";

    spark.sql(req6)
  }


  /* 7. Aggregate (count, max, mean) characterics for comments and likes (separtely) made by a) friends and b) followers per user */
  /* 7.a) Friends  */
  def Q7ASQL(spark: SparkSession): DataFrame =
  {
    val req7_a = "SELECT comments_users_view.user_id, " +
      "comments_users_view.total_comments_from_friends, comments_users_view.max_comments_from_friends_per_post, comments_users_view.mean_comments_from_friends_per_post, " +
      "likes_users_view.total_likes_from_friends, likes_users_view.max_likes_from_friends_per_post, likes_users_view.mean_likes_from_friends_per_post " +
      "FROM " +
      "(SELECT userWallPosts.owner_id as user_id, sum(comments_posts_view.comments_per_post) as total_comments_from_friends, " +
          "max(comments_posts_view.comments_per_post) as max_comments_from_friends_per_post, avg(comments_posts_view.comments_per_post) as mean_comments_from_friends_per_post " +
          "FROM userWallPosts, " +
          "(SELECT userWallPosts.id AS post_id, count(userWallComments.from_id) as comments_per_post  " +
              "FROM userWallPosts " +
              "LEFT JOIN userWallComments ON userWallPosts.id == userWallComments.post_id " +
              "WHERE userWallComments.from_id IN (SELECT friends.follower FROM friends WHERE friends.profile == userWallComments.post_owner_id) " +
              "GROUP BY userWallPosts.id) AS comments_posts_view " +
          "WHERE userWallPosts.id == comments_posts_view.post_id " +
          "GROUP BY userWallPosts.owner_id) AS comments_users_view, " +
      "(SELECT userWallPosts.owner_id as user_id, sum(likes_posts_view.likes_per_post) as total_likes_from_friends, " +
          "max(likes_posts_view.likes_per_post) as max_likes_from_friends_per_post, avg(likes_posts_view.likes_per_post) as mean_likes_from_friends_per_post " +
          "FROM userWallPosts, " +
          "(SELECT userWallPosts.id AS post_id, count(userWallLikes.likerId) as likes_per_post  " +
              "FROM userWallPosts " +
              "LEFT JOIN userWallLikes ON userWallPosts.id == userWallLikes.itemId " +
              "WHERE userWallLikes.likerId IN (SELECT friends.follower FROM friends WHERE friends.profile == userWallLikes.ownerId) " +
              "GROUP BY userWallPosts.id) AS likes_posts_view " +
          "WHERE userWallPosts.id == likes_posts_view.post_id " +
          "GROUP BY userWallPosts.owner_id) AS likes_users_view " +
      "WHERE comments_users_view.user_id == likes_users_view.user_id"

    spark.sql(req7_a)
  }

  /* 7. Aggregate (count, max, mean) characterics for comments and likes (separtely) made by a) friends and b) followers per user */
  /* 7.b) Followers */
  def Q7BSQL(spark: SparkSession): DataFrame =
  {
    val req7_b = "SELECT comments_users_view.user_id, " +
      "comments_users_view.total_comments_from_followers, comments_users_view.max_comments_from_followers_per_post, comments_users_view.mean_comments_from_followers_per_post, " +
      "likes_users_view.total_likes_from_followers, likes_users_view.max_likes_from_followers_per_post, likes_users_view.mean_likes_from_followers_per_post " +
      "FROM " +
      "(SELECT userWallPosts.owner_id as user_id, sum(comments_posts_view.comments_per_post) as total_comments_from_followers, " +
          "max(comments_posts_view.comments_per_post) as max_comments_from_followers_per_post, avg(comments_posts_view.comments_per_post) as mean_comments_from_followers_per_post " +
          "FROM userWallPosts, " +
          "(SELECT userWallPosts.id AS post_id, count(userWallComments.from_id) as comments_per_post  " +
              "FROM userWallPosts " +
              "LEFT JOIN userWallComments ON userWallPosts.id == userWallComments.post_id " +
              "WHERE userWallComments.from_id IN (SELECT followers.follower FROM followers WHERE followers.profile == userWallComments.post_owner_id) " +
              "GROUP BY userWallPosts.id) AS comments_posts_view " +
          "WHERE userWallPosts.id == comments_posts_view.post_id " +
          "GROUP BY userWallPosts.owner_id) AS comments_users_view, " +
      "(SELECT userWallPosts.owner_id as user_id, sum(likes_posts_view.likes_per_post) as total_likes_from_followers, " +
          "max(likes_posts_view.likes_per_post) as max_likes_from_followers_per_post, avg(likes_posts_view.likes_per_post) as mean_likes_from_followers_per_post " +
          "FROM userWallPosts, " +
          "(SELECT userWallPosts.id AS post_id, count(userWallLikes.likerId) as likes_per_post  " +
              "FROM userWallPosts " +
              "LEFT JOIN userWallLikes ON userWallPosts.id == userWallLikes.itemId " +
              "WHERE userWallLikes.likerId IN (SELECT followers.follower FROM followers WHERE followers.profile == userWallLikes.ownerId) " +
              "GROUP BY userWallPosts.id) AS likes_posts_view " +
          "WHERE userWallPosts.id == likes_posts_view.post_id " +
          "GROUP BY userWallPosts.owner_id) AS likes_users_view " +
      "WHERE comments_users_view.user_id == likes_users_view.user_id"

    spark.sql(req7_b)
  }


  /* 8. Find emoji in a) user's posts b) user's comments (separately, count of: all, positive, negative, others) */
  /* 8.a) User's posts */
  /* 8.a) User's comments */


  /* 9. Join all users data from each tasks to the one table with different columns like
   *  posts_count, posts_comments_count, likes_count, friends_count and etc. */
  def Q9SQL(spark: SparkSession, path_out: String): DataFrame =
  {

    spark.read.parquet(path_out + "Q1SQL").createOrReplaceTempView("Q1SQL")
    spark.read.parquet(path_out + "Q2SQL").createOrReplaceTempView("Q2SQL")
    spark.read.parquet(path_out + "Q3SQL").createOrReplaceTempView("Q3SQL")
    spark.read.parquet(path_out + "Q4SQL").createOrReplaceTempView("Q4SQL")
    spark.read.parquet(path_out + "Q5SQL").createOrReplaceTempView("Q5SQL")
    spark.read.parquet(path_out + "Q6SQL").createOrReplaceTempView("Q6SQL")

    val req9 = "SELECT Q1.user_id, count_friends, count_followers, count_groups, count_photos, " +
      "total_incoming_comments, max_incoming_comments_per_post, mean_incoming_comments_per_post, " +
      "total_incoming_likes, max_incoming_likes_per_post, mean_incoming_likes_per_post, " +
      "count_groups_opened, count_groups_closed, " +
      "count_deleted_friends, count_deleted_followers " +
      "FROM " +
      "Q1SQL AS Q1 " +
      "LEFT JOIN Q2SQL AS Q2 ON Q1.user_id == Q2.user_id " +
      "LEFT JOIN Q3SQL AS Q3 ON Q1.user_id == Q3.user_id " +
      "LEFT JOIN Q4SQL AS Q4 ON Q1.user_id == Q4.user_id " +
      "LEFT JOIN Q5SQL AS Q5 ON Q1.user_id == Q5.user_id " +
      "LEFT JOIN Q6SQL AS Q6 ON Q1.user_id == Q6.user_id "

    spark.sql(req9)
  }


  /* 1. count of posts, posts_comments and like made by user */
  def Q1DF(spark: SparkSession, userWallPosts: DataFrame, userWallLikes: DataFrame, userWallComments: DataFrame): DataFrame =
  {
    import spark.implicits._

    val user_count_posts = userWallPosts
      .select($"id", $"owner_id".as("user_id_posts"))
      .groupBy($"user_id_posts").agg(count("id").as("count_posts"))

    val user_count_likes = userWallLikes
      .select($"itemId", $"likerId".as("user_id_likes"))
      .groupBy($"user_id_likes").agg(count("itemId").as("count_likes"))

    val user_count_comments = userWallComments
      .select($"id", $"from_id".as("user_id_comments"))
      .groupBy($"user_id_comments").agg(count("id").as("count_comments"))

    user_count_posts.join(user_count_likes, user_count_posts("user_id_posts") === user_count_likes("user_id_likes"))
      .join(user_count_comments, user_count_likes("user_id_likes") === user_count_comments("user_id_comments"))
      .select($"user_id_posts".as("user_id"), $"count_posts", $"count_likes", $"count_comments")
  }


  /* 2. count of friends, followers, users groups, videos, audios, photos, gifts */
  def Q2DF(spark: SparkSession, friends: DataFrame, followers: DataFrame, userGroupsSubs: DataFrame, userWallPhotos: DataFrame): DataFrame =
  {
    import spark.implicits._

    val user_count_friends = friends
      .select($"follower", $"profile".as("user_id_friends"))
      .groupBy($"user_id_friends").agg(count("follower").as("count_friends"))

    val user_count_followers = followers
      .select($"follower", $"profile".as("user_id_followers"))
      .groupBy($"user_id_followers").agg(count("follower").as("count_followers"))

    val user_count_groups = userGroupsSubs
      .select($"group", $"user".as("user_id_groups"))
      .groupBy($"user_id_groups").agg(count("group").as("count_groups"))

    val user_count_photos = userWallPhotos
      .select($"id", $"owner_id".as("user_id_photos"))
      .groupBy($"user_id_photos").agg(count("id").as("count_photos"))

    user_count_friends.join(user_count_followers, user_count_friends("user_id_friends") === user_count_followers("user_id_followers"))
      .join(user_count_groups, user_count_followers("user_id_followers") === user_count_groups("user_id_groups"))
      .join(user_count_photos, user_count_groups("user_id_groups") === user_count_photos("user_id_photos"))
      .select($"user_id_friends".as("user_id"), $"count_friends", $"count_followers", $"count_groups", $"count_photos")
  }

  /* 3. count of "incoming" (made by other users) comments, max and mean "incoming" comments per post */
  def Q3DF(spark: SparkSession, userWallPosts: DataFrame, userWallComments: DataFrame): DataFrame =
  {
    import spark.implicits._

    val user_count_comments_per_post = userWallPosts
      .join(userWallComments, userWallPosts("id") === userWallComments("post_id"))
      .where(userWallPosts("owner_id").notEqual(userWallComments("from_id")))
      .select(userWallPosts("id").as("post_id"), userWallComments("from_id"))
      .groupBy($"post_id").agg(count("from_id").as("count_comments_per_post"))

    user_count_comments_per_post.join(userWallPosts, user_count_comments_per_post("post_id") === userWallPosts("id"))
      .select(userWallPosts("owner_id").as("user_id"), $"count_comments_per_post")
      .groupBy($"user_id")
      .agg(
        sum("count_comments_per_post").as("total_incoming_comments"),
        max("count_comments_per_post").as("max_incoming_comments_per_post"),
        avg("count_comments_per_post").as("mean_incoming_comments_per_post")
      )
  }

  /* 4. count of "incoming" likes, max and mean "incoming" likes per post */
  def Q4DF(spark: SparkSession, userWallPosts: DataFrame, userWallLikes: DataFrame): DataFrame =
  {
    import spark.implicits._

    val user_count_likes_per_post = userWallPosts
      .join(userWallLikes, userWallPosts("id") === userWallLikes("itemId"))
      .where(userWallPosts("owner_id").notEqual(userWallLikes("likerId")))
      .select(userWallPosts("id").as("post_id"), userWallLikes("likerId"))
      .groupBy($"post_id").agg(count("likerId").as("count_likes_per_post"))

    user_count_likes_per_post.join(userWallPosts, user_count_likes_per_post("post_id") === userWallPosts("id"))
      .select(userWallPosts("owner_id").as("user_id"), $"count_likes_per_post")
      .groupBy($"user_id")
      .agg(
        sum("count_likes_per_post").as("total_incoming_likes"),
        max("count_likes_per_post").as("max_incoming_likes_per_post"),
        avg("count_likes_per_post").as("mean_incoming_likes_per_post")
      )
  }

  /* 5. count of open/closed groups a user participates in */
  def Q5DF(spark: SparkSession, userGroupsSubs: DataFrame, groupsProfiles: DataFrame): DataFrame = {
    import spark.implicits._

    val user_count_opened_groups = userGroupsSubs
      .join(groupsProfiles, userGroupsSubs("group") === groupsProfiles("key"))
      .select( $"user".as("user_id_opened"), $"group")
      .where($"is_closed".equalTo(0))
      .groupBy("user_id_opened").agg(count("group").as("count_groups_opened"))

    val user_count_closed_groups = userGroupsSubs
      .join(groupsProfiles, userGroupsSubs("group") === groupsProfiles("key"))
      .select( $"user".as("user_id_closed"), $"group")
      .where($"is_closed".equalTo(1))
      .groupBy("user_id_closed").agg(count("group").as("count_groups_closed"))

    user_count_opened_groups
      .join(user_count_closed_groups, user_count_opened_groups("user_id_opened") === user_count_closed_groups("user_id_closed"))
      .select($"user_id_opened".as("user_id"), $"count_groups_opened", $"count_groups_closed")
  }

  /* 6. count of deleted users in friends and followers */
  def Q6DF(spark: SparkSession, friends: DataFrame, friendsProfiles: DataFrame, followers: DataFrame, followerProfiles: DataFrame): DataFrame = {
    import spark.implicits._

    val user_count_deleted_friends = friends
      .join(friendsProfiles, friends("follower") === friendsProfiles("id"))
      .where(friendsProfiles("deactivated").equalTo("deleted"))
      .select(friends("profile").as("user_id_friends"), friends("follower"))
      .groupBy("user_id_friends").agg(count("follower").as("count_deleted_friends"))

    val user_count_deleted_followers = followers
      .join(followerProfiles, followers("follower") === followerProfiles("id"))
      .where(followerProfiles("deactivated").equalTo("deleted"))
      .select(followers("profile").as("user_id_followers"), followers("follower"))
      .groupBy("user_id_followers").agg(count("follower").as("count_deleted_followers"))

    user_count_deleted_friends
      .join(user_count_deleted_followers, user_count_deleted_friends("user_id_friends") === user_count_deleted_followers("user_id_followers"))
      .select($"user_id_friends".as("user_id"), $"count_deleted_friends", $"count_deleted_followers")
  }

  /* 7. a) Aggregate (count, max, mean) characterics for comments and likes (separtely) made by friends per user */
  def Q7ADF(spark: SparkSession, userWallPosts: DataFrame, userWallComments: DataFrame, userWallLikes: DataFrame, friends: DataFrame): DataFrame = {
    import spark.implicits._

    val comments_per_post = userWallPosts
      .join(userWallComments, userWallPosts("id") === userWallComments("post_id"))
      .join(friends, userWallComments("from_id") === friends("follower"))
      .where(friends("profile") === userWallComments("post_owner_id"))
      .select(userWallPosts("id").as("post_id"), userWallComments("from_id"))
      .groupBy($"post_id").agg(count("from_id").as("count_comments_per_post"))

    val aggregates_comments_per_post = userWallPosts
      .join(comments_per_post, userWallPosts("id") === comments_per_post("post_id"))
      .select($"owner_id".as("user_id_comments"), $"count_comments_per_post")
      .groupBy($"user_id_comments")
      .agg(
        sum("count_comments_per_post").as("total_comments_from_friends"),
        max("count_comments_per_post").as("max_comments_from_friends_per_post"),
        avg("count_comments_per_post").as("mean_comments_from_friends_per_post")
      )

    val likes_per_post = userWallPosts
      .join(userWallLikes, userWallPosts("id") === userWallLikes("itemId"))
      .join(friends, userWallLikes("likerId") === friends("follower"))
      .where(friends("profile") === userWallLikes("ownerId"))
      .select(userWallPosts("id").as("post_id"), userWallLikes("likerId"))
      .groupBy($"post_id").agg(count("likerId").as("count_likes_per_post"))

    val aggregates_likes_per_post = userWallPosts
      .join(likes_per_post, userWallPosts("id") === likes_per_post("post_id"))
      .select($"owner_id".as("user_id_likes"), $"count_likes_per_post")
      .groupBy($"user_id_likes")
      .agg(
        sum("count_likes_per_post").as("total_likes_from_friends"),
        max("count_likes_per_post").as("max_likes_from_friends_per_post"),
        avg("count_likes_per_post").as("mean_likes_from_friends_per_post")
      )

    aggregates_comments_per_post
      .join(aggregates_likes_per_post, aggregates_comments_per_post("user_id_comments") === aggregates_likes_per_post("user_id_likes"))
      .select($"user_id_likes".as("user_id"),
        $"total_comments_from_friends", $"max_comments_from_friends_per_post", $"mean_comments_from_friends_per_post",
        $"total_likes_from_friends", $"max_likes_from_friends_per_post", $"mean_likes_from_friends_per_post")
  }

  /* 7. b) Aggregate (count, max, mean) characterics for comments and likes (separtely) made by followers per user */
  def Q7BDF(spark: SparkSession, userWallPosts: DataFrame, userWallComments: DataFrame, userWallLikes: DataFrame, followers: DataFrame): DataFrame = {
    import spark.implicits._

    val comments_per_post = userWallPosts
      .join(userWallComments, userWallPosts("id") === userWallComments("post_id"))
      .join(followers, userWallComments("from_id") === followers("follower"))
      .where(followers("profile") === userWallComments("post_owner_id"))
      .select(userWallPosts("id").as("post_id"), userWallComments("from_id"))
      .groupBy($"post_id").agg(count("from_id").as("count_comments_per_post"))

    val aggregates_comments_per_post = userWallPosts
      .join(comments_per_post, userWallPosts("id") === comments_per_post("post_id"))
      .select($"owner_id".as("user_id_comments"), $"count_comments_per_post")
      .groupBy($"user_id_comments")
      .agg(
        sum("count_comments_per_post").as("total_comments_from_followers"),
        max("count_comments_per_post").as("max_comments_from_followers_per_post"),
        avg("count_comments_per_post").as("mean_comments_from_followers_per_post")
      )

    val likes_per_post = userWallPosts
      .join(userWallLikes, userWallPosts("id") === userWallLikes("itemId"))
      .join(followers, userWallLikes("likerId") === followers("follower"))
      .where(followers("profile") === userWallLikes("ownerId"))
      .select(userWallPosts("id").as("post_id"), userWallLikes("likerId"))
      .groupBy($"post_id").agg(count("likerId").as("count_likes_per_post"))

    val aggregates_likes_per_post = userWallPosts
      .join(likes_per_post, userWallPosts("id") === likes_per_post("post_id"))
      .select($"owner_id".as("user_id_likes"), $"count_likes_per_post")
      .groupBy($"user_id_likes")
      .agg(
        sum("count_likes_per_post").as("total_likes_from_followers"),
        max("count_likes_per_post").as("max_likes_from_followers_per_post"),
        avg("count_likes_per_post").as("mean_likes_from_followers_per_post")
      )

    aggregates_comments_per_post
      .join(aggregates_likes_per_post, aggregates_comments_per_post("user_id_comments") === aggregates_likes_per_post("user_id_likes"))
      .select($"user_id_likes".as("user_id"),
        $"total_comments_from_followers", $"max_comments_from_followers_per_post", $"mean_comments_from_followers_per_post",
        $"total_likes_from_followers", $"max_likes_from_followers_per_post", $"mean_likes_from_followers_per_post"
      )
  }

  /* 9. Join all users data from each tasks to the one table with different columns like
     *  posts_count, posts_comments_count, likes_count, friends_count and etc. */
  def Q9DF(spark: SparkSession, path_out: String): DataFrame = {
    import spark.implicits._

    val Q1 = spark.read.parquet(path_out + "Q1DF").as("Q1")
    val Q2 = spark.read.parquet(path_out + "Q2DF").as("Q2")
    val Q3 = spark.read.parquet(path_out + "Q3DF").as("Q3")
    val Q4 = spark.read.parquet(path_out + "Q4DF").as("Q4")
    val Q5 = spark.read.parquet(path_out + "Q5DF").as("Q5")
    val Q6 = spark.read.parquet(path_out + "Q6DF").as("Q6")

    Q1.join(Q2, Q1("user_id") === Q2("user_id"), "left_outer" )
      .join(Q3, Q1("user_id") === Q3("user_id"), "left_outer")
      .join(Q4, Q1("user_id") === Q4("user_id"), "left_outer")
      .join(Q5, Q1("user_id") === Q5("user_id"), "left_outer")
      .join(Q6, Q1("user_id") === Q6("user_id"), "left_outer")
      .select(Q1("user_id"),
        $"count_friends", $"count_followers", $"count_groups", $"count_photos",
        $"total_incoming_comments", $"max_incoming_comments_per_post", $"mean_incoming_comments_per_post",
        $"total_incoming_likes", $"max_incoming_likes_per_post", $"mean_incoming_likes_per_post",
        $"count_groups_opened", $"count_groups_closed",
        $"count_deleted_friends", $"count_deleted_followers")

  }


}