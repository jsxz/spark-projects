package vip.anjun.movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet


/**
  * ratings.dat
  * UserID::MovieID::Rating::Timestamp
  * user.data
  * UserID::Gender::Age::Occupation::Zip-code
  * movies.dat
  * MovieID::Title::Genres
  * Occupation.dat
  * id::name
  *
  * @author anjun
  * @date 2019-03-17 11:26
  */
object MovieUserAnalyzerRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("log").setLevel(Level.ERROR)
    var masterUrl = "local[4]"
    var dataPath = "movie-comment/src/main/resources/data/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyzer"))
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    println("所有电影中平均得分最高的电影:")
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache()

    //    ratings.map(x => (x._2, (x._3.toDouble, 1))) //(movieID,(rating,1)) 及评分及人数
    //      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //每部电影的总评分和总人数
    //      .map(x => (x._2._1.toDouble / x._2._2, x._1)) //电影平均分
    //      .sortByKey(false) //降序
    //      .take(10)
    //      .foreach(println)
    //
    //    println("所有电影中粉丝最多的电影:")
    //    ratings.map(x => (x._2, 1)).reduceByKey(_ + _).map(x => (x._2, x._1))
    //      .sortByKey(false)
    //      .map(x => (x._2, x._1)).take(10).foreach(println)

    val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3))).join(
      usersRDD.map(_.split("::")).map(x => (x(0), x(1))).cache()
    )
    genderRatings.take(2).foreach(println)
    val maleFilteredRatings = genderRatings.filter(x => x._2._2.equals("M"))
      .map(x => x._2._1) //genderRatings with male rating
    val feMaleFilteredRatins = genderRatings.filter(x => x._2._2.equals("F"))
      .map(x => x._2._1) //genderRatings with female rating

    println("所有电影中最受男生喜爱的电影top10:")
    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1))) //(movieID,(rating,1)) 及评分及人数
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //每部电影的总评分和总人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1)) //电影平均分
      .sortByKey(false) //降序
      .take(10)
      .foreach(println)
    println("所有电影中最受女生喜爱的电影top10:")
    feMaleFilteredRatins.map(x => (x._2, (x._3.toDouble, 1))) //(movieID,(rating,1)) 及评分及人数
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) //每部电影的总评分和总人数
      .map(x => (x._2._1.toDouble / x._2._2, x._1)) //电影平均分
      .sortByKey(false) //降序
      .take(10)
      .foreach(println)
    //===================================================
    val targetQQUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_
      ._2.equals("18"))
    val targetTaobaoUesrs = usersRDD.map(_.split("::")).map(x => (x(0), x(2)))
      .filter(_._2.equals("25"))
    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaobaoUsersSet = HashSet() ++ targetTaobaoUesrs.map(_._1).collect()
    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaobaoUsersBroadcast = sc.broadcast(targetTaobaoUsersSet)

    val movieID2Name = moviesRDD.map(_.split("::")).map(x => (x(0), x(1)))
      .collect().toMap
    println("所有电影中QQ或微信核心目标用户最喜欢电影TopN分析：")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x => targetQQUsersBroadcast.value.contains(x._1)
      ).map(x=>(x._2,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false)
      .map(x=>(x._2,x._1)).take(10)
      .map(x=>(movieID2Name.getOrElse(x._1,null),x._2)).foreach(println)

    println("所有电影中taobao核心目标用户最喜欢电影TopN分析：")
    ratingsRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x =>
      targetTaobaoUsersBroadcast.value.contains(x._1)
    ).map(x=>(x._2,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false)
      .map(x=>(x._2,x._1)).take(10)
      .map(x=>(movieID2Name.getOrElse(x._1,null),x._2)).foreach(println) //名字，个数
  //=============================二次排序功能实现==================
    println("对电影评分系统数据以timpestamp和rating两个维度二次排序：")
    val pairWithSortkey=ratingsRDD.map(line=>{
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)
    })
    val sorted =pairWithSortkey.sortByKey(false)
    val sortedResult = sorted.map(_._2)
    sortedResult.take(10).foreach(println)

  }
class SecondarySortKey(val first:Double,val sencond:Double) extends
  Ordered[SecondarySortKey] with Serializable{
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first !=0){
      (this.first - that.first).toInt
    }else{
      if (this.sencond-that.sencond>0){
        Math.ceil(this.sencond-that.sencond).toInt
      }else if(this.sencond-that.sencond<0){
        Math.floor(this.sencond-that.sencond).toInt
      }else{
        (this.sencond-that.sencond).toInt
      }
    }
  }
}
  def test(args: Array[String]): Unit = {
    var masterUrl = "local[4]"
    var dataPath = "movie-comment/src/main/resources/data/"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("MovieUserAnalyzer"))
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    val userBasic = usersRDD.map(_.split("::")).map {
      //UserID::Gender::Age::Occupation::Zip-code
      user => (user(3), (user(0), user(1), user(2)))
    }
    for (elem <- userBasic.collect().take(22)) {
      println("userBasicRDD(职业ID,(用户ID,性别,年龄):" + elem)
    }
    val occupations = occupationsRDD.map(_.split("::")).map(job => {
      (job(0).trim, job(1))
    }
    )
    for (e <- occupations.collect().take(22)) {
      println("occupationsRDD(职业ID，职业名):" + e)
    }
    val userInfor = userBasic.join(occupations)
    userInfor.cache()
    for (e <- userInfor.take(2)) {
      println("usernfoRDD(职业ID,((用户ID,性别，年龄)，职业名):" + e)
    }
    val targetMovie = ratingsRDD.map(_.split("::")).map(x =>
      (x(0), x(1))).filter(_._2.equals("1193"))
    for (e <- targetMovie.collect().take(2)) {
      println("targetMoiveRDD(用户ID,电影ID)" + e)
    }
    val targetUsers = userInfor.map(x => (x._2._1._1, x._2))
    for (e <- targetUsers.collect().take(2)) {
      println("targetUsers(用户ID,((用户ID,性别，年龄)，职业名):" + e)
    }
    println("电影点评系统用户行为分析，统计观看电影iD为1193的电影用户信息：用户ID,性别，年龄，职业名")
    val userInfoForSecificMovie = targetMovie.join(targetUsers)
    for (e <- userInfoForSecificMovie.collect().take(10)) {
      println("userinfoForSpecificMovie(用户ID,(电影ID,((用户ID,性别，年龄)，职业名):" + e)
    }
  }
}
