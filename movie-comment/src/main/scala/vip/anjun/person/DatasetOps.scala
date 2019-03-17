package vip.anjun.person

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer


/**
  * Dataset开发实战企业人员管理系统应用案例代码
  */
object DatasetOps {

  /**
    * Case Class的特别之处在于：
    *
    * 编译器会为Case Class自动生成以下方法：
    * equals & hashCode
    * toString
    * copy
    * 编译器会为Case Class自动生成伴生对象
    *
    * 编译器会为伴生对象自动生成以下方法
    * apply
    * unapply
    * 这意味着你可以不必使用new关键字来实例化一个case class.
    *
    * case class的类参数在不指定val/var修饰时，会自动编译为val，即对外只读，如果需要case class的字段外部可写，可以显式地指定var关键字！
    */
  case class Person(name: String, age: Long)
  case class Score(n: String, score: Long)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("DatasetOps").master("local[4]").
      config("spark.sql.warehouse", "/home/ghost/IdeaProjects/SparkBook/spark-warehouse").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    /**
      * Dataset中的tranformation和Action操作，Action类型的操作有：
      * show collect first reduce take count等
      * 这些操作都会产生结果，也就是说会执行逻辑计算的过程
      */
    val personsDF = spark.read.json("data/peoplemanagedata/people.json")
    val personScoresDF = spark.read.json("data/peoplemanagedata/peopleScores.json")
    // 生成Dataset
    val personsDS = personsDF.as[Person]
    val personScoresDS = personScoresDF.as[Score]

    println("使用groupBy算子进行分组：")
    /**
      * +-------+---+-----+
      * |   name|age|count|
      * +-------+---+-----+
      * | Justin| 29|    1|
      * |   Andy| 30|    1|
      * |Michael| 16|    1|
      * |Michael| 46|    1|
      * | Justin| 19|    1|
      * +-------+---+-----+
      */
    // $"columnName"  Scala short hand for a named column
    val personsDSGrouped = personsDS.groupBy($"name", $"age").count()
    personsDSGrouped.show()

    println("使用agg算子concat内置函数将姓名、年龄连接在一起成为单个字符串列 ：")

    /**
      * +-------+---+-----------------+
      * |   name|age|concat(name, age)|
      * +-------+---+-----------------+
      * | Justin| 29|         Justin29|
      * |   Andy| 30|           Andy30|
      * |Michael| 16|        Michael16|
      * |Michael| 46|        Michael46|
      * | Justin| 19|         Justin19|
      * +-------+---+-----------------+
      */
    personsDS.groupBy($"name", $"age").agg(concat($"name", $"age")).show()

    println("使用col算子选择列 ：")

    /**
      * +------------+------------+
      * |          _1|          _2|
      * +------------+------------+
      * |[16,Michael]|[Michael,88]|
      * |   [30,Andy]|  [Andy,100]|
      * | [19,Justin]| [Justin,89]|
      * | [29,Justin]| [Justin,89]|
      * |[46,Michael]|[Michael,88]|
      * +------------+------------+
      */
    personsDS.joinWith(personScoresDS, personsDS.col("name")===personScoresDS.col("n")).show()

    println("使用sum、avg等函数计算年龄总和、平均年龄、最大年龄、最小年龄、唯一年龄计数、平均年龄、当前时间等数据 ：")

    /**
      * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      * |   name|sum(age)|avg(age)|max(age)|min(age)|count(age)|count(DISTINCT age)|avg(age)|current_date()|
      * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      * |Michael|      62|    31.0|      46|      16|         2|                  2|    31.0|    2019-02-26|
      * |   Andy|      30|    30.0|      30|      30|         1|                  1|    30.0|    2019-02-26|
      * | Justin|      48|    24.0|      29|      19|         2|                  2|    24.0|    2019-02-26|
      * +-------+--------+--------+--------+--------+----------+-------------------+--------+--------------+
      */
    personsDS.groupBy($"name").agg(sum($"age"), avg($"age"), max($"age"), min($"age"), count($"age"),
      countDistinct($"age"), mean($"age"), current_date()).show()

    println("函数collect_list、collect_set比较，collect_list函数结果中包含重复元素；collect_set函数结果中无重复元素：")

    /**
      * +-------+------------------+-----------------+
      * |   name|collect_list(name)|collect_set(name)|
      * +-------+------------------+-----------------+
      * |Michael|[Michael, Michael]|        [Michael]|
      * |   Andy|            [Andy]|           [Andy]|
      * | Justin|  [Justin, Justin]|         [Justin]|
      * +-------+------------------+-----------------+
      */
    personsDS.groupBy($"name").agg(collect_list($"name"), collect_set($"name")).show()

    println("使用sample算子进行随机采样：")

    /**
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 16|Michael|
      * | 30|   Andy|
      * | 19| Justin|
      * | 46|Michael|
      * +---+-------+
      * 不保证刚好0.5
      */
    personsDS.sample(false, 0.5).show()

    println("使用randomSplit算子进行随机切分：")

    /**
      * +---+------+
      * |age|  name|
      * +---+------+
      * | 29|Justin|
      * +---+------+
      *
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 16|Michael|
      * | 19| Justin|
      * | 30|   Andy|
      * | 46|Michael|
      * +---+-------+
      *
      * Array里面两个数为权重
      */
    personsDS.randomSplit(Array(10, 20)).foreach(dataset => dataset.show())

    println("使用select算子选择列：")

    /**
      * +-------+
      * |   name|
      * +-------+
      * |Michael|
      * |   Andy|
      * | Justin|
      * | Justin|
      * |Michael|
      * +-------+
      */
    personsDS.select("name").show()

    println("使用joinWith算子关联企业人员信息、企业人员分数评分信息：")

    /**
      * +------------+------------+
      * |          _1|          _2|
      * +------------+------------+
      * |[16,Michael]|[Michael,88]|
      * |   [30,Andy]|  [Andy,100]|
      * | [19,Justin]| [Justin,89]|
      * | [29,Justin]| [Justin,89]|
      * |[46,Michael]|[Michael,88]|
      * +------------+------------+
      */
    personsDS.joinWith(personScoresDS, $"name"===$"n").show()

    println("使用join算子关联企业人员信息、企业人员分数评分信息：")

    /**
      * +---+-------+-------+-----+
      * |age|   name|      n|score|
      * +---+-------+-------+-----+
      * | 16|Michael|Michael|   88|
      * | 30|   Andy|   Andy|  100|
      * | 19| Justin| Justin|   89|
      * | 29| Justin| Justin|   89|
      * | 46|Michael|Michael|   88|
      * +---+-------+-------+-----+
      */
    personsDS.join(personScoresDS, $"name"===$"n").show()

    println("使用sort算子对年龄进行降序排序：")

    /**
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 46|Michael|
      * | 30|   Andy|
      * | 29| Justin|
      * | 19| Justin|
      * | 16|Michael|
      * +---+-------+
      */
    personsDS.sort($"age".desc).show()

    def myFlatMapFunction(myPerson: Person, myEncoder: Person): Dataset[Person] = {
      personsDS
    }

    /**
      * +-------+---+
      * |     _1| _2|
      * +-------+---+
      * |Michael| 46|
      * |   Andy|100|
      * | Justin| 49|
      * | Justin| 59|
      * |Michael| 76|
      * +-------+---+
      */
    personsDS.flatMap(persons => persons match {
      case Person(name, age) if name=="Andy" => List((name, age+70))
      case Person(name, age) => List((name, age+30))
    }).show()

    /**
      * +-------+----+
      * |     _1|  _2|
      * +-------+----+
      * |Michael|1016|
      * |   Andy|1030|
      * | Justin|1019|
      * | Justin|1029|
      * |Michael|1046|
      * +-------+----+
      */
    personsDS.mapPartitions{ persons =>
      val result = ArrayBuffer[(String, Long)]()
      while (persons.hasNext){
        val person = persons.next()
        result += ((person.name, person.age+1000))
      }
      result.iterator
    }.show()

    println("使用dropDuplicates算子统计企业人员管理系统姓名无重复员工的记录：")

    /**
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 16|Michael|
      * | 30|   Andy|
      * | 19| Justin|
      * +---+-------+
      */
    personsDS.dropDuplicates("name").show()

    /**
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 46|Michael|
      * | 30|   Andy|
      * | 19| Justin|
      * | 16|Michael|
      * | 29| Justin|
      * +---+-------+
      */
    personsDS.distinct().show()

    println("使用repartition算子设置分区：")

    /**
      * 原分区数：1
      */
    println("原分区数：" + personsDS.rdd.partitions.size)
    val repartitionDs = personsDS.repartition(4)

    /**
      * repartition设置分区数:4
      */
    println("repartition设置分区数:" + repartitionDs.rdd.partitions.size)

    println("使用coalesce算子设置分区：")

    /**
      * repartition(numPartitions:Int):RDD[T]和coalesce(numPartitions:Int，shuffle:Boolean=false):RDD[T]
      *
      * 他们两个都是RDD的分区进行重新划分，repartition只是coalesce接口中shuffle为true的简易实现，（假设RDD有N个分区，需要重新划分成M个分区）
      *
      * 1）、N<M。一般情况下N个分区有数据分布不均匀的状况，利用HashPartitioner函数将数据重新分区为M个，这时需要将shuffle设置为true。
      *
      * 2）如果N>M并且N和M相差不多，(假如N是1000，M是100)那么就可以将N个分区中的若干个分区合并成一个新的分区，最终合并为M个分区，这时可以将shuff设置为false，在shuffl为false的情况下，如果M>N时，coalesce为无效的，不进行shuffle过程，父RDD和子RDD之间是窄依赖关系。
      *
      * 3）如果N>M并且两者相差悬殊，这时如果将shuffle设置为false，父子ＲＤＤ是窄依赖关系，他们同处在一个Ｓｔａｇｅ中，就可能造成spark程序的并行度不够，从而影响性能，如果在M为1的时候，为了使coalesce之前的操作有更好的并行度，可以讲shuffle设置为true。
      *
      * 总之：如果shuff为false时，如果传入的参数大于现有的分区数目，RDD的分区数不变，也就是说不经过shuffle，是无法将RDDde分区数变多的。
      *
      * coalesce 合并
      */
    val coalesced: Dataset[Person] = repartitionDs.coalesce(2)

    /**
      * coalesce设置分区数：2
      */
    println("coalesce设置分区数：" + coalesced.rdd.partitions.size)

    /**
      * +---+-------+
      * |age|   name|
      * +---+-------+
      * | 30|   Andy|
      * | 19| Justin|
      * | 29| Justin|
      * | 16|Michael|
      * | 46|Michael|
      * +---+-------+
      */
    coalesced.show

    spark.stop()

  }
}
