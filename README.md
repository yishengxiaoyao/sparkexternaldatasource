# 自定义外部数据源

翻译自：[Extending Spark Datasource API: write a custom spark datasource](http://sparkdatasourceapi.blogspot.com/2016/10/spark-data-source-api-write-custom.html)

## Data Source API
### Basic Interfaces
>* BaseRelation:展示从DataFrame中产生的底层数据源的关系或者表。定义如何产生schema信息。或者说是数据源的关系。
>* RelationProvider:获取参数列表，返回一个BaseRelation对象。
>* TableScan：对数据的schame信息，进行完整扫描，返回一个没有过滤的RDD。
>* DataSourceRegister:定义数据源的简写。
### Providers
>* SchemaRelationProvider：用户可以自定义schema信息。
>* CreatableRelationProvider：用户可以定义从DataFrame中产生新的Relation。
### Scans
>* PrunedScan:自定义方法，删除不需要的列。
>* PrunedFilteredScan：自定义方法，删除不需要的列，并且对列的值进行过滤。
>* CatalystScan：用于试验与查询计划程序的更直接连接的界面。
### Relations
>* InsertableRelation:插入数据。三个假设：1.插入方法提供的数据与BaseRelation中定义的Schame信息匹配到；2.schema信息不变；3.插入方法中的数据都是可以为null的。
>* HadoopFsRelation：Hadoop 文件系统的数据源。
### Output Interfaces
 如果使用HadoopFsRelation，会使用到这一块。
## 准备工作

### 数据格式
使用文本数据作为数据源，文件中的数据都是以都好分割，行之间以回车为分隔符，数据的格式为：
```
//编号,名称,性别(1为男性,0为女性),工资，费用
10001,Alice,0,30000,12000
```
### 创建项目
在IDEA中创建一个maven项目，添加相应的spark、scala依赖。
```
<properties>
    <scala.version>2.11.8</scala.version>
    <spark.version>2.2.0</spark.version>
</properties>

<repositories>
    <repository>
        <id>scala-tools.org</id>
        <name>Scala-Tools Maven2 Repository</name>
        <url>http://scala-tools.org/repo-releases</url>
    </repository>
</repositories>

<pluginRepositories>
    <pluginRepository>
        <id>scala-tools.org</id>
        <name>Scala-Tools Maven2 Repository</name>
        <url>http://scala-tools.org/repo-releases</url>
    </pluginRepository>
</pluginRepositories>

<dependencies>
    <dependency>
        <groupId>org.scala-lang</groupId>
        <artifactId>scala-library</artifactId>
        <version>${scala.version}</version>
    </dependency>
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>4.4</version>
    </dependency>
    <dependency>
        <groupId>org.specs</groupId>
        <artifactId>specs</artifactId>
        <version>1.2.5</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
        <version>${spark.version}</version>
    </dependency>

</dependencies>
```
##开始编写自定义数据源
###创建Schema信息
为了自定义Schema信息，必须要创建一个DefaultSource的类(源码规定，如果不命名为DefaultSource，会报找不到DefaultSource类的错误)。
还需要继承RelationProvider和SchemaRelationProvider。RelationProvider用来创建数据的关系，SchemaRelationProvider用来明确schema信息。
在编写DefaultSource.scala文件时，如果文件存在的情况下，需要创建相应的Relation来根据路径读取文件。

DefaultSource.scala文件代码：
```
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path=parameters.get("path")
    path match {
      case Some(p) => new TextDataSourceRelation(sqlContext,p,schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }
}
```
在编写Relation时，需要实现BaseRelation来重写自定数据源的schema信息。如果是parquet/csv/json文件，可以直接获取schema信息。
然后实现序列化接口，为了网络传输。

TextDataSourceRelation.scala文件的代码：
```
class TextDataSourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
    extends BaseRelation
      with Serializable {
  override def schema: StructType = {
    if(userSchema!=null){
      userSchema
    }else{
      StructType(
        StructField("id",IntegerType,false) ::
        StructField("name",StringType,false) ::
        StructField("gender",StringType,false) ::
        StructField("salary",LongType,false) ::
        StructField("expenses",LongType,false) :: Nil)
    }
  }
}
```

根据上面编写代码，可以简单测试一下是否可以拿到正确的schema信息。
在编写测试方法时，使用sqlContext.read来读取文件，使用format参数来指定自定义数据源的包路径，使用printSchema()验证是否可以拿到相应的schema信息。
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("com.edu.spark.text").load("/Users/Downloads/data")
  println("output schema...")
  df.printSchema()
  println("Application Ended...")
}
```

输出的schema信息如下：
```
output schema...
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = false)
 |-- gender: string (nullable = false)
 |-- salary: long (nullable = false)
 |-- expenses: long (nullable = false)
```
通过输出的schema，与自己定义的schema一致。

### 读取数据

为了读取数据，TextDataSourceRelation需要实现TableScan，实现buildScan()方法。
这个方法会将数据以Row组成的RDD的形式返回数据，每一个Row表示一行数据。
在读取文件时，使用WholeTextFiles根据指定的路径来读取文件，返回的形式为(文件名，内容)。
在读取数据之后,然后按照逗号分割数据，将性别这个字段根据数字转换为相应的字符串，然后根据在shema信息，转换为相应的类型。

转换的代码如下：
```
object Util {
  def castTo(value : String, dataType : DataType) ={
    dataType match {
      case _ : IntegerType => value.toInt
      case _ : LongType => value.toLong
      case _ : StringType => value
    }
  }
}
```
实现TableScan的代码：
```
override def buildScan(): RDD[Row] = {
    println("TableScan: buildScan called...")
    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)
    
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) =>
          val colName = schemaFields(index).name
          Util.castTo(
            if (colName.equalsIgnoreCase("gender")) {
              if(value.toInt == 1) {
                "Male"
              } else {
                "Female"
              }
            } else {
              value
            }, schemaFields(index).dataType)
      })
      tmp.map(s => Row.fromSeq(s))
    })
    rows.flatMap(e => e)
}

```
测试是否可以读取到数据的代码：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("com.edu.spark.text").load("/Users/Downloads/data")
  df.show()
  println("Application Ended...")
}
```
拿到的数据为：
```
+-----+---------------+------+------+--------+
|   id|           name|gender|salary|expenses|
+-----+---------------+------+------+--------+
|10002|    Alice Heady|Female| 20000|    8000|
|10003|    Jenny Brown|Female| 30000|  120000|
|10004|     Bob Hayden|  Male| 40000|   16000|
|10005|    Cindy Heady|Female| 50000|   20000|
|10006|     Doug Brown|  Male| 60000|   24000|
|10007|Carolina Hayden|Female| 70000|  280000|
+-----+---------------+------+------+--------+
```
### 写数据

本代码有两种编写方法：自定义格式和Json。
继承CreateTableRelationProvider，实现createRelation方法。

```
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext,parameters,null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path=parameters.get("path")
    path match {
      case Some(p) => new TextDataSourceRelation(sqlContext,p,schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-datasource format!!")
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", "./output/") //can throw an exception/error, it's just for this tutorial
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + path + " already exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val formatName = parameters.getOrElse("format", "customFormat")
    formatName match {
      case "customFormat" => saveAsCustomFormat(data, path, mode)
      case "json" => saveAsJson(data, path, mode)
      case _ => throw new IllegalArgumentException(formatName + " is not supported!!!")
    }
    createRelation(sqlContext, parameters, data.schema)
  }
  private def saveAsJson(data : DataFrame, path : String, mode: SaveMode): Unit = {
    /**
      * Here, I am using the dataframe's Api for storing it as json.
      * you can have your own apis and ways for saving!!
      */
    data.write.mode(mode).json(path)
  }

  private def saveAsCustomFormat(data : DataFrame, path : String, mode: SaveMode): Unit = {
    /**
      * Here, I am  going to save this as simple text file which has values separated by "|".
      * But you can have your own way to store without any restriction.
      */
    val customFormatRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString("|")
    })
    customFormatRDD.saveAsTextFile(path)
  }
}
```
测试代码：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("com.edu.spark.text").load("/Users/Downloads/data")
  //save the data
    df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_custom/")
    df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_json/")
    df.write.mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_none/")
  println("Application Ended...")
}
```
输出的结果：

自定义格式：
```
10002|Alice Heady|Female|20000|8000
10003|Jenny Brown|Female|30000|120000
10004|Bob Hayden|Male|40000|16000
10005|Cindy Heady|Female|50000|20000
10006|Doug Brown|Male|60000|24000
10007|Carolina Hayden|Female|70000|280000
```
Json格式：
```
{"id":10002,"name":"Alice Heady","gender":"Female","salary":20000,"expenses":8000}
{"id":10003,"name":"Jenny Brown","gender":"Female","salary":30000,"expenses":120000}
{"id":10004,"name":"Bob Hayden","gender":"Male","salary":40000,"expenses":16000}
{"id":10005,"name":"Cindy Heady","gender":"Female","salary":50000,"expenses":20000}
{"id":10006,"name":"Doug Brown","gender":"Male","salary":60000,"expenses":24000}
{"id":10007,"name":"Carolina Hayden","gender":"Female","salary":70000,"expenses":280000}
```
### 修建列
继承PrunedScan，实现buildScan方法,只展示需要的项。
```
override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    println("PrunedScan: buildScan called...")
    
    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)
    
    val rows = rdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
      val tmp = data.map(words => words.zipWithIndex.map{
        case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {if(value.toInt == 1) "Male" else "Female"} else value,
            schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      })
    
      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })
    
    rows.flatMap(e => e)
}
```
测试代码：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("com.edu.spark.text").load("/Users/Downloads/data")
  //select some specific columns
  df.createOrReplaceTempView("test")
  spark.sql("select id, name, salary from test").show()
  println("Application Ended...")
}
```
输出的结果为：
```
+-----+---------------+------+
|10002|    Alice Heady| 20000|
|10003|    Jenny Brown| 30000|
|10004|     Bob Hayden| 40000|
|10005|    Cindy Heady| 50000|
|10006|     Doug Brown| 60000|
|10007|Carolina Hayden| 70000|
+-----+---------------+------+
```
### 过滤
继承PrunedFilterScan，实现buildScan方法,只展示需要的项。
```
override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("PrunedFilterScan: buildScan called...")
    
    println("Filters: ")
    filters.foreach(f => println(f.toString))
    
    var customFilters: Map[String, List[CustomFilter]] = Map[String, List[CustomFilter]]()
    filters.foreach( f => f match {
      case EqualTo(attr, value) =>
        println("EqualTo filter is used!!" + "Attribute: " + attr + " Value: " + value)
    
        /**
          * as we are implementing only one filter for now, you can think that this below line doesn't mak emuch sense
          * because any attribute can be equal to one value at a time. so what's the purpose of storing the same filter
          * again if there are.
          * but it will be useful when we have more than one filter on the same attribute. Take the below condition
          * for example:
          * attr > 5 && attr < 10
          * so for such cases, it's better to keep a list.
          * you can add some more filters in this code and try them. Here, we are implementing only equalTo filter
          * for understanding of this concept.
          */
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "equalTo")
        })
      case GreaterThan(attr,value) =>
        println("GreaterThan Filter is used!!"+ "Attribute: " + attr + " Value: " + value)
        customFilters = customFilters ++ Map(attr -> {
          customFilters.getOrElse(attr, List[CustomFilter]()) :+ new CustomFilter(attr, value, "greaterThan")
        })
      case _ => println("filter: " + f.toString + " is not implemented by us!!")
    })
    
    val schemaFields = schema.fields
    // Reading the file's content
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(f => f._2)
    
    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val data = lines.map(line => line.split(",").map(word => word.trim).toSeq)
    
      val filteredData = data.map(s => if (customFilters.nonEmpty) {
        var includeInResultSet = true
        s.zipWithIndex.foreach {
          case (value, index) =>
            val attr = schemaFields(index).name
            val filtersList = customFilters.getOrElse(attr, List())
            if (filtersList.nonEmpty) {
              if (CustomFilter.applyFilters(filtersList, value, schema)) {
              } else {
                includeInResultSet = false
              }
            }
        }
        if (includeInResultSet) s else Seq()
      } else s)
    
      val tmp = filteredData.filter(_.nonEmpty).map(s => s.zipWithIndex.map {
        case (value, index) =>
          val colName = schemaFields(index).name
          val castedValue = Util.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value.toInt == 1) "Male" else "Female"
          } else value,
            schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      })
    
      tmp.map(s => Row.fromSeq(s.filter(_.isDefined).map(value => value.get)))
    })
    
    rows.flatMap(e => e)
}
```
测试代码：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("com.edu.spark.text").load("/Users/Downloads/data")
  //filter data
  df.createOrReplaceTempView("test")
  spark.sql("select id,name,gender from test where salary == 50000").show()

  println("Application Ended...")
}
```
输出的结果为：
```
+-----+-----------+------+
|   id|       name|gender|
+-----+-----------+------+
|10005|Cindy Heady|Female|
+-----+-----------+------+
```

### 注册自定义数据源

实现DataSourceRegister的shortName。

实现代码如下：
```
override def shortName(): String = "udftext"
```
然后在resource目录下，创建文件为META-INF/services/org.apache.spark.sql.sources.DataSourceRegister,文件内容如下：
```
com.edu.spark.text.DefaultSource
```
测试代码如下：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.format("udftext").load("/Users/Downloads/data")
  df.show()
  println("Application Ended...")
}
```
输出的结果为：
```
+-----+---------------+------+------+--------+
|   id|           name|gender|salary|expenses|
+-----+---------------+------+------+--------+
|10002|    Alice Heady|Female| 20000|    8000|
|10003|    Jenny Brown|Female| 30000|  120000|
|10004|     Bob Hayden|  Male| 40000|   16000|
|10005|    Cindy Heady|Female| 50000|   20000|
|10006|     Doug Brown|  Male| 60000|   24000|
|10007|Carolina Hayden|Female| 70000|  280000|
+-----+---------------+------+------+--------+
```
编写相应的DataFrameReader来简写自定义的数据源,代码如下：
```
object ReaderObject {
  implicit class UDFTextReader(val reader: DataFrameReader) extends AnyVal{
    def udftext(path:String) = reader.format("udftext").load(path)
  }
}
```
测试代码(需要将隐士转换导入相应的DataFrameReader)：
```
object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  val df=spark.sqlContext.read.udftext("/Users/Downloads/data")
  df.show()
  println("Application Ended...")
}
```
输出结果与上面一致，不再赘述。

## 附加CustomFilter.scala代码
```
case class CustomFilter(attr : String, value : Any, filter : String)
object CustomFilter {
  def applyFilters(filters : List[CustomFilter], value : String, schema : StructType): Boolean = {
    var includeInResultSet = true

    val schemaFields = schema.fields
    val index = schema.fieldIndex(filters.head.attr)
    val dataType = schemaFields(index).dataType
    val castedValue = Util.castTo(value, dataType)

    filters.foreach(f => {
      val givenValue = Util.castTo(f.value.toString, dataType)
      f.filter match {
        case "equalTo" => {
          includeInResultSet = castedValue == givenValue
          println("custom equalTo filter is used!!")
        }
        case "greaterThan" => {
          includeInResultSet = castedValue.equals(givenValue)
          println("custom greaterThan filter is used!!")
        }
        case _ => throw new UnsupportedOperationException("this filter is not supported!!")
      }
    })

    includeInResultSet
  }
}
```
##参考文献
1. [Extending Spark Datasource API: write a custom spark datasource](http://sparkdatasourceapi.blogspot.com/2016/10/spark-data-source-api-write-custom.html)(主要是翻译这片文章)
2. [How to create a custom Spark SQL data source (using Parboiled2)](https://michalsenkyr.github.io/2017/02/spark-sql_datasource)
3. [spark-custom-datasource-example](https://github.com/aokolnychyi/spark-custom-datasource-example)
4. [Implement REST DataSource using Spark DataSource API ](https://www.alibabacloud.com/forum/read-474)
5. [spark-custom-datasource](https://github.com/VishvendraRana/spark-custom-datasource)
6. [spark-excel](https://github.com/Timehsw/spark-excel)
