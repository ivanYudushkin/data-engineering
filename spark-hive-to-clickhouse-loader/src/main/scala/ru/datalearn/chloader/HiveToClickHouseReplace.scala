package ru.datalearn.chloader

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
  * Universal Spark Scala loader: Hive table -> ClickHouse (replace).
  *
  * Params (KEY=VALUE):
  * - HIVE_SOURCE=schema.table
  * - HIVE_FILTERS=expr1,expr2,... (optional; comma-separated Spark SQL expressions)
  * - HIVE_DATE_COLUMN=colName (optional; if set -> load by years)
  *
  * - CLICKHOUSE_HOST=host[:port]
  * - CLICKHOUSE_LOGIN=user
  * - CLICKHOUSE_PASSWORD=pass
  * - CLICKHOUSE_TARGET=schema.table (optional; default = HIVE_SOURCE)
  * - CLICKHOUSE_CLUSTER=clusterName
  * - CLICKHOUSE_PARTITION_BY=expression
  * - CLICKHOUSE_ORDER_BY=expression (comma-separated ClickHouse expressions)
  * - CLICKHOUSE_SHARDING_KEY=expression (optional; default cityHash64(first order-by column))
  *
  * - JDBC_BATCHSIZE=50000 (optional)
  */
object HiveToClickHouseReplace extends App {

  private val SPARK_SETTINGS = Seq(
    "spark.yarn.queue" -> "default",
    "spark.driver.memory" -> "4G",
    "spark.driver.maxResultSize" -> "4G",
    "spark.executor.memory" -> "8G",
    "spark.executor.memoryOverhead" -> "1024",
    "spark.executor.cores" -> "3",
    "spark.executor.instances" -> "8",
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.dynamicAllocation.maxExecutors" -> "100",
    "spark.sql.sources.partitionOverwriteMode" -> "dynamic",
    "spark.sql.shuffle.partitions" -> "800",
    "spark.default.parallelism" -> "800",
    "hive.exec.dynamic.partition" -> "true",
    "hive.exec.dynamic.partition.mode" -> "nonstrict",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer" -> "1000m",
    "spark.kryoserializer.buffer.max" -> "2000m",
    "spark.sql.hive.convertMetastoreOrc" -> "false",
    "spark.sql.hive.convertMetastoreParquet" -> "false",
    "spark.sql.hive.metastorePartitionPruning" -> "true",
    "spark.eventLog.enabled" -> "true",
    "spark.task.maxFailures" -> "10",
    "spark.speculation" -> "true",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1"
  )

  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      arg.split("=", 2) match {
        case Array(k, v) => Some(k.trim -> v.trim)
        case _ => None
      }
    }.toMap
  }

  private def required(params: Map[String, String], key: String): String =
    params.getOrElse(key, throw new IllegalArgumentException(s"Missing required param: $key"))

  private def opt(params: Map[String, String], key: String): Option[String] =
    params.get(key).map(_.trim).filter(_.nonEmpty)

  private val params = parseArgs(args)

  private val hiveSource = required(params, "HIVE_SOURCE")
  private val hiveFilters = opt(params, "HIVE_FILTERS")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty))
    .getOrElse(Seq.empty)
  private val hiveDateColumnOpt = opt(params, "HIVE_DATE_COLUMN")

  private val chHostRaw = required(params, "CLICKHOUSE_HOST")
  private val chUser = required(params, "CLICKHOUSE_LOGIN")
  private val chPass = required(params, "CLICKHOUSE_PASSWORD")
  private val chTarget = params.getOrElse("CLICKHOUSE_TARGET", hiveSource).trim
  private val chCluster = required(params, "CLICKHOUSE_CLUSTER")
  private val chPartitionBy = required(params, "CLICKHOUSE_PARTITION_BY")
  private val chOrderBy = required(params, "CLICKHOUSE_ORDER_BY")
  private val chShardingKey = opt(params, "CLICKHOUSE_SHARDING_KEY")

  private val jdbcBatchSize = opt(params, "JDBC_BATCHSIZE").getOrElse("50000")

  private val Array(chDb, chTable) = chTarget.split("\\.", 2)
  private val chLocalTable = s"${chTable}_local"

  private val appName = s"hive_to_clickhouse_replace:$hiveSource->$chTarget"

  private val conf = new SparkConf()
    .setAppName(appName)
    .setMaster("yarn")
    .setAll(SPARK_SETTINGS)

  private val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.session.timeZone", "Europe/Moscow")

  private def parseHostAndPort(hostRaw: String): (String, Int) = {
    val parts = hostRaw.split(":", 2)
    if (parts.length == 2) (parts(0), parts(1).toInt) else (hostRaw, 8123)
  }

  private def chAdminUrl(host: String, port: Int): String =
    s"jdbc:clickhouse://$host:$port/default?multiquery=true"

  private def chDbUrl(host: String, port: Int, db: String): String =
    s"jdbc:clickhouse://$host:$port/$db"

  private def withConnection[T](url: String)(f: Connection => T): T = {
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val props = new Properties()
    props.setProperty("user", chUser)
    props.setProperty("password", chPass)
    val conn = DriverManager.getConnection(url, props)
    try f(conn)
    finally conn.close()
  }

  // Must not be private: otherwise Scala may infer a public member type as private and fail compilation
  // with "private class Compression escapes its defining scope".
  case class Compression(zstd: Int, lz4hc: Int)

  private def compressionForRows(rows: Long): Compression = {
    // landing: keep moderate compression
    if (rows <= 100000000L) Compression(zstd = 1, lz4hc = 3)
    else if (rows <= 500000000L) Compression(zstd = 2, lz4hc = 4)
    else if (rows <= 1000000000L) Compression(zstd = 3, lz4hc = 4)
    else if (rows <= 2000000000L) Compression(zstd = 4, lz4hc = 5)
    else if (rows <= 5000000000L) Compression(zstd = 5, lz4hc = 6)
    else Compression(zstd = 6, lz4hc = 7)
  }

  private def firstOrderByIdentifier(orderBy: String): Option[String] = {
    // tries to extract first simple column name from orderBy list
    val first = orderBy.split(",", 2).headOption.map(_.trim).getOrElse("")
    val cleaned = first.stripPrefix("(").stripSuffix(")").trim
    val ident = cleaned
      .replaceAll("`", "")
      .trim

    if (ident.matches("[A-Za-z_][A-Za-z0-9_]*")) Some(ident) else None
  }

  private def defaultShardingKey(orderBy: String): String =
    firstOrderByIdentifier(orderBy).map(c => s"cityHash64($c)").getOrElse("rand()")

  private def codecForClickHouseType(chType: String, comp: Compression): String = {
    val base = chType
      .replaceFirst("^Nullable\\(", "")
      .stripSuffix(")")
      .trim

    base match {
      case "String" | "UUID" => s"CODEC(LZ4HC(${comp.lz4hc}))"
      case t if t.startsWith("FixedString") => s"CODEC(LZ4HC(${comp.lz4hc}))"
      case t if t.startsWith("DateTime") || t == "Date" => s"CODEC(DoubleDelta, ZSTD(${comp.zstd}))"
      case t if t.startsWith("Int") || t.startsWith("UInt") || t.startsWith("Float") || t.startsWith("Decimal") =>
        s"CODEC(DoubleDelta, ZSTD(${comp.zstd}))"
      case _ => s"CODEC(ZSTD(${comp.zstd}))"
    }
  }

  private def sparkTypeToClickHouseType(dt: org.apache.spark.sql.types.DataType): String = {
    import org.apache.spark.sql.types._

    dt match {
      case StringType => "String"
      case BooleanType => "Int8" // will be normalized to 0/1
      case ByteType => "Int8"
      case ShortType => "Int16"
      case IntegerType => "Int32"
      case LongType => "Int64"
      case FloatType => "Float32"
      case DoubleType => "Float64"
      case d: DecimalType =>
        if (d.precision > 76) {
          // avoid type conflicts: Spark decimal precision can exceed ClickHouse limits
          "String"
        } else {
          val p = d.precision
          val s = math.min(d.scale, p)
          s"Decimal($p,$s)"
        }
      case DateType => "Date"
      case TimestampType => "DateTime64(3)"
      case BinaryType => "String" // normalize to base64 string
      case _: ArrayType => "String" // normalize to JSON
      case _: MapType => "String"   // normalize to JSON
      case _: StructType => "String" // normalize to JSON
      case _ => "String"
    }
  }

  private def normalizeForWrite(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.types._

    val cols = df.schema.fields.map { f =>
      val c = col(f.name)
      f.dataType match {
        case BooleanType => c.cast("byte").as(f.name)
        case BinaryType => base64(c).as(f.name)
        case _: ArrayType | _: MapType | _: StructType => to_json(c).as(f.name)
        case d: DecimalType if d.precision > 76 => c.cast("string").as(f.name)
        case _ => c.as(f.name)
      }
    }

    df.select(cols: _*)
  }

  private def buildCreateLocalDDL(df: DataFrame, comp: Compression): String = {
    val cols = df.schema.fields.map { f =>
      val baseType = sparkTypeToClickHouseType(f.dataType)
      val chType = if (f.nullable) s"Nullable($baseType)" else baseType
      val codec = codecForClickHouseType(chType, comp)
      s"    `${f.name}` $chType $codec"
    }.mkString(",\n")

    // replicated path mirrors example, based on target schema/table (without _local)
    val replicatedPath = s"/clickhouse/tables/{shard}/$chDb/$chTable"

    s"""CREATE TABLE IF NOT EXISTS $chDb.$chLocalTable
       |ON CLUSTER $chCluster
       |(
       |$cols
       |)
       |ENGINE = ReplicatedMergeTree(
       |    '$replicatedPath',
       |    '{replica}'
       |)
       |PARTITION BY $chPartitionBy
       |ORDER BY ($chOrderBy)
       |SETTINGS allow_nullable_key = 1
       |""".stripMargin
  }

  private def buildCreateDistributedDDL(shardingKeyExpr: String): String = {
    s"""CREATE TABLE IF NOT EXISTS $chDb.$chTable
       |ON CLUSTER $chCluster
       |AS $chDb.$chLocalTable
       |ENGINE = Distributed(
       |    $chCluster,
       |    $chDb,
       |    $chLocalTable,
       |    $shardingKeyExpr
       |)
       |""".stripMargin
  }

  private def execDDL(conn: Connection, sql: String): Unit = {
    val st = conn.createStatement()
    try {
      st.execute(sql)
    } finally {
      st.close()
    }
  }

  private def truncateTables(conn: Connection): Unit = {
    // truncating local is enough, but truncating distributed is harmless
    execDDL(conn, s"TRUNCATE TABLE $chDb.$chLocalTable ON CLUSTER $chCluster")
    execDDL(conn, s"TRUNCATE TABLE $chDb.$chTable ON CLUSTER $chCluster")
  }

  // ===== Load Hive -> DF =====
  var df = spark.table(hiveSource)
  hiveFilters.foreach { f =>
    df = df.filter(expr(f))
  }

  // materialize & count once (used for compression decision and later writes)
  df = df.persist(StorageLevel.DISK_ONLY)
  val rowCount = df.count()

  private val comp = compressionForRows(rowCount)
  val normalized = normalizeForWrite(df)

  val (chHost, chPort) = parseHostAndPort(chHostRaw)

  val shardingKeyExpr = chShardingKey.getOrElse(defaultShardingKey(chOrderBy))

  // ===== Create/clean ClickHouse =====
  withConnection(chAdminUrl(chHost, chPort)) { conn =>
    val ddlLocal = buildCreateLocalDDL(normalized, comp)
    val ddlDist = buildCreateDistributedDDL(shardingKeyExpr)

    execDDL(conn, ddlLocal)
    execDDL(conn, ddlDist)
    truncateTables(conn)
  }

  // ===== Write data (replace already ensured by truncate) =====
  def writeChunk(chunk: DataFrame): Unit = {
    chunk.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", chDbUrl(chHost, chPort, chDb))
      .option("dbtable", chTable)
      .option("user", chUser)
      .option("password", chPass)
      .option("batchsize", jdbcBatchSize)
      .option("isolationLevel", "NONE")
      .mode("append")
      .save()
  }

  hiveDateColumnOpt match {
    case None =>
      writeChunk(normalized)

    case Some(dateColName) =>
      val dateCol = col(dateColName)
      val yearCol = coalesce(
        year(to_timestamp(dateCol)),
        year(to_date(dateCol))
      ).as("__year")

      val years = normalized
        .select(yearCol)
        .distinct()
        .collect()
        .map(r => if (r.isNullAt(0)) -1 else r.getInt(0))
        .distinct
        .sorted

      years.foreach { y =>
        val chunk = if (y == -1) {
          normalized.filter(coalesce(year(to_timestamp(dateCol)), year(to_date(dateCol))).isNull)
        } else {
          normalized.filter(coalesce(year(to_timestamp(dateCol)), year(to_date(dateCol))) === lit(y))
        }
        writeChunk(chunk)
      }
  }

  df.unpersist(blocking = false)
  spark.stop()
}
