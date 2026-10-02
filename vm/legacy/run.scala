// Loaded by Spark 1.3 spark-shell -i. Environment paths are set by run_site.py.
object LegacySiteJob {
  def run(sc: org.apache.spark.SparkContext): Unit = {
    require(sc.version.startsWith("1.3."), "This runner targets Spark 1.3.x")
    import org.apache.spark.sql.{SQLContext, Row}
    import org.apache.spark.sql.hive.HiveContext
    import org.apache.spark.sql.types._
    import org.apache.hadoop.fs.Path
    val sql = new HiveContext(sc)
    sql.setConf("spark.sql.shuffle.partitions", "2")
    sql.setConf("spark.sql.parquet.compression.codec", "snappy")
    val input = sys.env("VM_INPUT")
    val output = sys.env("VM_OUTPUT")
    val props = new java.util.Properties()
    val propFile = new java.io.FileInputStream(sys.env("VM_PROPERTIES"))
    try { props.load(propFile) } finally { propFile.close() }
    val site = props.getProperty("site")
    require(site.matches("[a-z0-9][a-z0-9.-]*"), "Invalid site")
    val root = new Path(output)
    val fs = root.getFileSystem(sc.hadoopConfiguration)
    require(!fs.exists(root), "Output already exists: " + output)
    require(fs.mkdirs(root), "Cannot create output")
    def schema(fields: (String, DataType)*) = StructType(fields.map { case (n,t) => StructField(n,t,true) })
    val schemas = Map(
      "posts" -> schema("id"->LongType,"post_type_id"->IntegerType,"creation_date"->LongType,"parent_id"->LongType,"month"->IntegerType),
      "users" -> schema("id"->LongType,"creation_date"->LongType,"month"->IntegerType),
      "votes" -> schema("id"->LongType,"vote_type_id"->IntegerType,"creation_date"->LongType,"month"->IntegerType))
    def loadTSV(path: String, s: StructType) = {
      val rows = sc.textFile(path, 1).map { line =>
        val values = line.split("\t", -1)
        require(values.length == s.fields.length, "TSV width mismatch")
        Row.fromSeq(values.zip(s.fields).map { case (v,f) =>
          if (v == "\\N") null else f.dataType match {
            case LongType => v.toLong
            case IntegerType => v.toInt
            case BooleanType => require(v == "true" || v == "false"); v.toBoolean
            case StringType => v
            case other => throw new IllegalArgumentException(other.toString)
          }
        }.toSeq)
      }
      sql.createDataFrame(rows, s)
    }
    for (table <- Seq("posts","users","votes")) {
      val years = props.getProperty(table + ".years").split(",").filter(_.nonEmpty).map(_.toInt)
      val fullSchema = StructType(schemas(table).fields ++ Array(StructField("site",StringType,true),StructField("year",IntegerType,true)))
      var combined = sql.createDataFrame(sc.emptyRDD[Row], fullSchema)
      var total = 0L
      for (year <- years) {
        val df = loadTSV(input + "/" + table + "/year=" + year + "/*.tsv", schemas(table))
        val count = df.count()
        val path = output + "/raw/" + table + "/site=" + site + "/year=" + year
        df.saveAsParquetFile(path)
        val saved = sql.parquetFile(path)
        require(saved.count() == count, "Parquet count mismatch")
        require(saved.schema.fields.map(f => (f.name,f.dataType)).toSeq == schemas(table).fields.map(f => (f.name,f.dataType)).toSeq, "Parquet schema mismatch")
        val partitionView = "partition_" + table + "_" + year
        saved.registerTempTable(partitionView)
        val names = schemas(table).fieldNames.mkString(",")
        combined = combined.unionAll(sql.sql("SELECT " + names + ", '" + site + "' AS site, CAST(" + year + " AS INT) AS year FROM " + partitionView))
        total += count
      }
      require(total == props.getProperty(table + ".rows").toLong, "Input row mismatch")
      combined.registerTempTable(table)
      require(sql.sql("SELECT id FROM " + table + " GROUP BY site,id HAVING COUNT(*) > 1").count() == 0, "Duplicate IDs")
      require(sql.sql("SELECT id FROM " + table + " WHERE id IS NULL OR creation_date IS NULL OR month IS NULL OR month < 1 OR month > 12").count() == 0, "Invalid required values")
    }
    val cs = schema("site"->StringType,"domain_group"->StringType,"is_meta"->BooleanType,"parent_site"->StringType,
      "year"->IntegerType,"month_number"->IntegerType,"month_key"->StringType,"complete_month"->BooleanType,"answer_mature"->BooleanType)
    val calendar = loadTSV(input + "/calendar.tsv",cs)
    require(calendar.count() == 63, "Calendar length")
    calendar.registerTempTable("analysis_calendar")
    val source = scala.io.Source.fromFile(sys.env("VM_SQL"), "UTF-8")
    val statements = try { source.mkString } finally { source.close() }
    for (block <- statements.split("-- @view ").filter(_.trim.nonEmpty)) {
      val pos = block.indexOf('\n')
      val name = block.substring(0,pos).trim
      sql.sql(block.substring(pos+1).trim).registerTempTable(name)
    }
    val audit = sql.sql("SELECT * FROM monthly_audit ORDER BY month")
    require(audit.count() == 63, "Monthly row count")
    audit.toJSON.coalesce(1).saveAsTextFile(output + "/audit")
    sql.sql("SELECT site,month,domain_group,questions_count,new_users_count,answered_rate,downvote_ratio FROM monthly_audit")
      .coalesce(1).saveAsParquetFile(output + "/serving")
    val marker = fs.create(new Path(output + "/_VM_SUCCESS"), false)
    try { marker.write((site + "\n" + props.getProperty("freeze_sha256") + "\n").getBytes("UTF-8")) }
    finally { marker.close() }
  }
}
try { LegacySiteJob.run(sc); sc.stop(); System.exit(0) } catch { case e: Throwable => e.printStackTrace(); sc.stop(); System.exit(1) }
