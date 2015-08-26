name := "spark-sql"

version := "1.0"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.10" % "1.3.0",
                            "org.apache.spark" % "spark-sql_2.10"  % "1.3.0",
                            "com.databricks"   % "spark-avro_2.10" % "1.0.0",
                            "org.apache.avro"  % "avro"            % "1.7.7",
                            "org.apache.avro"  % "avro-mapred"     % "1.7.7" )

    