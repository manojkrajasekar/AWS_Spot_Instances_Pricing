rom pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

northeast1 = sc.textFile("/user/training/ap-northeast-1.csv")
notheast2 = sc.textFile("/user/training/ap-northeast-2.csv")
apsouth1 = sc.textFile("/user/training/ap-south-1.csv")
apsoutheast1 = sc.textFile("/user/training/ap-southeast-1.csv")
apsoutheast2 = sc.textFile("/user/training/ap-southeast-2.csv")
cacentral1 = sc.textFile("/user/training/ca-central-1.csv")
cacentral2 = sc.textFile("/user/training/ca-centrl-2.csv")
euwest1 = sc.textFile("/user/training/eu-west-1.csv")
saeast1 = sc.textFile("/user/training/sa-east-1.csv")
useast1 = sc.textFile("/user/training/us-east-1.csv")
useast2 = sc.textFile("/user/training/us-east-2.csv")


northeast1 = lines.map(lambda l: l.split("\t"))
northeast1data = northeast11.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemanortheast = sqlContext.createDataFrame(northeast1data)
schemanortheast.registerTempTable("northeast1data_table")


northeast2 = lines.map(lambda l: l.split("\t"))
northeast2data = northeast2.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemanortheast2data = sqlContext.createDataFrame(northeast2data)
schemanortheast2data.registerTempTable("northeast2data_table")

apsouth1 = lines.map(lambda l: l.split("\t"))
apsouth1data = apsouth1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemaapsouth1data = sqlContext.createDataFrame(apsouth1data)
schemaapsouth1data.registerTempTable("apsouth1data_table")

apsoutheast1 = lines.map(lambda l: l.split("\t"))
apsoutheast1data = apsoutheast1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemaapsoutheast1data = sqlContext.createDataFrame(apsoutheast1data)
schemaapsoutheast1data.registerTempTable("apsoutheast1data_table")


apsoutheast2 = lines.map(lambda l: l.split("\t"))
apsoutheast2data = apsoutheast2.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemaapsoutheast2data = sqlContext.createDataFrame(apsoutheast2data)
schemaapsoutheast2data.registerTempTable("apsoutheast2data_table")

cacentral1 = lines.map(lambda l: l.split("\t"))
cacentral1data = cacentral1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemacacentral1data = sqlContext.createDataFrame(cacentral1data)
schemacacentral1data.registerTempTable("cacentral1data_table")

cacentral2 = lines.map(lambda l: l.split("\t"))
cacentral2data = cacentral2.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemacacentral2data = sqlContext.createDataFrame(cacentral2data)
schemacacentral2data.registerTempTable("cacentral2data_table")

euwest1 = lines.map(lambda l: l.split("\t"))
euwest1data = euwest1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemaeuwest1data = sqlContext.createDataFrame(euwest1data)
schemaeuwest1data.registerTempTable("euwest1data_table")

saeast1 = lines.map(lambda l: l.split("\t"))
saeast1data = saeast1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemasaeast1data = sqlContext.createDataFrame(saeast1data)
schemasaeast1data.registerTempTable("saeast1data_table")

useast1 = lines.map(lambda l: l.split("\t"))
useast1data = useast1.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemauseast1data = sqlContext.createDataFrame(useast1data)
schemauseast1data.registerTempTable("useast1data_table")

useast2 = lines.map(lambda l: l.split("\t"))
useast2data = useast2.map(lambda p: Row(instance_type=concat(y[1], y[2]), instance_price=y[4]))
schemauseast2data = sqlContext.createDataFrame(useast2data)
schemauseast2data.registerTempTable("useast2data_table")


val result = northeast1data_table.join(northeast2data_table, northeast1data_table("instance_type")===northeast2data_table("instance_type"))
                     .join(apsouth1data_table, northeast1data_table("instance_type")===apsouth1data_table("instance_type"))
                     .join(apsoutheast1data_table, apsoutheast1data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(apsoutheast2data_table, apsoutheast2data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(cacentral1data_table, cacentral1data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(cacentral2data_table, cacentral2data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(euwest1data_table, euwest1data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(saeast1data_table, saeast1data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(useast1data_table, useast1data_table("instance_type")===northeast1data_table("instance_type"))
                     .join(useast2data_table, useast2data_table("instance_type")===northeast1data_table("instance_type"))

aws_region_result = sqlContext.sql("SELECT region, MIN(avg_price) FROM ( SELECT AVG(insnce_price) AS avg_price FROM result GROUP BY instance_type").show()

