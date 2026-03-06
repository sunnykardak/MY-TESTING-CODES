Managed Flink — HiveCatalog with Glue Metastore (Anishvaran's approach)
Attempted to implement the approach shared by Anishvaran using Flink's HiveCatalog with AWSGlueDataCatalogHiveClientFactory and hive.metastore.glue.catalogid set to the target account. This requires the aws-glue-datacatalog-hive3-client JAR.
Build failed because this JAR (com.amazonaws.glue:aws-glue-datacatalog-hive3-client) is not available in our Barclays Nexus repository. Also org.apache.hadoop:hadoop-common was not found. Searched Nexus — the Glue Hive client artifact does not appear to be present.
Result: Blocked — required dependency aws-glue-datacatalog-hive3-client not available in Nexus.
