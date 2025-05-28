root@010015be8ea1:~# /opt/spark/bin/spark-submit /mnt/upload/Docker_Q1_RDD_API.py
25/05/27 20:31:52 INFO SparkContext: Running Spark version 3.5.5
25/05/27 20:31:52 INFO SparkContext: OS info Linux, 5.15.167.4-microsoft-standard-WSL2, amd64
25/05/27 20:31:52 INFO SparkContext: Java version 11.0.26
25/05/27 20:31:52 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/05/27 20:31:52 INFO ResourceUtils: ==============================================================
25/05/27 20:31:52 INFO ResourceUtils: No custom resources configured for spark.driver.
25/05/27 20:31:52 INFO ResourceUtils: ==============================================================
25/05/27 20:31:52 INFO SparkContext: Submitted application: Q1_Local
25/05/27 20:31:52 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/05/27 20:31:52 INFO ResourceProfile: Limiting resource is cpu
25/05/27 20:31:52 INFO ResourceProfileManager: Added ResourceProfile id: 0
25/05/27 20:31:52 INFO SecurityManager: Changing view acls to: root
25/05/27 20:31:52 INFO SecurityManager: Changing modify acls to: root
25/05/27 20:31:52 INFO SecurityManager: Changing view acls groups to:
25/05/27 20:31:52 INFO SecurityManager: Changing modify acls groups to:
25/05/27 20:31:52 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: root; groups with view permissions: EMPTY; users with modify permissions: root; groups with modify permissions: EMPTY
25/05/27 20:31:52 INFO Utils: Successfully started service 'sparkDriver' on port 46733.
25/05/27 20:31:52 INFO SparkEnv: Registering MapOutputTracker
25/05/27 20:31:52 INFO SparkEnv: Registering BlockManagerMaster
25/05/27 20:31:52 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
25/05/27 20:31:52 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
25/05/27 20:31:52 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/05/27 20:31:52 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-4c706eb1-59b4-4573-aafe-2b1ad1f6f003
25/05/27 20:31:52 INFO MemoryStore: MemoryStore started with capacity 434.4 MiB
25/05/27 20:31:52 INFO SparkEnv: Registering OutputCommitCoordinator
25/05/27 20:31:52 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
25/05/27 20:31:52 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/05/27 20:31:52 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://spark-master:7077...
25/05/27 20:31:52 INFO TransportClientFactory: Successfully created connection to spark-master/172.18.0.6:7077 after 19 ms (0 ms spent in bootstraps)
25/05/27 20:31:52 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20250527203152-0002
25/05/27 20:31:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20250527203152-0002/0 on worker-20250527184522-172.18.0.8-38721 (172.18.0.8:38721) with 2 core(s)
25/05/27 20:31:52 INFO StandaloneSchedulerBackend: Granted executor ID app-20250527203152-0002/0 on hostPort 172.18.0.8:38721 with 2 core(s), 1024.0 MiB RAM
25/05/27 20:31:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20250527203152-0002/1 on worker-20250527184522-172.18.0.9-42027 (172.18.0.9:42027) with 2 core(s)
25/05/27 20:31:52 INFO StandaloneSchedulerBackend: Granted executor ID app-20250527203152-0002/1 on hostPort 172.18.0.9:42027 with 2 core(s), 1024.0 MiB RAM
25/05/27 20:31:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20250527203152-0002/2 on worker-20250527184522-172.18.0.10-42347 (172.18.0.10:42347) with 2 core(s)
25/05/27 20:31:52 INFO StandaloneSchedulerBackend: Granted executor ID app-20250527203152-0002/2 on hostPort 172.18.0.10:42347 with 2 core(s), 1024.0 MiB RAM
25/05/27 20:31:52 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20250527203152-0002/3 on worker-20250527184522-172.18.0.7-38731 (172.18.0.7:38731) with 2 core(s)
25/05/27 20:31:52 INFO StandaloneSchedulerBackend: Granted executor ID app-20250527203152-0002/3 on hostPort 172.18.0.7:38731 with 2 core(s), 1024.0 MiB RAM
25/05/27 20:31:52 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 44843.
25/05/27 20:31:52 INFO NettyBlockTransferService: Server created on 010015be8ea1:44843
25/05/27 20:31:52 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
25/05/27 20:31:52 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 010015be8ea1, 44843, None)
25/05/27 20:31:52 INFO BlockManagerMasterEndpoint: Registering block manager 010015be8ea1:44843 with 434.4 MiB RAM, BlockManagerId(driver, 010015be8ea1, 44843, None)
25/05/27 20:31:52 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 010015be8ea1, 44843, None)
25/05/27 20:31:52 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 010015be8ea1, 44843, None)
25/05/27 20:31:53 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20250527203152-0002/0 is now RUNNING
25/05/27 20:31:53 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20250527203152-0002/1 is now RUNNING
25/05/27 20:31:53 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20250527203152-0002/2 is now RUNNING
25/05/27 20:31:53 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20250527203152-0002/3 is now RUNNING
25/05/27 20:31:54 INFO SingleEventLogFileWriter: Logging events to hdfs://namenode:9000/logs/app-20250527203152-0002.inprogress
25/05/27 20:31:54 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
25/05/27 20:31:54 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
25/05/27 20:31:54 INFO SharedState: Warehouse path is 'file:/root/spark-warehouse'.
25/05/27 20:31:55 INFO StandaloneSchedulerBackend$StandaloneDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.7:43446) with ID 3,  ResourceProfileId 0
25/05/27 20:31:55 INFO StandaloneSchedulerBackend$StandaloneDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.10:42636) with ID 2,  ResourceProfileId 0
25/05/27 20:31:55 INFO StandaloneSchedulerBackend$StandaloneDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.9:56726) with ID 1,  ResourceProfileId 0
25/05/27 20:31:55 INFO StandaloneSchedulerBackend$StandaloneDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.8:45796) with ID 0,  ResourceProfileId 0
25/05/27 20:31:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.7:32771 with 434.4 MiB RAM, BlockManagerId(3, 172.18.0.7, 32771, None)
25/05/27 20:31:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.10:38341 with 434.4 MiB RAM, BlockManagerId(2, 172.18.0.10, 38341, None)
25/05/27 20:31:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.9:37195 with 434.4 MiB RAM, BlockManagerId(1, 172.18.0.9, 37195, None)
25/05/27 20:31:55 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.8:37395 with 434.4 MiB RAM, BlockManagerId(0, 172.18.0.8, 37395, None)
25/05/27 20:31:55 WARN FileStreamSink: Assume no metadata directory. Error while looking for metadata directory in the path: hdfs://172.18.0.2:9000/data/yellow_tripdata_2015.csv.
java.lang.IllegalArgumentException: java.net.URISyntaxException: Illegal character in hostname at index 34: hdfs://namenode.01-lab1-spark-hdfs_hadoop-net:9000
        at org.apache.hadoop.net.NetUtils.getCanonicalUri(NetUtils.java:327)
        at org.apache.hadoop.hdfs.DistributedFileSystem.canonicalizeUri(DistributedFileSystem.java:2029)
        at org.apache.hadoop.fs.FileSystem.getCanonicalUri(FileSystem.java:381)
        at org.apache.hadoop.fs.FileSystem.checkPath(FileSystem.java:781)
        at org.apache.hadoop.hdfs.DistributedFileSystem.getPathName(DistributedFileSystem.java:254)
        at org.apache.hadoop.hdfs.DistributedFileSystem$29.doCall(DistributedFileSystem.java:1753)
        at org.apache.hadoop.hdfs.DistributedFileSystem$29.doCall(DistributedFileSystem.java:1750)
        at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81)
        at org.apache.hadoop.hdfs.DistributedFileSystem.getFileStatus(DistributedFileSystem.java:1765)
        at org.apache.hadoop.fs.FileSystem.isDirectory(FileSystem.java:1777)
        at org.apache.spark.sql.execution.streaming.FileStreamSink$.hasMetadata(FileStreamSink.scala:54)
        at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:366)
        at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:229)
        at org.apache.spark.sql.DataFrameReader.$anonfun$load$2(DataFrameReader.scala:211)
        at scala.Option.getOrElse(Option.scala:189)
        at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:211)
        at org.apache.spark.sql.DataFrameReader.csv(DataFrameReader.scala:538)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
        at java.base/java.lang.reflect.Method.invoke(Unknown Source)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
        at py4j.Gateway.invoke(Gateway.java:282)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
        at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
        at java.base/java.lang.Thread.run(Unknown Source)
Caused by: java.net.URISyntaxException: Illegal character in hostname at index 34: hdfs://namenode.01-lab1-spark-hdfs_hadoop-net:9000
        at java.base/java.net.URI$Parser.fail(Unknown Source)
        at java.base/java.net.URI$Parser.parseHostname(Unknown Source)
        at java.base/java.net.URI$Parser.parseServer(Unknown Source)
        at java.base/java.net.URI$Parser.parseAuthority(Unknown Source)
        at java.base/java.net.URI$Parser.parseHierarchical(Unknown Source)
        at java.base/java.net.URI$Parser.parse(Unknown Source)
        at java.base/java.net.URI.<init>(Unknown Source)
        at org.apache.hadoop.net.NetUtils.getCanonicalUri(NetUtils.java:325)
        ... 28 more
Traceback (most recent call last):
  File "/mnt/upload/Docker_Q1_RDD_API.py", line 7, in <module>
    df = spark.read.option("header", "true").option("inferSchema", "true") \
  File "/opt/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 740, in csv
  File "/opt/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/opt/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 185, in deco
pyspark.errors.exceptions.captured.IllegalArgumentException: java.net.URISyntaxException: Illegal character in hostname at index 34: hdfs://namenode.01-lab1-spark-hdfs_hadoop-net:9000
25/05/27 20:31:56 INFO SparkContext: Invoking stop() from shutdown hook
25/05/27 20:31:56 INFO SparkContext: SparkContext is stopping with exitCode 0.
25/05/27 20:31:56 INFO SparkUI: Stopped Spark web UI at http://010015be8ea1:4040
25/05/27 20:31:56 INFO StandaloneSchedulerBackend: Shutting down all executors
25/05/27 20:31:56 INFO StandaloneSchedulerBackend$StandaloneDriverEndpoint: Asking each executor to shut down
25/05/27 20:31:56 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
25/05/27 20:31:56 INFO MemoryStore: MemoryStore cleared
25/05/27 20:31:56 INFO BlockManager: BlockManager stopped
25/05/27 20:31:56 INFO BlockManagerMaster: BlockManagerMaster stopped
25/05/27 20:31:56 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
25/05/27 20:31:56 INFO SparkContext: Successfully stopped SparkContext
25/05/27 20:31:56 INFO ShutdownHookManager: Shutdown hook called
25/05/27 20:31:56 INFO ShutdownHookManager: Deleting directory /tmp/spark-600b9e7c-4bb2-41e7-9418-867d3988a315/pyspark-e88ff1c2-573b-4942-9f30-893965f86587
25/05/27 20:31:56 INFO ShutdownHookManager: Deleting directory /tmp/spark-600b9e7c-4bb2-41e7-9418-867d3988a315
25/05/27 20:31:56 INFO ShutdownHookManager: Deleting directory /tmp/spark-15f9918f-fe45-46ca-a4cf-1696cf0b7d63
