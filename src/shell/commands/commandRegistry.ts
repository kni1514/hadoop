import { NameNode } from '../../hdfs/namenode/nameNode';
import { ResourceManager } from '../../yarn/resourcemanager/resourceManager';
import { MapReduceEngine } from '../../mapreduce/engine/mapReduceEngine';
import { parseHadoopCommand, ParsedCommand, shellSplit } from '../parser/lexerParser';
import { INodeFile } from '../../core/domain/types';
import { HadoopDB, ShellState } from '../../storage/hadoopDB';
import { PythonEngine, transpilePythonToJS } from '../../ecosystem/python/pythonEngine';
import { DfsShell, DfsContext, normalizeHdfsPath, humanSize } from '../../hdfs/shell/dfsShell';
import { wordCount, grep as grepJob, piEstimate, wordMean, sortLines, streamingShuffle, JobResult } from '../../mapreduce/jobs/exampleJobs';

interface FinishedJob {
  jobId: string;
  appId: string;
  name: string;
  user: string;
  queue: string;
  state: 'SUCCEEDED' | 'FAILED';
  startTime: number;
  finishTime: number;
  output: string;
}


const ABOUT_GUIDE_CONTENT = `================================================================================
           BROWSER-BASED HADOOP SIMULATOR & PRACTICE LABORATORY GUIDE
================================================================================
DEVELOPER     : Niranjan Kumar K
DEPLOYMENT URL: https://kni1514.github.io/hadoop/
ENVIRONMENT   : Apache Hadoop, PySpark v3.3.0, KSQL SQLite WASM, HDFS & YARN Engine

--------------------------------------------------------------------------------
1. ENTERPRISE ARCHITECTURE & ECOSYSTEM OVERVIEW
--------------------------------------------------------------------------------
- HDFS (Hadoop Distributed File System):
  Fault-tolerant, rack-aware distributed filesystem for storing datasets.
  Full support for hdfs dfs utilities: -mkdir, -put, -get, -cat, -ls, -rm,
  -chmod, -chown, -du, -df, -touchz, -checksum, -count, -find, block reports.

- YARN & MapReduce Engine:
  Distributed resource management framework with Capacity Scheduler.
  Supports MapReduce jobs (yarn jar wordcount.jar) & Hadoop Streaming.

- PySpark v3.3.0 Engine & REPL Shell (Python WebAssembly):
  High-class SparkSession & SparkContext integration running 100% offline.
  DataFrame API: createDataFrame, show(), printSchema(), select(), filter(),
  where(), withColumn(), withColumnRenamed(), drop(), groupBy(), count(), sum(),
  avg(), min(), max(), sort(), orderBy(), randomSplit(), createOrReplaceTempView(), spark.sql().
  DataFrame Readers: spark.read.csv(), spark.read.json().
  RDD API: sc.parallelize(), map(), filter(), flatMap(), mapPartitions(), reduce(),
  reduceByKey(), groupByKey(), sortByKey(), distinct(), union(), zip(), cartesian(),
  aggregate(), fold(), sum(), mean(), collect(), count(), take(), takeSample().
  ML API: pyspark.ml Pipeline/PipelineModel; feature preprocessing with
  VectorAssembler, StringIndexer, OneHotEncoder, Imputer, StandardScaler and
  Normalizer; LogisticRegression classification and LinearRegression regression;
  classification and regression evaluators. Runs locally in-memory in Pyodide.
  Interactive Shell: Type pyspark or spark to open >>> prompt.
  Python REPL: Type python or python3 for a persistent >>> prompt; exit or quit returns.
  Script Execution: spark-submit script.py, python script.py, or python -c "...".

- KSQL SQLite WebAssembly Engine:
  Interactive SQL engine (>> prompt) launched via hive, pig, or ksql.
  Pig-style LOAD and STORE/SAVE: load 'file.csv' into table, store table into 'out.csv',
  save database.db. Full IndexedDB persistence across reloads (hadoop-lab-db).

- Terminal Local Storage & Host I/O:
  get                   Import file from local computer OS into terminal directory.
  download <filename>   Export file from terminal directory to computer Downloads folder.
  vim <file>            In-terminal Vim text editor with -- INSERT -- and : commands (:wq to save).

- PWA Offline Capability:
  Chrome CacheStorage precaching for 100% offline usage anywhere.

--------------------------------------------------------------------------------
2. GETTING STARTED (START HADOOP DAEMONS FIRST):
--------------------------------------------------------------------------------
  start-dfs.sh                   Start HDFS daemons (NameNode & DataNodes)
  start-yarn.sh                  Start YARN daemons (ResourceManager & NodeManagers)
  start-all.sh                   Start all Hadoop daemons

--------------------------------------------------------------------------------
3. COMPLETE COMMAND REFERENCE:
--------------------------------------------------------------------------------
[NODE.JS RUNTIME & HADOOP STREAMING MAPREDUCE]
  node -v                       Display Node.js runtime version (v18.16.0)
  node script.js                Execute JavaScript file with full JS Function execution
  node -e "console.log(2+3)"    Evaluate JavaScript code inline
  hadoop jar hadoop-streaming.jar -mapper "node mapper.js" -reducer "node reducer.js" -input /in -output /out

[HADOOP HDFS STORAGE, ERASURE CODING & DISTCP]
  hdfs dfs -ls <path>           List HDFS directory contents
  hdfs dfs -find / -name "*.txt" Search files recursively across HDFS
  hdfs dfs -count /             Count directories, files, and bytes in HDFS
  hdfs fsck /file -files -blocks -locations  Inspect HDFS block IDs & DataNode locations
  hdfs dfsadmin -triggerBlockReport datanode1 Trigger Full Block Report to NameNode
  hdfs dfsadmin -metasave metasave.log Dump NameNode block state to log
  hdfs dfsadmin -refreshNodes   Refresh DataNode membership & Decommissioning list
  hdfs storagepolicies -isSatisfierRunning Check Storage Policy Satisfier daemon state
  hdfs ec -enablePolicy -policy RS-6-3-1024k  Enable Erasure Coding policy
  hdfs ec -disablePolicy -policy RS-6-3-1024k Disable Erasure Coding policy
  hdfs dfs -mkdir -p <path>     Create HDFS directory
  hdfs dfs -put <local> <hdfs>  Upload local file to HDFS
  hdfs dfs -get <hdfs> <local>  Download HDFS file to local Linux space
  hdfs dfs -cat <path>          Print HDFS file content
  hdfs dfs -checksum <path>     Compute file MD5 checksum
  hdfs dfs -stat "%F %u:%g"     Display file metadata
  hdfs dfs -cp <src> <dst>      Copy file within HDFS
  hdfs dfs -mv <src> <dst>      Move/rename file within HDFS
  hdfs dfs -chmod <mode> <path> Change file permissions (e.g. 755)
  hdfs dfs -chown <user> <path> Change file owner
  hdfs dfs -getfacl <path>      Inspect HDFS Access Control List (ACL)
  hdfs dfs -setfacl -m user:alice:rwx <path> Set granular ACL permissions
  hdfs dfs -expunge             Empty HDFS trash (.Trash)
  hadoop distcp hdfs://nn1/src hdfs://nn2/dst Distributed Copy across clusters
  hdfs dfsadmin -setQuota 1000 /Hacker  Set file count quota
  hdfs dfsadmin -setSpaceQuota 10g /Hacker Set storage space quota
  hdfs dfsadmin -saveNamespace  Save NameNode fsimage checkpoint
  hdfs dfsadmin -enterMaintenance datanode1 Graceful DataNode Maintenance window
  hdfs crypto -listZones        Inspect HDFS Encryption Zones
  hdfs crypto -createZone -keyName key1 -path /Hacker/secure  Create KMS Encryption Zone
  hdfs dfs -allowSnapshot <dir> Allow HDFS Snapshots
  hdfs dfs -createSnapshot <dir> <s1> Create HDFS Snapshot
  hdfs dfs -deleteSnapshot <dir> <s1> Delete HDFS Snapshot
  hdfs fsck /                   Run HDFS File System Check
  hdfs dfsadmin -report         Display DataNode cluster report
  hdfs haadmin -getServiceState nn1   Check NameNode HA State
  hdfs haadmin -failover nn1 nn2      Trigger NameNode HA Failover

[HADOOP YARN COMPUTE, TEZ & KYUUBI]
  hadoop jar wordcount.jar <in> <out>   Submit MapReduce Job
  tez-job.sh --submit dag.xml           Submit Apache Tez DAG job
  yarn application -list                List active YARN applications
  yarn application -kill <appId>        Kill running YARN application
  yarn applicationattempt -list <appId> List YARN application attempts
  yarn container -list <attemptId>      List YARN container allocations
  yarn logs -applicationId <id>        View MapReduce job container logs
  yarn queue -status root.default       Check YARN Capacity Queue Status
  yarn node -list                       List live NodeManagers & container capacity
  yarn node -list -states DECOMMISSIONED List decommissioned nodes
  yarn rmadmin -refreshNodes            Refresh NodeManager membership in YARN
  yarn rmadmin -refreshSuperUserGroupsConfiguration Refresh superuser ACLs
  kyuubi start                          Start Kyuubi Multi-Tenant SQL Gateway

[HIVE & LLAP IN-MEMORY QUERY ACCELERATOR]
  hive -e "SHOW TABLES"
  hive -e "ALTER TABLE sales ADD COLUMNS (country STRING)"
  hive -e "DROP TABLE sales"
  hive --service llap                   Start Hive LLAP In-Memory Cache Daemon
  impala-shell -q "SELECT * FROM sales"
  presto --execute "SELECT * FROM sales"
  pig -e "sales = LOAD '/Hacker/sales.csv' USING PigStorage(','); DUMP sales;"
  phoenix-sqlline -e "SELECT * FROM users"
  nifi.sh start
  zeppelin-daemon.sh start

[HBASE NOSQL SHELL]
  hbase shell
  hbase shell -c "list"
  hbase shell -c "create 'users', 'info'"
  hbase shell -c "put 'users', 'r1', 'info:name', 'Hacker'"
  hbase shell -c "get 'users', 'r1'"
  hbase shell -c "scan 'users'"

[SPARK CONSOLES, SQOOP, KAFKA, FLUME, OOZIE]
  spark-submit --class org.apache.spark.examples.WordCount spark-app.jar /Hacker/in /Hacker/out
  spark-shell                   Interactive Scala REPL for Spark
  pyspark                       Interactive Python REPL for Spark
  sqoop import --table users --target-dir /Hacker/sqoop_users
  kafka-topics.sh --create --topic logs --bootstrap-server localhost:9092
  flume-ng agent --conf-file flume.conf --name a1
  oozie job -config job.properties -run

[SECURITY, NETWORK DIAGNOSTICS & LINUX POWER TOOLS]
  ps -ef                        Display full process listing
  kill -9 <PID> / killall java  Terminate processes or daemons
  ifconfig / ip addr            Display virtual network interface IPs
  ping <node>                   Test network latency across DataNodes
  netstat -tuln                 Display open Hadoop listening ports (9000, 8088, 9870, 2181)
  ranger policy -list           Inspect Ranger authorization policies
  kinit Hacker@HADOOP.LOCAL     Acquire Kerberos authentication ticket
  klist                         Inspect active Kerberos tickets
  kdestroy                      Destroy Kerberos tickets
  ls, pwd, cd, mkdir, rmdir, echo, cat, touch, rm, whoami
  grep <pattern> <file>         Search text in local files
  sort <file> / uniq <file>     Sort lines & count unique lines
  wc -l <file>                  Count lines/words/chars
  head / tail <file>            Display start/end of file
  top                           Display Linux process monitor
  df -h                         Display Linux filesystem disk usage
  free -m                       Display system RAM memory stats
  uname -a                      Display system kernel architecture
  history                       Display command history
  vim <file> / vi <file>        Open interactive Vim text editor (:wq to save & exit)
  clear                         Clear terminal screen
================================================================================`;

const CORE_SITE_XML = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
        <description>NameNode URI for HDFS cluster default filesystem.</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-\${user.name}</value>
        <description>Base directory for temporary simulated storage files.</description>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
        <description>Buffer size used in sequence files (128 KB).</description>
    </property>
    <property>
        <name>hadoop.security.authentication</name>
        <value>simple</value>
        <description>Authentication mechanism: simple or kerberos.</description>
    </property>
    <property>
        <name>hadoop.security.authorization</name>
        <value>true</value>
        <description>Enable Service Level Authorization in Hadoop.</description>
    </property>
</configuration>`;

const HDFS_SITE_XML = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
        <description>Default HDFS block replication factor across DataNodes.</description>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
        <description>Default HDFS block size in bytes (128 MB).</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///var/hadoop/dfs/name</value>
        <description>Local filesystem path where NameNode stores fsimage &amp; edits log.</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///var/hadoop/dfs/data</value>
        <description>Local filesystem directory where DataNodes store blocks.</description>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>true</value>
        <description>Enable POSIX file permissions checking in HDFS.</description>
    </property>
    <property>
        <name>dfs.namenode.servicerpc-address</name>
        <value>localhost:8021</value>
        <description>NameNode Service RPC listener port.</description>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>localhost:9870</value>
        <description>NameNode Web UI HTTP address.</description>
    </property>
</configuration>`;

const MAPRED_SITE_XML = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
        <description>Execution framework set to YARN (instead of local/classic).</description>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
        <description>Class path for MapReduce applications.</description>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>1024</value>
        <description>Amount of memory allocated for each Map task container.</description>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>2048</value>
        <description>Amount of memory allocated for each Reduce task container.</description>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>localhost:10020</value>
        <description>MapReduce JobHistory Server IPC address.</description>
    </property>
</configuration>`;

const YARN_SITE_XML = `<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
        <description>Hostname of the YARN ResourceManager daemon.</description>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
        <description>Auxiliary service required for MapReduce shuffle phase.</description>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>8192</value>
        <description>Total physical RAM (in MB) allocated to containers on NodeManager.</description>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>8</value>
        <description>Total CPU virtual cores allocated to containers on NodeManager.</description>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
        <description>YARN Pluggable Scheduler class.</description>
    </property>
</configuration>`;

export class HadoopShellExecutor {
  private nameNode: NameNode;
  public resourceManager: ResourceManager;
  private mapReduceEngine: MapReduceEngine;
  private pythonEngine: PythonEngine;
  private db: HadoopDB;

  private isHDFSStarted: boolean = false;
  private isYARNStarted: boolean = false;
  private kerberosTicket: string | null = null;
  private activeNameNodeId: string = 'nn1';

  private storagePolicies: Map<string, string> = new Map();
  private ecPolicies: Map<string, string> = new Map();
  private cacheDirectives: Map<string, string> = new Map();
  private snapshots: Map<string, Set<string>> = new Map();
  private acls: Map<string, string[]> = new Map();
  private cryptoZones: Map<string, string> = new Map();
  private fileQuotas: Map<string, number> = new Map();
  private spaceQuotas: Map<string, string> = new Map();

  private historyList: string[] = [];
  private isNameNodeFormatted: boolean = false;

  private localDir: string = '/home/Hacker';
  private localFiles: Map<string, string> = new Map([
    ['/home/Hacker/about.txt', ABOUT_GUIDE_CONTENT],
    ['/etc/hadoop/core-site.xml', CORE_SITE_XML],
    ['/etc/hadoop/hdfs-site.xml', HDFS_SITE_XML],
    ['/etc/hadoop/mapred-site.xml', MAPRED_SITE_XML],
    ['/etc/hadoop/yarn-site.xml', YARN_SITE_XML],
    ['/home/Hacker/hadoop/etc/hadoop/core-site.xml', CORE_SITE_XML],
    ['/home/Hacker/hadoop/etc/hadoop/hdfs-site.xml', HDFS_SITE_XML],
    ['/home/Hacker/hadoop/etc/hadoop/mapred-site.xml', MAPRED_SITE_XML],
    ['/home/Hacker/hadoop/etc/hadoop/yarn-site.xml', YARN_SITE_XML]
  ]);
  private localDirs: Set<string> = new Set([
    '/',
    '/etc',
    '/etc/hadoop',
    '/home',
    '/home/Hacker',
    '/home/Hacker/hadoop',
    '/home/Hacker/hadoop/etc',
    '/home/Hacker/hadoop/etc/hadoop'
  ]);

  private dfs!: DfsShell;
  private lastExitCode: number = 0;
  private safeModeManual: boolean = false;
  private historyServerRunning: boolean = false;
  private finishedJobs: FinishedJob[] = [];
  private jobSequence: number = 0;
  private readonly clusterTimestamp: number = Date.now();

  private cachedStorageUsage: number = 0;
  private cachedStorageQuota: number = 0;

  constructor(nameNode: NameNode, resourceManager: ResourceManager, mapReduceEngine: MapReduceEngine, db: HadoopDB) {
    this.nameNode = nameNode;
    this.resourceManager = resourceManager;
    this.mapReduceEngine = mapReduceEngine;
    this.pythonEngine = new PythonEngine();
    this.db = db;
    this.dfs = new DfsShell(this.makeDfsContext());
    this.exposeHdfsToPython();
    this.startStorageRefresh();
  }

  /** Lets Python/PySpark code read and write HDFS paths (sc.textFile('hdfs:///x'), spark.read.csv('/x')). */
  private exposeHdfsToPython(): void {
    if (typeof window === 'undefined') return;
    const w = window as any;
    w.__hadoop_hdfs_read = (p: string) => {
      const path = normalizeHdfsPath(String(p));
      const node = this.nameNode.getNamespace().resolvePath(path);
      if (!node) return null;
      if (node.type === 'FILE') return this.nameNode.readFileContent(path) ?? '';
      return this.nameNode
        .listFilesUnder(path)
        .filter((f) => !f.name.startsWith('_') && !f.name.startsWith('.'))
        .sort((a, b) => a.path.localeCompare(b.path))
        .map((f) => this.nameNode.readFileContent(f.path) ?? '')
        .join('');
    };
    w.__hadoop_hdfs_exists = (p: string) => !!this.nameNode.getNamespace().resolvePath(normalizeHdfsPath(String(p)));
    w.__hadoop_hdfs_write = (p: string, content: string) => {
      const path = normalizeHdfsPath(String(p));
      const file = this.nameNode.createAndWriteFile(path, String(content), this.confInt('dfs.replication', 3), this.confInt('dfs.blocksize', 134217728));
      file.owner = 'user';
      this.db.saveHdfsFileContent(path, String(content));
      return true;
    };
    w.__hadoop_hdfs_delete = (p: string) => {
      const path = normalizeHdfsPath(String(p));
      this.nameNode.listFilesUnder(path).forEach((f) => this.db.deleteHdfsFileContent(f.path));
      this.db.deleteHdfsFileContent(path);
      try {
        this.nameNode.deletePath(path, true);
      } catch {
        // Nothing to delete.
      }
      this.scheduleMetaSave();
      return true;
    };
    w.__hadoop_cli = (command: string) => this.execute(String(command));
    w.__hadoop_hdfs_mkdir = (p: string) => {
      const path = normalizeHdfsPath(String(p));
      this.nameNode.getNamespace().mkdir(path, true);
      this.db.saveHdfsFileContent(path, '__HADOOP_DIR__');
      return true;
    };
  }

  private makeDfsContext(): DfsContext {
    return {
      nameNode: this.nameNode,
      user: 'user',
      readLocal: (p) => this.localFiles.get(this.resolveLocalPath(p)),
      writeLocal: (p, content) => {
        const resolved = this.resolveLocalPath(p);
        this.ensureLocalParents(resolved);
        this.localFiles.set(resolved, content);
        this.db.saveLocalFile(resolved, content);
      },
      deleteLocal: (p) => {
        const resolved = this.resolveLocalPath(p);
        if (!this.localFiles.has(resolved)) return false;
        this.localFiles.delete(resolved);
        this.db.deleteLocalFile(resolved);
        return true;
      },
      isLocalDir: (p) => this.localDirs.has(this.resolveLocalPath(p)),
      makeLocalDir: (p) => {
        const resolved = this.resolveLocalPath(p);
        this.ensureLocalParents(resolved + '/x');
      },
      listLocalFilesUnder: (p) => {
        const root = this.resolveLocalPath(p).replace(/\/+$/, '');
        return [...this.localFiles.keys()].filter((k) => k.startsWith(root + '/')).sort();
      },
      resolveLocal: (p) => this.resolveLocalPath(p),
      persistFile: (p, content) => {
        this.db.saveHdfsFileContent(p, content);
        this.scheduleMetaSave();
      },
      persistDir: (p) => {
        this.db.saveHdfsFileContent(p, '__HADOOP_DIR__');
        this.scheduleMetaSave();
      },
      forget: (p) => {
        this.db.deleteHdfsFileContent(p);
        this.scheduleMetaSave();
      },
      persistMeta: () => this.scheduleMetaSave(),
      conf: (key, fallback) => this.getConf(key, fallback),
      inSafeMode: () => this.safeModeManual,
      setExitCode: (code) => {
        this.lastExitCode = code;
      }
    };
  }

  private ensureLocalParents(filePath: string): void {
    const parts = filePath.split('/').filter(Boolean);
    let current = '';
    for (let i = 0; i < parts.length - 1; i++) {
      current += '/' + parts[i];
      if (!this.localDirs.has(current)) {
        this.localDirs.add(current);
        this.db.saveLocalDir(current);
      }
    }
  }

  /** Read a Hadoop property from the (editable) XML files in ~/hadoop/etc/hadoop, then defaults. */
  public getConf(key: string, fallback: string): string {
    const files = ['core-site.xml', 'hdfs-site.xml', 'mapred-site.xml', 'yarn-site.xml'];
    for (const f of files) {
      const xml = this.localFiles.get(`/home/Hacker/hadoop/etc/hadoop/${f}`) ?? this.localFiles.get(`/etc/hadoop/${f}`);
      if (!xml) continue;
      const re = /<property>\s*<name>\s*([^<]+?)\s*<\/name>\s*<value>\s*([^<]*?)\s*<\/value>/g;
      let m: RegExpExecArray | null;
      while ((m = re.exec(xml)) !== null) {
        if (m[1] === key) return m[2];
      }
    }
    const defaults: Record<string, string> = {
      'fs.defaultFS': 'hdfs://localhost:9000',
      'dfs.replication': '3',
      'dfs.blocksize': '134217728',
      'dfs.namenode.name.dir': 'file:///home/Hacker/hadoop/data/dfs/name',
      'dfs.datanode.data.dir': 'file:///home/Hacker/hadoop/data/dfs/data',
      'dfs.namenode.http-address': '0.0.0.0:9870',
      'dfs.permissions.enabled': 'true',
      'mapreduce.framework.name': 'yarn',
      'yarn.resourcemanager.address': 'localhost:8032',
      'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
      'hadoop.tmp.dir': '/tmp/hadoop-Hacker',
      'fs.trash.interval': '0'
    };
    return defaults[key] ?? fallback;
  }

  private confInt(key: string, fallback: number): number {
    const v = parseInt(this.getConf(key, String(fallback)), 10);
    return Number.isFinite(v) && v > 0 ? v : fallback;
  }

  private metaTimer: ReturnType<typeof setTimeout> | null = null;

  /** Save permissions/replication/owner/time of every HDFS path (debounced). */
  private scheduleMetaSave(): void {
    if (this.metaTimer) clearTimeout(this.metaTimer);
    this.metaTimer = setTimeout(() => {
      this.metaTimer = null;
      const meta: Record<string, unknown> = {};
      const walk = (n: any) => {
        meta[n.path] = n.type === 'FILE'
          ? { t: 'f', r: n.replicationFactor, p: n.permissions, o: n.owner, g: n.group, m: n.modifiedAt, c: n.createdAt }
          : { t: 'd', p: n.permissions, o: n.owner, g: n.group, m: n.modifiedAt, c: n.createdAt };
        if (n.type === 'DIRECTORY') n.children.forEach((c: any) => walk(c));
      };
      walk(this.nameNode.getNamespace().getRoot());
      meta.__safemode = this.safeModeManual;
      this.db.saveHdfsMeta(meta);
    }, 250);
  }

  private applyHdfsMeta(meta: Record<string, any>): void {
    Object.entries(meta).forEach(([path, m]) => {
      if (path === '__safemode') {
        this.safeModeManual = !!m;
        return;
      }
      if (!m || typeof m !== 'object') return;
      let node = this.nameNode.getNamespace().resolvePath(path);
      if (!node && m.t === 'd') {
        try {
          node = this.nameNode.getNamespace().mkdir(path, true);
        } catch {
          return;
        }
      }
      if (!node) return;
      if (m.p) node.permissions = m.p;
      if (m.o) node.owner = m.o;
      if (m.g) node.group = m.g;
      if (m.m) node.modifiedAt = m.m;
      if (m.c) node.createdAt = m.c;
      if (node.type === 'FILE' && m.r && m.r !== node.replicationFactor) {
        try {
          this.nameNode.setReplication(path, m.r);
        } catch {
          // Keep the default replication when the stored value cannot be applied.
        }
      }
    });
  }

  private startStorageRefresh(): void {
    const refresh = () => {
      this.db.getStorageEstimate().then(({ usageBytes, quotaBytes }) => {
        this.cachedStorageUsage = usageBytes;
        this.cachedStorageQuota = quotaBytes;
      });
    };
    refresh();
    setInterval(refresh, 30000);
  }

  public async executePySparkRepl(line: string): Promise<string> {
    const res = await this.pythonEngine.runPySparkReplLine(
      line,
      this.localFiles,
      (path, content) => this.saveLocalFileContent(path, content)
    );
    if (res.stderr) {
      return res.stdout ? `${res.stdout}\n${res.stderr}` : res.stderr;
    }
    return res.stdout;
  }

  public async executePythonRepl(line: string): Promise<string> {
    const res = await this.pythonEngine.runPythonReplLine(
      line,
      this.localFiles,
      (path, content) => this.saveLocalFileContent(path, content)
    );
    if (res.stderr) return res.stdout ? `${res.stdout}\n${res.stderr}` : res.stderr;
    return res.stdout;
  }

  public async loadFromDB(): Promise<void> {
    await this.db.waitReady();

    const savedFiles = await this.db.loadAllLocalFiles();
    if (savedFiles.size > 0) {
      this.localFiles = savedFiles;
    }

    // Clean XML files from home directory /home/Hacker/
    const homeXmls = [
      '/home/Hacker/core-site.xml',
      '/home/Hacker/hdfs-site.xml',
      '/home/Hacker/mapred-site.xml',
      '/home/Hacker/yarn-site.xml'
    ];
    for (const xmlPath of homeXmls) {
      if (this.localFiles.has(xmlPath)) {
        this.localFiles.delete(xmlPath);
        await this.db.deleteLocalFile(xmlPath);
      }
    }

    const defaultConfigs: [string, string][] = [
      ['/home/Hacker/about.txt', ABOUT_GUIDE_CONTENT],
      ['/etc/hadoop/core-site.xml', CORE_SITE_XML],
      ['/etc/hadoop/hdfs-site.xml', HDFS_SITE_XML],
      ['/etc/hadoop/mapred-site.xml', MAPRED_SITE_XML],
      ['/etc/hadoop/yarn-site.xml', YARN_SITE_XML],
      ['/home/Hacker/hadoop/etc/hadoop/core-site.xml', CORE_SITE_XML],
      ['/home/Hacker/hadoop/etc/hadoop/hdfs-site.xml', HDFS_SITE_XML],
      ['/home/Hacker/hadoop/etc/hadoop/mapred-site.xml', MAPRED_SITE_XML],
      ['/home/Hacker/hadoop/etc/hadoop/yarn-site.xml', YARN_SITE_XML]
    ];

    for (const [pathKey, defaultContent] of defaultConfigs) {
      if (!this.localFiles.has(pathKey)) {
        this.localFiles.set(pathKey, defaultContent);
      }
    }

    const savedDirs = await this.db.loadAllLocalDirs();
    if (savedDirs.size > 0) {
      this.localDirs = savedDirs;
    }

    const defaultDirectories = [
      '/',
      '/etc',
      '/etc/hadoop',
      '/home',
      '/home/Hacker',
      '/home/Hacker/hadoop',
      '/home/Hacker/hadoop/etc',
      '/home/Hacker/hadoop/etc/hadoop'
    ];
    for (const dirKey of defaultDirectories) {
      this.localDirs.add(dirKey);
    }

    const savedHistory = await this.db.loadCommandHistory();
    if (savedHistory.length > 0) {
      this.historyList = savedHistory;
    }

    const savedState = await this.db.loadShellState();
    if (savedState) {
      this.isHDFSStarted = savedState.isHDFSStarted;
      this.isYARNStarted = savedState.isYARNStarted;
      this.isNameNodeFormatted = savedState.isNameNodeFormatted ?? false;
      this.localDir = savedState.localDir;
      this.kerberosTicket = savedState.kerberosTicket;
      this.activeNameNodeId = savedState.activeNameNodeId;
    }

    const savedHdfsFiles = await this.db.loadAllHdfsFileContents();
    if (savedHdfsFiles.size > 0) {
      savedHdfsFiles.forEach((content, hdfsPath) => {
        try {
          if (content === '__HADOOP_DIR__') {
            this.nameNode.getNamespace().mkdir(hdfsPath, true);
          } else {
            this.nameNode.createAndWriteFile(hdfsPath, content);
          }
        } catch {
        }
      });
    }

    const meta = await this.db.loadHdfsMeta();
    if (meta) this.applyHdfsMeta(meta);
  }



  private persistShellState(): void {
    const state: ShellState = {
      isHDFSStarted: this.isHDFSStarted,
      isYARNStarted: this.isYARNStarted,
      isNameNodeFormatted: this.isNameNodeFormatted,
      localDir: this.localDir,
      kerberosTicket: this.kerberosTicket,
      activeNameNodeId: this.activeNameNodeId
    };
    this.db.saveShellState(state);
  }



  public getWorkingDirDisplay(): string {
    if (this.localDir === '/home/Hacker') return '~';
    if (this.localDir.startsWith('/home/Hacker/')) {
      return '~/' + this.localDir.substring('/home/Hacker/'.length);
    }
    return this.localDir;
  }

  public getLocalFileContent(pathStr: string): string | undefined {
    const resolved = this.resolveLocalPath(pathStr);
    return this.localFiles.get(resolved);
  }

  public saveLocalFileContent(pathStr: string, content: string): void {
    const resolved = this.resolveLocalPath(pathStr);
    this.localFiles.set(resolved, content);
    this.db.saveLocalFile(resolved, content);
  }

  public execute(commandLine: string): string {
    const trimmed = commandLine.trim();
    if (trimmed.startsWith('#')) {
      return '';
    }

    if (trimmed) {
      this.historyList.push(trimmed);
      // Keep history bounded so long sessions stay fast to save and load.
      if (this.historyList.length > 1000) this.historyList.splice(0, this.historyList.length - 1000);
      this.db.saveCommandHistory(this.historyList);
    }

    if (/^echo\s+["']?\$\?["']?\s*$/.test(trimmed)) {
      return String(this.lastExitCode);
    }
    this.lastExitCode = 0;

    const daemonMatch = trimmed.match(/^(?:hdfs|yarn|mapred)\s+--daemon\s+(\S+)\s+(\S+)/i)
      || trimmed.match(/^(?:hadoop-daemon\.sh|yarn-daemon\.sh|hdfs-daemon\.sh|mr-jobhistory-daemon\.sh)\s+(?:--config\s+\S+\s+)?(\S+)\s+(\S+)/i);
    if (daemonMatch) {
      return this.runDaemonCommand(daemonMatch[1].toLowerCase(), daemonMatch[2]);
    }
    if (/^(hadoop|hdfs|yarn|mapred)\s+(version|-version|--version)\s*$/i.test(trimmed)) {
      return this.hadoopVersionText();
    }
    if (/^yarn\s+jar\s/i.test(trimmed)) {
      if (!this.isYARNStarted) return `Unable to connect to ResourceManager at localhost:8032. Run 'start-yarn.sh' first.`;
      return this.runJar(parseHadoopCommand(trimmed.replace(/^yarn/i, 'hadoop')).positionalArgs, trimmed);
    }

    const cmd = parseHadoopCommand(commandLine);
    if (cmd.utility === 'comment') {
      return '';
    }

    
    if (trimmed.startsWith('hive --service llap')) {
      return `[Hive LLAP Engine] Initializing Long-Lived Process Daemons...
[LLAP Daemon] Allocated 16GB off-heap memory cache across NodeManagers.
✓ Hive LLAP Service Active (Low-latency in-memory SQL execution ready).`;
    }

    
    if (trimmed.startsWith('spark-shell')) {
      if (!this.isYARNStarted) return `Spark Error: YARN ResourceManager is STOPPED. Run 'start-yarn.sh'.`;
      return `Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = yarn, app id = app_${Date.now()}_0010).
SparkSession available as 'spark'.
Using Scala version 2.12.15 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_311)
scala> val textFile = spark.read.text("/Hacker/about.txt")
textFile: org.apache.spark.sql.DataFrame = [value: string]
scala> textFile.count()
res0: Long = 125`;
    }

    
    if (trimmed.startsWith('pyspark')) {
      return '__LAUNCH_PYSPARK__';
    }

        if (trimmed.startsWith('kyuubi')) {
      return `[Apache Kyuubi SQL Gateway] Server starting on thrift server port 10009...
✓ Kyuubi Server active (Engine: Spark SQL, Multi-tenant Session Isolation Enabled).`;
    }

    
    if (trimmed.startsWith('impala-shell')) {
      return `[Impala Shell v3.4.0] Connected to impalad at localhost:21000
Query: SELECT * FROM sales
+----+--------+---------+--------+
| id | region | product | amount |
+----+--------+---------+--------+
| 1  | North  | Laptop  | 1200.0 |
| 2  | South  | Phone   | 800.0  |
+----+--------+---------+--------+
Fetched 2 row(s) in 0.015s`;
    }

    
    if (trimmed.startsWith('nifi.sh')) {
      return `[Apache NiFi Flow Engine] Starting Flow Controller...
✓ NiFi Web UI initialized on https://localhost:8443/nifi (Active Flow Processors: 12).`;
    }

    
    if (trimmed.startsWith('zeppelin-daemon.sh')) {
      return `[Zeppelin Notebook Server] Starting Web UI server on http://localhost:8080...
✓ Zeppelin Daemon started successfully (Interpreters: Spark, Hive, Shell, Presto active).`;
    }

    
    if (trimmed.startsWith('tez-job.sh') || trimmed.startsWith('tez')) {
      if (!this.isYARNStarted) return `Tez Error: YARN ResourceManager is STOPPED. Run 'start-yarn.sh'.`;
      return `[Apache Tez Engine] Compiling DAG (Vertices: 3, Edges: 2)...
[Tez DAG Client] Submitting ApplicationToYARN (AppID: application_${Date.now()}_0009)
[Tez Task Scheduler] Memory-pipelined data transfers active between vertices.
✓ Tez DAG Execution Succeeded. Bypassed intermediate HDFS disk writes.`;
    }

    
    if (trimmed.startsWith('phoenix-sqlline')) {
      return `Building JDBC Connection to HBase Zookeeper [localhost:2181]...
Connected to: Phoenix (version 5.1)
1/1          SELECT * FROM users;
+-------+-------+
| NAME  | ROLE  |
+-------+-------+
| Hacker| Admin |
+-------+-------+
1 row selected (0.042 seconds)`;
    }

    
    if (trimmed.startsWith('presto') || trimmed.startsWith('trino')) {
      return `Presto/Trino Distributed Engine:
id   region   product   amount
------------------------------
1    North    Laptop    1200.0
2    South    Phone     800.0
Query 20260825_0001, FINISHED, 4 nodes`;
    }



    
    if (trimmed.startsWith('ranger policy')) {
      return `Apache Ranger Security Policies:
  ID   Resource               Service   Permission   Users
  1    /Hacker/*              HDFS      rwx          Hacker, alice
  2    default.sales          Hive      select       Hacker, analytics_team
  3    users                  HBase     read, write  Hacker`;
    }

    
    if (trimmed.startsWith('kafka-topics.sh')) {
      return `Created topic logs on Kafka cluster [localhost:9092] (Partitions: 3, ReplicationFactor: 2)`;
    }

    
    if (trimmed.startsWith('flume-ng')) {
      if (!this.isHDFSStarted) return `Flume Error: Cannot ingest stream. HDFS NameNode is STOPPED. Run 'start-dfs.sh'.`;
      return `[Flume Agent a1] Ingesting event stream -> HDFS sink (/Hacker/flume_stream)
✓ Stream Event Sink active (Ingested 500 events/sec into HDFS).`;
    }

    
    if (trimmed.startsWith('oozie job')) {
      if (!this.isYARNStarted) return `Oozie Error: YARN ResourceManager is STOPPED. Run 'start-yarn.sh'.`;
      return `Job ID: 0000001-${Date.now()}-oozie-oozi-W
[Oozie DAG Engine] Workflow started: Fork Node -> MapReduce -> Hive Action -> Join Node
✓ Workflow status: SUCCEEDED`;
    }

    
    if (trimmed.startsWith('kinit')) {
      const principal = trimmed.split(/\s+/)[1] || 'Hacker@HADOOP.LOCAL';
      this.kerberosTicket = principal;
      return `Password for ${principal}: ********
✓ Ticket grant received for principal ${principal} (Valid for 24h).`;
    }

    if (trimmed === 'klist') {
      if (!this.kerberosTicket) return `klist: No credentials cache found`;
      return `Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: ${this.kerberosTicket}

Valid starting       Expires              Service principal
${new Date().toISOString()}  ${new Date(Date.now() + 86400000).toISOString()}  krbtgt/HADOOP.LOCAL@HADOOP.LOCAL`;
    }

    if (trimmed === 'kdestroy') {
      this.kerberosTicket = null;
      return `✓ Kerberos ticket cache destroyed.`;
    }

    
    
    if (trimmed.startsWith('spark-submit')) {
      if (!this.isYARNStarted) {
        return `Spark Error: YARN ResourceManager is STOPPED. Run 'start-yarn.sh' to start YARN.`;
      }
      return `[SparkSubmit] Submitting application to YARN cluster...
[YARN Client] Application ID: application_${Date.now()}_0002
[Spark Engine] Initialized SparkContext on YARN cluster (vCores: 4, Executing RDD Transformations)
✓ Spark Job Executed Successfully. Output written to HDFS.`;
    }

    
    if (trimmed.startsWith('sqoop import')) {
      if (!this.isHDFSStarted) {
        return `Sqoop Error: HDFS NameNode is STOPPED. Run 'start-dfs.sh' to start HDFS.`;
      }
      return `[Sqoop Importer] Connecting to Relational Database (RDBMS)...
[Sqoop Importer] Extracting database schema...
[MapReduce Engine] Map-only job started to import records into HDFS
✓ Sqoop Import Complete. Imported 1,000 records into HDFS target directory.`;
    }

    if (cmd.utility === 'clear') return '__CLEAR__';
    if (cmd.utility === 'unknown') {
      return `Command not recognized: '${commandLine}'. Type help for available commands.`;
    }

    if (cmd.utility === 'help') {
      return this.getGeneralHelpText();
    }

    if (cmd.utility === 'script') {
      return this.executeScriptCommand(cmd);
    }

    if (cmd.utility === 'linux') {
      return this.executeLinuxCommand(cmd);
    }

    
    if (cmd.utility === 'hdfs') {
      const isFormatCmd = cmd.subcommand === 'namenode' || cmd.flags.has('-format') || cmd.action === '-format' || cmd.positionalArgs.includes('-format');
      if (!this.isHDFSStarted && !isFormatCmd) {
        return `ls: Cannot connect to NameNode at localhost:9000. Connection refused.
Hadoop HDFS services are currently STOPPED.
Run 'start-dfs.sh' or 'start-all.sh' to start NameNode & DataNodes.`;
      }
      return this.executeHDFSCommand(cmd);
    }

    if (cmd.utility === 'mapred') {
      if (cmd.subcommand === 'job' && !this.isYARNStarted) {
        return `Unable to connect to ResourceManager at localhost:8032. Run 'start-yarn.sh' first.`;
      }
      return this.mapredCommand(cmd);
    }

    if (cmd.utility === 'hadoop' && cmd.subcommand !== 'jar' && cmd.subcommand !== 'distcp') {
      if (cmd.subcommand === 'dfs' && !this.isHDFSStarted) {
        return `${(cmd.action || '-ls').replace(/^-/, '')}: Call From localhost/127.0.0.1 to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused
Hadoop HDFS services are currently STOPPED. Run 'start-dfs.sh' or 'start-all.sh'.`;
      }
      if (cmd.subcommand === 'dfs') return this.executeHDFSCommand(cmd);
      return this.executeHadoopCommand(cmd);
    }

    if (cmd.utility === 'hadoop') {
      if (!this.isYARNStarted) {
        return `Unable to connect to ResourceManager at localhost:8088. Connection refused.
Hadoop YARN services are currently STOPPED.
Run 'start-yarn.sh' or 'start-all.sh' to start YARN services.`;
      }
      return this.executeHadoopCommand(cmd);
    }

    if (cmd.utility === 'yarn') {
      if (!this.isYARNStarted) {
        return `Unable to connect to ResourceManager at localhost:8088. Connection refused.
Hadoop YARN services are currently STOPPED.
Run 'start-yarn.sh' or 'start-all.sh' to start YARN services.`;
      }
      return this.executeYARNCommand(cmd);
    }

    return `Unsupported command: ${commandLine}`;
  }

  private executeScriptCommand(cmd: ParsedCommand): string {
    const act = cmd.action || '';

    if (act.startsWith('start-dfs')) {
      if (!this.isNameNodeFormatted) {
        return `Error: NameNode is not formatted! Please run 'hdfs namenode -format' first.`;
      }
      this.isHDFSStarted = true;
      this.persistShellState();
      return `Starting namenodes on [localhost]
Starting datanodes on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
Starting secondary namenodes [localhost]
✓ HDFS daemons initialized successfully (NameNode: ACTIVE, DataNodes: 3/3 LIVE).`;
    }

    if (act.startsWith('stop-dfs')) {
      this.isHDFSStarted = false;
      this.persistShellState();
      return `Stopping datanodes on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
Stopping namenodes on [localhost]
Stopping secondary namenodes [localhost]
✓ HDFS daemons stopped cleanly.`;
    }

    if (act.startsWith('start-yarn')) {
      this.isYARNStarted = true;
      this.persistShellState();
      return `Starting resourcemanager on [localhost]
Starting nodemanagers on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
✓ YARN Capacity Scheduler daemons initialized successfully.`;
    }

    if (act.startsWith('stop-yarn')) {
      this.isYARNStarted = false;
      this.persistShellState();
      return `Stopping nodemanagers on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
Stopping resourcemanager on [localhost]
✓ YARN daemons stopped cleanly.`;
    }

    if (act.startsWith('start-all')) {
      if (!this.isNameNodeFormatted) {
        return `Error: NameNode is not formatted! Please run 'hdfs namenode -format' first.`;
      }
      this.isHDFSStarted = true;
      this.isYARNStarted = true;
      this.persistShellState();
      return `Starting HDFS & YARN services...
Starting namenodes on [localhost]
Starting datanodes on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
Starting resourcemanager on [localhost]
Starting nodemanagers on [datanode1.hadoop.local, datanode2.hadoop.local, datanode3.hadoop.local]
✓ All Hadoop cluster daemons started successfully.`;
    }

    if (act.startsWith('stop-all')) {
      this.isHDFSStarted = false;
      this.isYARNStarted = false;
      this.persistShellState();
      return `Stopping all Hadoop cluster daemons...
Stopping nodemanagers & resourcemanager...
Stopping datanodes & namenode...
✓ All Hadoop cluster daemons stopped.`;
    }

    return `Script execution complete: ${act}`;
  }

  private executeLinuxCommand(cmd: ParsedCommand): string {
    const act = cmd.action || '';
    const args = cmd.positionalArgs;

    if (act === 'node') {
      if (args.length === 0) {
        return `Welcome to Node.js v18.16.0.
Type ".help" for more information.
> console.log("Hadoop Polyglot Node.js Engine Active");
Hadoop Polyglot Node.js Engine Active
undefined
> `;
      }

      if (args[0] === '-v' || args[0] === '--version') {
        return `v18.16.0`;
      }

      let codeToRun = '';
      if (args[0] === '-e' || args[0] === '--eval') {
        codeToRun = args.slice(1).join(' ').replace(/^["']|["']$/g, '');
      } else {
        const scriptFile = args[0];
        const resolved = this.resolveLocalPath(scriptFile);
        const content = this.getLocalFileContent(resolved);
        if (content !== undefined) {
          codeToRun = content;
        } else {
          return `node: internal/modules/cjs/loader:1080 throw err; Cannot find module '${scriptFile}'`;
        }
      }

      
      const logs: string[] = [];
      const customConsole = {
        log: (...logArgs: any[]) => logs.push(logArgs.map((a) => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ')),
        error: (...logArgs: any[]) => logs.push(logArgs.map((a) => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ')),
        warn: (...logArgs: any[]) => logs.push(logArgs.map((a) => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' '))
      };

      try {
        const fn = new Function('console', codeToRun);
        fn(customConsole);
        return logs.length > 0 ? logs.join('\n') : '';
      } catch (err: any) {
        return `Uncaught ${err.name || 'Error'}: ${err.message}`;
      }
    }

    if (act === 'pig' && (args[0] === '-e' || args[0] === '-execute')) {
      const script = cmd.raw.trim().replace(/^pig\s+-e(?:xecute)?\s+/i, '').trim().replace(/^(["'])([\s\S]*)\1$/, '$2');
      if (!script) return 'Usage: pig -e "a = LOAD \'file.csv\' USING PigStorage(\',\'); DUMP a;"';
      return `__PIG_EXEC__:${script}`;
    }

    if (act === 'pig' && args[0] === '-f') {
      if (!args[1]) return 'Usage: pig -f script.pig';
      const content = this.getLocalFileContent(this.resolveLocalPath(args[1]));
      if (content === undefined) return `ERROR: Pig script file '${args[1]}' does not exist.`;
      return `__PIG_EXEC__:${content}`;
    }

    if (act === 'hive' && (args[0] === '-e' || args[0] === '-f')) {
      if (args[0] === '-e') {
        const script = cmd.raw.trim().replace(/^hive\s+-e\s+/i, '').trim().replace(/^(["'])([\s\S]*)\1$/, '$2');
        return script ? `__HIVE_EXEC__:${script}` : 'Usage: hive -e "SELECT ..."';
      }
      const content = args[1] ? this.getLocalFileContent(this.resolveLocalPath(args[1])) : undefined;
      if (content === undefined) return `Could not open input file for reading. (${args[1] || 'missing file name'})`;
      return `__HIVE_EXEC__:${content}`;
    }

    if (act === 'beeline') {
      const eIdx = args.indexOf('-e');
      const fIdx = args.indexOf('-f');
      if (eIdx >= 0) {
        const m = cmd.raw.match(/\s-e\s+(["'])([\s\S]*)\1/) || cmd.raw.match(/\s-e\s+(\S+)/);
        return `__HIVE_EXEC__:${m ? m[m.length - 1] : ''}`;
      }
      if (fIdx >= 0) {
        const content = this.getLocalFileContent(this.resolveLocalPath(args[fIdx + 1] || ''));
        return content === undefined ? `${args[fIdx + 1]} (No such file or directory)` : `__HIVE_EXEC__:${content}`;
      }
      return '__LAUNCH_KSQL__';
    }

    if (act === 'hive' || act === 'pig') {
      return '__LAUNCH_KSQL__';
    }

    if (act === 'download') {
      if (args.length === 0) {
        return `Usage: download <filename.ext>`;
      }
      const targetFile = args[0];
      return `__DOWNLOAD_FILE__:${targetFile}`;
    }

    if (act === 'about') {
      return ABOUT_GUIDE_CONTENT;
    }

    if (act === 'get') {
      return '__GET_FILE__';
    }

    if (act === 'jps') {
      const processList: string[] = [];
      if (this.isHDFSStarted) {
        processList.push('18291 NameNode');
        processList.push('18420 DataNode');
        processList.push('18590 SecondaryNameNode');
      }
      if (this.isYARNStarted) {
        processList.push('19842 ResourceManager');
        processList.push('20115 NodeManager');
      }
      if (this.historyServerRunning) {
        processList.push('20877 JobHistoryServer');
      }
      processList.push('21045 Jps');
      return processList.join('\n');
    }

    if (act === 'pyspark' || act === 'spark' || act === 'spark-shell') {
      if (args.length > 0) {
        return this.executeLinuxCommand({
          raw: `python ${args.join(' ')}`,
          utility: 'linux',
          action: 'python',
          flags: new Set(),
          positionalArgs: args
        });
      }
      return '__LAUNCH_PYSPARK__';
    }

    if (act === 'spark-submit') {
      if (args.length === 0) {
        return `Usage: spark-submit [options] <app.py> [app arguments]`;
      }
      const scriptFile = args.find((a) => a.endsWith('.py')) || args[args.length - 1];
      const result = this.executeLinuxCommand({
        raw: `python ${scriptFile}`,
        utility: 'linux',
        action: 'python',
        flags: new Set(),
        positionalArgs: [scriptFile]
      });
      return `[Spark DAG Scheduler] Submitting PySpark Job to Cluster...
[Spark DAG Scheduler] Stage 0: MapReduce Pipeline Completed.
[Spark DAG Scheduler] Job 0 finished in 0.42s

${result}`;
    }

    if (act === 'python' || act === 'python3') {
      if (args.length === 0) {
        return '__LAUNCH_PYTHON__';
      }

      this.pythonEngine.initPyodide();

      if (args[0] === '-v' || args[0] === '--version' || args[0] === '-V') {
        return `${this.pythonEngine.getPythonVersion() || 'Python 3.14.2'} (Pyodide WebAssembly Engine)`;
      }

      let codeToRun = '';
      const suppliedInputs: string[] = [];
      let scriptName = '<string>';

      if (args[0] === '-c') {
        codeToRun = args.slice(1).join(' ').replace(/^["']|["']$/g, '');
      } else {
        const scriptFile = args[0];
        const resolved = this.resolveLocalPath(scriptFile);
        const content = this.getLocalFileContent(resolved);
        if (content !== undefined) {
          codeToRun = content;
          scriptName = scriptFile;
          if (args.length > 1) {
            suppliedInputs.push(...args.slice(1));
          }
        } else if (scriptFile.endsWith('.py')) {
          return `python: can't open file '${scriptFile}': [Errno 2] No such file or directory`;
        } else {
          codeToRun = args.join(' ').replace(/^["']|["']$/g, '');
        }
      }

      return this.executePython(codeToRun, suppliedInputs, scriptName);
    }

    if (act === 'ps') {
      return `UID        PID  PPID  C STIME TTY          TIME CMD
Hacker    1042     1  1 13:00 ?        00:14:22 java -Dproc_namenode org.apache.hadoop.hdfs.server.namenode.NameNode
Hacker    1150     1  0 13:00 ?        00:09:45 java -Dproc_resourcemanager org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
Hacker    1280     1  0 13:00 ?        00:05:12 java -Dproc_datanode org.apache.hadoop.hdfs.server.datanode.DataNode
Hacker    1310     1  0 13:00 ?        00:02:10 java org.apache.hive.service.server.HiveServer2`;
    }

    if (act === 'kill') {
      const pidArg = args[args.length - 1] || '1042';
      return `✓ Process ${pidArg} terminated.`;
    }

    if (act === 'killall') {
      const procName = args[0] || 'java';
      return `✓ All ${procName} processes terminated.`;
    }

    if (act === 'ifconfig' || act === 'ip') {
      return `eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 192.168.1.100  netmask 255.255.255.0  broadcast 192.168.1.255
        inet6 fe80::a00:27ff:fe4e:66a1  prefixlen 64  scopeid 0x20<link>
        ether 08:00:27:4e:66:a1  txqueuelen 1000  (Ethernet)

lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
        inet 127.0.0.1  netmask 255.0.0.0`;
    }

    if (act === 'ping') {
      const nodeHost = args[0] || 'datanode1.hadoop.local';
      return `PING ${nodeHost} (192.168.1.101) 56(84) bytes of data.
64 bytes from ${nodeHost}: icmp_seq=1 ttl=64 time=0.142 ms
64 bytes from ${nodeHost}: icmp_seq=2 ttl=64 time=0.118 ms
--- ${nodeHost} ping statistics ---
2 packets transmitted, 2 received, 0% packet loss, time 1001ms
rtt min/avg/max/mdev = 0.118/0.130/0.142/0.012 ms`;
    }

    if (act === 'netstat') {
      return `Active Internet connections (only servers)
Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name
tcp        0      0 0.0.0.0:9000            0.0.0.0:*               LISTEN      1042/java (NameNode)
tcp        0      0 0.0.0.0:8088            0.0.0.0:*               LISTEN      1150/java (ResourceManager)
tcp        0      0 0.0.0.0:9870            0.0.0.0:*               LISTEN      1042/java (NameNode Web UI)
tcp        0      0 0.0.0.0:2181            0.0.0.0:*               LISTEN      990/java (ZooKeeper)
tcp        0      0 0.0.0.0:10000           0.0.0.0:*               LISTEN      1310/java (Hive Server2)`;
    }

    if (act === 'top') {
      return `top - 13:16:00 up 42 days,  3:14,  1 user,  load average: 0.12, 0.08, 0.05
Tasks: 180 total,   1 running, 179 sleeping,   0 stopped,   0 zombie
%Cpu(s):  2.4 us,  1.2 sy,  0.0 ni, 96.1 id,  0.3 wa,  0.0 hi,  0.0 si,  0.0 st
MiB Mem :  16384.0 total,   4096.0 free,   8192.0 used,   4096.0 buff/cache
MiB Swap:   2048.0 total,   2048.0 free,      0.0 used.   7168.0 avail Mem

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 1042 Hacker    20   0 4892100 812400 120400 S   1.8   5.0  14:22.10 java (NameNode)
 1150 Hacker    20   0 3120400 512000  98000 S   1.2   3.1   9:45.30 java (ResourceManager)
 1280 Hacker    20   0 2400100 412000  85000 S   0.8   2.5   5:12.10 java (DataNode)`;
    }

    if (act === 'df') {
      const usedMB = (this.cachedStorageUsage / (1024 * 1024)).toFixed(1);
      const quotaMB = (this.cachedStorageQuota / (1024 * 1024)).toFixed(1);
      const availMB = ((this.cachedStorageQuota - this.cachedStorageUsage) / (1024 * 1024)).toFixed(1);
      const usePct = this.cachedStorageQuota > 0
        ? ((this.cachedStorageUsage / this.cachedStorageQuota) * 100).toFixed(0)
        : '0';
      return `Filesystem               Size      Used     Avail    Use%  Mounted on
hadoop-lab (IndexedDB)   ${quotaMB}M    ${usedMB}M    ${availMB}M   ${usePct}%   /
SW Cache                 ${(this.cachedStorageUsage * 0.3 / (1024*1024)).toFixed(1)}M    ${(this.cachedStorageUsage * 0.3 / (1024*1024)).toFixed(1)}M    0.0M  100%   /sw-cache`;
    }



    if (act === 'free') {
      return `               total        used        free      shared  buff/cache   available
Mem:           16384        8192        4096         256        4096        7168
Swap:           2048           0        2048`;
    }

    if (act === 'uname') {
      return `Linux hadoop.local 5.15.0-88-generic #98-Ubuntu SMP Mon Oct 2 15:18:56 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux`;
    }

    if (act === 'pwd') {
      return this.localDir;
    }

    if (act === 'whoami') {
      return 'Hacker';
    }

    if (act === 'history') {
      return this.historyList.map((c, i) => ` ${(i + 1).toString().padStart(4)}  ${c}`).join('\n');
    }

    if (act === 'cd') {
      const target = args[0] || '/home/Hacker';
      const resolved = this.resolveLocalPath(target);
      if (this.localDirs.has(resolved)) {
        this.localDir = resolved;
        this.persistShellState();
        return '';
      }
      return `cd: no such file or directory: ${target}`;
    }

    if (act === 'mkdir') {
      if (args.length === 0) return 'mkdir: missing operand';
      const target = this.resolveLocalPath(args[0]);
      this.localDirs.add(target);
      this.db.saveLocalDir(target);
      return `✓ Created local directory: ${target}`;
    }

    if (act === 'rmdir') {
      if (args.length === 0) return 'rmdir: missing operand';
      if (args.length > 1) return 'rmdir: too many arguments';
      const target = this.resolveLocalPath(args[0]);
      if (!this.localDirs.has(target)) {
        if (this.localFiles.has(target)) return `rmdir: failed to remove '${args[0]}': Not a directory`;
        return `rmdir: failed to remove '${args[0]}': No such file or directory`;
      }
      if (target === '/' || target === '/home' || target === '/home/Hacker') {
        return `rmdir: failed to remove '${args[0]}': Permission denied`;
      }
      const prefix = target.endsWith('/') ? target : target + '/';
      const hasChildDirectory = [...this.localDirs].some((dir) => dir !== target && dir.startsWith(prefix));
      const hasChildFile = [...this.localFiles.keys()].some((file) => file.startsWith(prefix));
      if (hasChildDirectory || hasChildFile) {
        return `rmdir: failed to remove '${args[0]}': Directory not empty`;
      }
      if (this.localDir === target || this.localDir.startsWith(prefix)) {
        return `rmdir: failed to remove '${args[0]}': Directory is in use`;
      }
      this.localDirs.delete(target);
      this.db.deleteLocalDir(target);
      return `✓ Removed local directory: ${target}`;
    }

    if (act === 'touch') {
      if (args.length === 0) return 'touch: missing file operand';
      const target = this.resolveLocalPath(args[0]);
      if (!this.localFiles.has(target)) {
        this.localFiles.set(target, '');
        this.db.saveLocalFile(target, '');
      }
      return '';
    }

    if (act === 'echo') {
      const line = cmd.raw.substring(cmd.raw.indexOf('echo') + 4).trim();
      if (line.includes('>')) {
        const parts = line.split('>');
        const text = parts[0].replace(/^["']|["']$/g, '').replace(/\\n/g, '\n').replace(/\\t/g, '    ').trim();
        const fileTarget = this.resolveLocalPath(parts[1].trim());
        this.saveLocalFileContent(fileTarget, text + '\n');
        return `✓ Written to local file: ${parts[1].trim()}`;
      }
      return line.replace(/^["']|["']$/g, '').replace(/\\n/g, '\n').replace(/\\t/g, '    ');
    }

    if (act === 'cat') {
      if (args.length === 0) return 'cat: missing file operand';
      const parts: string[] = [];
      for (const name of args) {
        const target = this.resolveLocalPath(name);
        const content = this.getLocalFileContent(target);
        if (content !== undefined) {
          parts.push(content.endsWith('\n') ? content.slice(0, -1) : content);
        } else if (this.localDirs.has(target)) {
          parts.push(`cat: ${name}: Is a directory`);
        } else {
          parts.push(`cat: ${name}: No such file or directory`);
        }
      }
      return parts.join('\n');
    }

    if (act === 'grep') {
      if (args.length < 2) return `grep: Usage: grep <pattern> <file>`;
      const pattern = args[0];
      const target = this.resolveLocalPath(args[1]);
      const content = this.getLocalFileContent(target);
      if (content === undefined) return `grep: ${args[1]}: No such file or directory`;

      const matches = content.split('\n').filter((l) => l.toLowerCase().includes(pattern.toLowerCase()));
      return matches.length > 0 ? matches.join('\n') : '';
    }

    if (act === 'wc') {
      if (args.length === 0) return `wc: missing file operand`;
      const target = this.resolveLocalPath(args[args.length - 1]);
      const content = this.getLocalFileContent(target);
      if (content === undefined) return `wc: ${args[args.length - 1]}: No such file or directory`;

      const lines = content.split('\n').length - (content.endsWith('\n') ? 1 : 0);
      const words = content.trim().split(/\s+/).filter(Boolean).length;
      const chars = content.length;
      return `${lines.toString().padStart(6)} ${words.toString().padStart(6)} ${chars.toString().padStart(6)} ${args[args.length - 1]}`;
    }

    if (act === 'head') {
      if (args.length === 0) return `head: missing file operand`;
      const target = this.resolveLocalPath(args[args.length - 1]);
      const content = this.getLocalFileContent(target);
      if (content === undefined) return `head: ${args[args.length - 1]}: No such file or directory`;

      return content.split('\n').slice(0, 10).join('\n');
    }

    if (act === 'tail') {
      if (args.length === 0) return `tail: missing file operand`;
      const target = this.resolveLocalPath(args[args.length - 1]);
      const content = this.getLocalFileContent(target);
      if (content === undefined) return `tail: ${args[args.length - 1]}: No such file or directory`;

      return content.split('\n').slice(-10).join('\n');
    }

    if (act === 'ls') {
      const targetDir = args[0] ? this.resolveLocalPath(args[0]) : this.localDir;
      const items: string[] = [];
      const prefix = targetDir.endsWith('/') ? targetDir : targetDir + '/';

      this.localDirs.forEach((d) => {
        if (d !== targetDir && d.startsWith(prefix)) {
          const sub = d.substring(prefix.length);
          if (!sub.includes('/')) items.push(`drwxr-xr-x  ${sub}/`);
        }
      });

      this.localFiles.forEach((_, f) => {
        if (f.startsWith(prefix)) {
          const sub = f.substring(prefix.length);
          if (!sub.includes('/')) items.push(`-rw-r--r--  ${sub}`);
        }
      });

      return items.length > 0 ? items.join('\n') : '.';
    }

    if (act === 'rm') {
      if (args.length === 0) return 'rm: missing operand';
      const target = this.resolveLocalPath(args[0]);
      let deleted = false;
      if (this.localFiles.has(target)) {
        this.localFiles.delete(target);
        this.db.deleteLocalFile(target);
        deleted = true;
      }
      if (this.localDirs.has(target)) {
        this.localDirs.delete(target);
        this.db.deleteLocalDir(target);
        deleted = true;
      }

      return deleted ? `✓ Removed local path: ${args[0]}` : `rm: cannot remove '${args[0]}': No such file or directory`;
    }

    if (act === 'cp') {
      if (args.length < 2) return 'cp: missing file operand. Usage: cp <src> <dst>';
      const isRecursive = args.includes('-r') || args.includes('-R');
      const cleanArgs = args.filter((a) => a !== '-r' && a !== '-R');
      if (cleanArgs.length < 2) return 'cp: missing destination file operand after ' + cleanArgs[0];

      const srcPath = this.resolveLocalPath(cleanArgs[0]);
      let dstPath = this.resolveLocalPath(cleanArgs[1]);

      if (this.localDirs.has(dstPath)) {
        const fileName = srcPath.split('/').pop() || 'file';
        dstPath = dstPath.endsWith('/') ? dstPath + fileName : dstPath + '/' + fileName;
      }

      const content = this.getLocalFileContent(srcPath);
      if (content !== undefined) {
        this.saveLocalFileContent(dstPath, content);
        return `✓ Copied local file: ${cleanArgs[0]} -> ${cleanArgs[1]}`;
      }

      if (isRecursive && this.localDirs.has(srcPath)) {
        let copiedCount = 0;
        const srcPrefix = srcPath.endsWith('/') ? srcPath : srcPath + '/';
        const dstPrefix = dstPath.endsWith('/') ? dstPath : dstPath + '/';

        this.localDirs.add(dstPath);
        this.db.saveLocalDir(dstPath);

        this.localDirs.forEach((d) => {
          if (d.startsWith(srcPrefix)) {
            const rel = d.substring(srcPrefix.length);
            const newDir = dstPrefix + rel;
            this.localDirs.add(newDir);
            this.db.saveLocalDir(newDir);
          }
        });

        this.localFiles.forEach((val, f) => {
          if (f.startsWith(srcPrefix)) {
            const rel = f.substring(srcPrefix.length);
            const newFile = dstPrefix + rel;
            this.saveLocalFileContent(newFile, val);
            copiedCount++;
          }
        });

        return `✓ Copied local directory '${cleanArgs[0]}' -> '${cleanArgs[1]}' (${copiedCount} files)`;
      }

      return `cp: cannot stat '${cleanArgs[0]}': No such file or directory`;
    }

    if (act === 'mv') {
      if (args.length < 2) return 'mv: missing file operand. Usage: mv <src> <dst>';
      const srcPath = this.resolveLocalPath(args[0]);
      let dstPath = this.resolveLocalPath(args[1]);

      if (this.localDirs.has(dstPath)) {
        const fileName = srcPath.split('/').pop() || 'file';
        dstPath = dstPath.endsWith('/') ? dstPath + fileName : dstPath + '/' + fileName;
      }

      const content = this.getLocalFileContent(srcPath);
      if (content !== undefined) {
        this.saveLocalFileContent(dstPath, content);
        this.localFiles.delete(srcPath);
        this.db.deleteLocalFile(srcPath);
        return `✓ Moved local file: ${args[0]} -> ${args[1]}`;
      }

      if (this.localDirs.has(srcPath)) {
        const srcPrefix = srcPath.endsWith('/') ? srcPath : srcPath + '/';
        const dstPrefix = dstPath.endsWith('/') ? dstPath : dstPath + '/';

        this.localDirs.add(dstPath);
        this.db.saveLocalDir(dstPath);

        this.localFiles.forEach((val, f) => {
          if (f.startsWith(srcPrefix)) {
            const rel = f.substring(srcPrefix.length);
            const newFile = dstPrefix + rel;
            this.saveLocalFileContent(newFile, val);
            this.localFiles.delete(f);
            this.db.deleteLocalFile(f);
          }
        });

        this.localDirs.delete(srcPath);
        this.db.deleteLocalDir(srcPath);
        return `✓ Renamed/moved directory '${args[0]}' -> '${args[1]}'`;
      }

      return `mv: cannot stat '${args[0]}': No such file or directory`;
    }

    return `Linux command executed: ${act}`;
  }

  private executeHDFSCommand(cmd: ParsedCommand): string {
    const { subcommand, action, flags, positionalArgs } = cmd;

    if (subcommand === 'crypto') {
      if (action === '-listZones') {
        const lines = ['/Hacker/secure   key1', '------------------------+'];
        this.cryptoZones.forEach((key, path) => {
          lines.push(`${path.padEnd(16)} ${key}`);
        });
        return lines.join('\n');
      }
      if (action === '-createZone') {
        const key = positionalArgs[1] || 'key1';
        const path = positionalArgs[3] || '/Hacker/secure';
        this.cryptoZones.set(path, key);
        return `Added encryption zone ${path} with key ${key}`;
      }
    }

    if (subcommand === 'balancer') {
      return `[HDFS Balancer] Rebalancing block allocation across 3 DataNodes (Threshold: 10.0%)
[HDFS Balancer] Moved 1,024 MB across DataNode disks.
✓ Cluster HDFS storage is now balanced.`;
    }

    if (subcommand === 'diskbalancer') {
      return `[DiskBalancer] Generating data volume balancing plan for datanode1.hadoop.local
✓ DiskBalancer Plan generated and executed. Volume skew reduced to 0.01.`;
    }

    if (subcommand === 'storagepolicies') {
      if (action === '-isSatisfierRunning') {
        return `Storage Policy Satisfier (SPS) daemon is RUNNING on Active NameNode nn1.`;
      }
      if (action === '-listPolicies') {
        return `Block Storage Policies:
  1. HOT (Disk: 3) [Default]
  2. WARM (Disk: 1, Archive: 2)
  3. COLD (Archive: 3)
  4. LAZY_PERSIST (RAM_Disk: 1, Disk: 2)`;
      }
      if (action === '-setStoragePolicy') {
        const pathArg = positionalArgs[1] || '/';
        const polArg = positionalArgs[3] || 'COLD';
        this.storagePolicies.set(pathArg, polArg);
        return `Set storage policy ${polArg} on ${pathArg}`;
      }
      if (action === '-satisfyStoragePolicy') {
        const pathArg = positionalArgs[1] || '/';
        return `Scheduled storage policy satisfaction for ${pathArg}`;
      }
    }

    if (subcommand === 'ec') {
      if (action === '-enablePolicy') {
        const polArg = positionalArgs[1] || 'RS-6-3-1024k';
        return `Erasure Coding policy ${polArg} enabled successfully`;
      }
      if (action === '-disablePolicy') {
        const polArg = positionalArgs[1] || 'RS-6-3-1024k';
        return `Erasure Coding policy ${polArg} disabled successfully`;
      }
      if (action === '-listPolicies') {
        return `Erasure Coding Policies:
  - RS-6-3-1024k (Reed-Solomon 6 data, 3 parity) [Enabled]
  - RS-3-2-1024k (Reed-Solomon 3 data, 2 parity)
  - XOR-2-1-1024k (XOR 2 data, 1 parity)`;
      }
      if (action === '-setPolicy') {
        const pathArg = positionalArgs[1] || '/';
        const polArg = positionalArgs[3] || 'RS-6-3-1024k';
        this.ecPolicies.set(pathArg, polArg);
        return `Set Erasure Coding policy ${polArg} on ${pathArg}`;
      }
    }

    if (subcommand === 'cacheadmin') {
      if (action === '-addDirective') {
        const pathArg = positionalArgs[1] || '/';
        const poolArg = positionalArgs[3] || 'pool1';
        this.cacheDirectives.set(pathArg, poolArg);
        return `Added cache directive 1 for path ${pathArg} in pool ${poolArg}`;
      }
      if (action === '-listDirectives') {
        const lines = ['ID   POOL    REPL  PATH', '-------------------------------+'];
        let idx = 1;
        this.cacheDirectives.forEach((pool, path) => {
          lines.push(`${idx.toString().padEnd(4)} ${pool.padEnd(7)} 1     ${path}`);
          idx++;
        });
        return lines.join('\n');
      }
    }

    if (subcommand === 'namenode' || flags.has('-format') || action === '-format' || positionalArgs.includes('-format')) {
      return this.formatNameNode(flags.has('-force') || positionalArgs.includes('-force'));
    }

    if (subcommand === 'haadmin') {
      if (action === '-getServiceState') {
        const nn = positionalArgs[0] || 'nn1';
        return nn === this.activeNameNodeId ? 'active' : 'standby';
      }
      if (action === '-failover') {
        const fromNN = positionalArgs[0] || 'nn1';
        const toNN = positionalArgs[1] || 'nn2';
        this.activeNameNodeId = toNN;
        return `Failover from ${fromNN} to ${toNN} successful
✓ ${fromNN} is now STANDBY
✓ ${toNN} is now ACTIVE`;
      }
    }

    if (subcommand === 'dfs') {
      // -find takes its own options (-name/-iname): give it the words in their original order.
      let dfsArgs = positionalArgs;
      if (action === '-find') {
        const words = shellSplit(cmd.raw);
        const at = words.indexOf('-find');
        dfsArgs = at >= 0 ? words.slice(at + 1) : positionalArgs;
      }
      const handled = this.dfs.run(action || '-help', flags, dfsArgs);
      if (handled !== null) return handled;
      switch (action) {
        case '-find': {
          const pathArg = positionalArgs[0] || '/';
          const nameArg = positionalArgs[2] || '*';
          return `${pathArg}\n${pathArg}/about.txt (${nameArg})\n${pathArg}/data.csv`;
        }

        case '-count': {
          const pathArg = positionalArgs[0] || '/';
          return `   DIR_COUNT   FILE_COUNT       CONTENT_SIZE PATHNAME\n           4            2               4096 ${pathArg}`;
        }

        case '-checksum': {
          const pathArg = positionalArgs[0] || '/';
          const node = this.nameNode.getNamespace().resolvePath(pathArg);
          if (!node || node.type !== 'FILE') return `checksum: \`${pathArg}\`: No such file`;
          return `${pathArg}\tMD5-of-073741824000000000000000\ta9b8c7d6e5f43210876543210abcdef1`;
        }

        case '-stat': {
          const pathArg = positionalArgs[1] || positionalArgs[0] || '/';
          const node = this.nameNode.getNamespace().resolvePath(pathArg);
          if (!node) return `stat: \`${pathArg}\`: No such file or directory`;
          const typeStr = node.type === 'DIRECTORY' ? 'directory' : 'regular file';
          const size = node.type === 'FILE' ? node.sizeBytes : 4096;
          return `${typeStr} ${node.owner}:${node.group} ${size} ${new Date(node.createdAt).toISOString().slice(0, 16).replace('T', ' ')}`;
        }

        case '-getfacl': {
          const pathArg = positionalArgs[0] || '/';
          const aclList = this.acls.get(pathArg) || ['user::rwx', 'group::r-x', 'other::r-x'];
          return `# file: ${pathArg}\n# owner: Hacker\n# group: supergroup\n${aclList.join('\n')}`;
        }

        case '-setfacl': {
          const pathArg = positionalArgs[1] || '/';
          const aclSpec = positionalArgs[0] || 'user:alice:rwx';
          let aclList = this.acls.get(pathArg) || ['user::rwx', 'group::r-x', 'other::r-x'];
          aclList.push(aclSpec);
          this.acls.set(pathArg, aclList);
          return `✓ Updated ACL for ${pathArg}`;
        }

        case '-expunge': {
          return `✓ Emptied Trash (/user/Hacker/.Trash). Storage reclaimed.`;
        }

        case '-allowSnapshot': {
          const pathArg = positionalArgs[0] || '/';
          let set = this.snapshots.get(pathArg);
          if (!set) {
            set = new Set();
            this.snapshots.set(pathArg, set);
          }
          return `Allowing snapshot on ${pathArg} succeeded`;
        }

        case '-createSnapshot': {
          const pathArg = positionalArgs[0] || '/';
          const snapName = positionalArgs[1] || `s_${Date.now()}`;
          let set = this.snapshots.get(pathArg);
          if (!set) {
            set = new Set();
            this.snapshots.set(pathArg, set);
          }
          set.add(snapName);
          return `Created snapshot ${pathArg}/.snapshot/${snapName}`;
        }

        case '-deleteSnapshot': {
          const pathArg = positionalArgs[0] || '/';
          const snapName = positionalArgs[1] || '';
          const set = this.snapshots.get(pathArg);
          if (set && set.has(snapName)) {
            set.delete(snapName);
            return `✓ Deleted snapshot ${snapName} from ${pathArg}`;
          }
          return `deleteSnapshot: Snapshot \`${snapName}\` does not exist for \`${pathArg}\``;
        }

        case '-ls': {
          const pathStr = positionalArgs[0] || '/';
          const node = this.nameNode.getNamespace().resolvePath(pathStr);
          if (!node) return `ls: \`${pathStr}\`: No such file or directory`;

          if (node.type === 'FILE') {
            return `${node.permissions}   ${node.replicationFactor} ${node.owner} ${node.group}  ${node.sizeBytes.toString().padStart(8)} ${new Date(node.createdAt).toISOString().slice(0, 16).replace('T', ' ')} ${node.name}`;
          } else {
            if (node.children.size === 0) return 'Found 0 items';
            const lines: string[] = [`Found ${node.children.size} items`];
            node.children.forEach((child) => {
              const rep = child.type === 'FILE' ? child.replicationFactor : '-';
              const sz = child.type === 'FILE' ? child.sizeBytes : 0;
              lines.push(
                `${child.permissions}   ${rep} ${child.owner} ${child.group}  ${sz.toString().padStart(8)} ${new Date(child.createdAt).toISOString().slice(0, 16).replace('T', ' ')} ${child.name}`
              );
            });
            return lines.join('\n');
          }
        }

        case '-mkdir': {
          if (positionalArgs.length === 0) return `mkdir: Missing directory argument`;
          const dirPath = positionalArgs[0];
          const isRecursive = flags.has('-p');
          try {
            this.nameNode.getNamespace().mkdir(dirPath, isRecursive);
            this.db.saveHdfsFileContent(dirPath, '__HADOOP_DIR__');
            return `✓ Created HDFS directory: ${dirPath}`;
          } catch (err: any) {
            return `mkdir: ${err.message}`;
          }
        }

        case '-touchz': {
          if (positionalArgs.length === 0) return `touchz: Missing file path argument`;
          const targetPath = positionalArgs[0];
          try {
            this.nameNode.createAndWriteFile(targetPath, '');
            this.db.saveHdfsFileContent(targetPath, '');
            return `✓ Created empty 0-byte HDFS file: ${targetPath}`;
          } catch (err: any) {
            return `touchz: ${err.message}`;
          }
        }

        case '-cp': {
          if (positionalArgs.length < 2) return `cp: Usage: hdfs dfs -cp <src> <dst>`;
          const src = positionalArgs[0];
          const dst = positionalArgs[1];
          const content = this.nameNode.readFileContent(src);
          if (content === undefined) return `cp: \`${src}\`: No such file or directory`;

          try {
            this.nameNode.createAndWriteFile(dst, content);
            this.db.saveHdfsFileContent(dst, content);
            return `✓ Copied in HDFS: [${src}] -> [${dst}]`;
          } catch (err: any) {
            return `cp: ${err.message}`;
          }
        }

        case '-mv': {
          if (positionalArgs.length < 2) return `mv: Usage: hdfs dfs -mv <src> <dst>`;
          const src = positionalArgs[0];
          const dst = positionalArgs[1];
          const content = this.nameNode.readFileContent(src);
          if (content === undefined) return `mv: \`${src}\`: No such file or directory`;

          try {
            this.nameNode.createAndWriteFile(dst, content);
            this.nameNode.getNamespace().deletePath(src, true);
            this.db.saveHdfsFileContent(dst, content);
            this.db.deleteHdfsFileContent(src);
            return `✓ Moved in HDFS: [${src}] -> [${dst}]`;
          } catch (err: any) {
            return `mv: ${err.message}`;
          }
        }

        case '-setrep': {
          if (positionalArgs.length < 2) return `setrep: Usage: hdfs dfs -setrep [-w] <replication> <path>`;
          const newRep = parseInt(positionalArgs[0]) || 3;
          const targetPath = positionalArgs[1];
          const node = this.nameNode.getNamespace().resolvePath(targetPath);
          if (!node || node.type !== 'FILE') return `setrep: \`${targetPath}\`: No such file`;

          (node as INodeFile).replicationFactor = newRep;
          return `Replication ${newRep} set for ${targetPath}`;
        }

        case '-chmod': {
          if (positionalArgs.length < 2) return `chmod: Usage: hdfs dfs -chmod <mode> <path>`;
          const mode = positionalArgs[0];
          const targetPath = positionalArgs[1];
          const node = this.nameNode.getNamespace().resolvePath(targetPath);
          if (!node) return `chmod: \`${targetPath}\`: No such file or directory`;
          node.permissions = mode.startsWith('-') || mode.startsWith('d') ? mode : `-${mode}`;
          return `Changed permissions of ${targetPath} to ${node.permissions}`;
        }

        case '-chown': {
          if (positionalArgs.length < 2) return `chown: Usage: hdfs dfs -chown <owner[:group]> <path>`;
          const ownerPart = positionalArgs[0];
          const targetPath = positionalArgs[1];
          const node = this.nameNode.getNamespace().resolvePath(targetPath);
          if (!node) return `chown: \`${targetPath}\`: No such file or directory`;

          if (ownerPart.includes(':')) {
            const [o, g] = ownerPart.split(':');
            node.owner = o;
            node.group = g;
          } else {
            node.owner = ownerPart;
          }
          return `Changed owner of ${targetPath} to ${node.owner}:${node.group}`;
        }

        case '-put':
        case '-copyFromLocal': {
          if (positionalArgs.length < 2) return `put: Usage: hdfs dfs -put <localfile> <hdfspath>`;
          const localSrc = positionalArgs[0];
          const hdfsDst = positionalArgs[1];

          let contentToUpload = localSrc;
          const localContent = this.getLocalFileContent(localSrc);
          if (localContent !== undefined) {
            contentToUpload = localContent;
          }

          try {
            this.nameNode.createAndWriteFile(hdfsDst, contentToUpload);
            this.db.saveHdfsFileContent(hdfsDst, contentToUpload);
            return `✓ Copied from Local [${localSrc}] -> HDFS [${hdfsDst}] (3 replicas allocated across DataNodes)`;
          } catch (err: any) {
            return `put: ${err.message}`;
          }
        }

        case '-get':
        case '-copyToLocal': {
          if (positionalArgs.length < 2) return `get: Usage: hdfs dfs -get <hdfspath> <localfile>`;
          const hdfsSrc = positionalArgs[0];
          const localDst = positionalArgs[1];

          const hdfsContent = this.nameNode.readFileContent(hdfsSrc);
          if (hdfsContent === undefined) {
            return `get: \`${hdfsSrc}\`: No such file or directory in HDFS`;
          }

          this.saveLocalFileContent(localDst, hdfsContent);
          return `✓ Copied from HDFS [${hdfsSrc}] -> Local [${localDst}] successfully.`;
        }

        case '-cat': {
          if (positionalArgs.length === 0) return `cat: Missing file path argument`;
          const filePath = positionalArgs[0];
          const node = this.nameNode.getNamespace().resolvePath(filePath);
          if (!node || node.type !== 'FILE') return `cat: \`${filePath}\`: No such file`;

          const content = this.nameNode.readFileContent(filePath);
          return content !== undefined ? content : `hello hadoop\nhello world\nhadoop world\n`;
        }

        case '-tail': {
          if (positionalArgs.length === 0) return `tail: Missing file path argument`;
          const filePath = positionalArgs[0];
          const content = this.nameNode.readFileContent(filePath);
          if (content === undefined) return `tail: \`${filePath}\`: No such file`;
          const lines = content.split('\n');
          return lines.slice(-10).join('\n');
        }

        case '-head': {
          if (positionalArgs.length === 0) return `head: Missing file path argument`;
          const filePath = positionalArgs[0];
          const content = this.nameNode.readFileContent(filePath);
          if (content === undefined) return `head: \`${filePath}\`: No such file`;
          const lines = content.split('\n');
          return lines.slice(0, 10).join('\n');
        }

        case '-rm':
        case '-rmdir': {
          if (positionalArgs.length === 0) return `rm: Missing file path argument`;
          const targetPath = positionalArgs[0];
          const isRecursive = flags.has('-r') || action === '-rm';
          try {
            const success = this.nameNode.getNamespace().deletePath(targetPath, isRecursive);
            if (success) {
              this.db.deleteHdfsFileContent(targetPath);
            }
            return success ? `✓ Deleted ${targetPath}` : `rm: \`${targetPath}\`: No such file or directory`;
          } catch (err: any) {
            return `rm: ${err.message}`;
          }
        }

        case '-du': {
          const pathStr = positionalArgs[0] || '/';
          const node = this.nameNode.getNamespace().resolvePath(pathStr);
          if (!node) return `du: \`${pathStr}\`: No such file or directory`;
          const sz = node.type === 'FILE' ? node.sizeBytes : 1024;
          return `${sz}  ${sz * 3}  ${pathStr}`;
        }

        case '-df': {
          const dns = this.nameNode.getDataNodes();
          const totalCap = dns.reduce((acc, n) => acc + n.storageCapacityBytes, 0);
          const totalUsed = dns.reduce((acc, n) => acc + n.storageUsedBytes, 0);
          const avail = totalCap - totalUsed;
          return `Filesystem           Size       Used      Available  Use%\nfile:///         ${(totalCap / (1024*1024)).toFixed(1)}MB    ${(totalUsed / (1024*1024)).toFixed(1)}MB    ${(avail / (1024*1024)).toFixed(1)}MB     ${((totalUsed/totalCap)*100).toFixed(1)}%`;
        }

        case '-help':
        default: {
          return `HDFS Shell Commands:
  hdfs dfs -ls [-r] <path>             List HDFS directory contents
  hdfs dfs -find <path> -name <p>      Search files recursively in HDFS
  hdfs dfs -count <path>               Count dirs, files, and bytes in HDFS
  hdfs dfs -mkdir [-p] <path>         Create HDFS directory
  hdfs dfs -put <local> <hdfs>         Copy file from Local Linux FS -> HDFS
  hdfs dfs -get <hdfs> <local>         Copy file from HDFS -> Local Linux FS
  hdfs dfs -cp <src> <dst>             Copy file/directory within HDFS
  hdfs dfs -mv <src> <dst>             Move/rename file/directory within HDFS
  hdfs dfs -chmod <mode> <path>        Change HDFS file permissions (e.g. 755)
  hdfs dfs -chown <owner[:grp]> <path> Change HDFS file owner/group
  hdfs dfs -getfacl <path>             Inspect Granular ACL permissions
  hdfs dfs -setfacl -m user:alice:rwx  Set Granular ACL permissions
  hdfs dfs -checksum <path>            Compute file MD5 checksum
  hdfs dfs -expunge                    Empty HDFS trash (.Trash)
  hdfs dfs -allowSnapshot <dir>        Allow HDFS Snapshots
  hdfs dfs -createSnapshot <dir> <s1>  Create HDFS Snapshot
  hdfs dfs -deleteSnapshot <dir> <s1>  Delete HDFS Snapshot
  hdfs dfs -touchz <path>              Create empty 0-byte file in HDFS
  hdfs dfs -cat <path>                Display file contents
  hdfs dfs -tail / -head <path>        Display tail/head of file
  hdfs dfs -rm [-r] <path>            Remove file or directory
  hdfs dfs -du <path>                 Display disk usage
  hdfs dfs -df                        Display filesystem storage capacity`;
        }
      }
    }

    if (subcommand === 'fsck') {
      return this.runFsck(positionalArgs[0] || '/', flags);
    }

    if (subcommand === 'getconf') {
      return this.runGetconf(action, positionalArgs);
    }

    if (subcommand === 'dfsadmin') {
      if (action === '-refreshNodes') {
        return `[HDFS Admin] Re-reading dfs.hosts and dfs.hosts.exclude...
✓ DataNode membership & decommissioning list refreshed successfully.`;
      }

      if (action === '-metasave') {
        const fileArg = positionalArgs[0] || 'metasave.log';
        this.saveLocalFileContent(fileArg, `NameNode Metasave Report at ${new Date().toISOString()}
Live DataNodes: 3
Under-replicated blocks: 0
Corrupt blocks: 0
Replication Queues: Empty`);
        return `Created metasave report in local file ${fileArg}`;
      }

      if (action === '-triggerBlockReport') {
        const nodeArg = positionalArgs[0] || 'datanode1.hadoop.local';
        return `Triggering full block report on ${nodeArg}...
✓ Full block report sent to Active NameNode nn1.`;
      }

      if (action === '-enterMaintenance') {
        const nodeArg = positionalArgs[0] || 'datanode1.hadoop.local';
        return `[HDFS Admin] ${nodeArg} entering maintenance mode...
✓ DataNode ${nodeArg} placed in Maintenance Window. Re-replication suppressed.`;
      }

      if (action === '-exitMaintenance') {
        const nodeArg = positionalArgs[0] || 'datanode1.hadoop.local';
        return `✓ DataNode ${nodeArg} exited maintenance mode.`;
      }

      if (action === '-saveNamespace') {
        return `[SecondaryNameNode] Merging edits log into fsimage...
✓ Saved namespace image (fsimage_0000000000000000124) successfully.`;
      }

      if (action === '-setQuota') {
        const qCount = parseInt(positionalArgs[0]) || 1000;
        const targetPath = positionalArgs[1] || '/';
        this.fileQuotas.set(targetPath, qCount);
        return `Set file count quota ${qCount} on ${targetPath}`;
      }

      if (action === '-setSpaceQuota') {
        const sVal = positionalArgs[0] || '10g';
        const targetPath = positionalArgs[1] || '/';
        this.spaceQuotas.set(targetPath, sVal);
        return `Set space quota ${sVal} on ${targetPath}`;
      }

      if (action === '-clrSpaceQuota') {
        const targetPath = positionalArgs[0] || '/';
        this.spaceQuotas.delete(targetPath);
        return `Cleared space quota on ${targetPath}`;
      }

      if (action === '-report') {
        return this.dfsadminReport();
      }
      if (action === '-safemode') {
        const op = (positionalArgs[0] || 'get').toLowerCase();
        if (op === 'enter') {
          this.safeModeManual = true;
          this.scheduleMetaSave();
          return 'Safe mode is ON';
        }
        if (op === 'leave' || op === 'forceexit') {
          this.safeModeManual = false;
          this.scheduleMetaSave();
          return 'Safe mode is OFF';
        }
        return `Safe mode is ${this.safeModeManual ? 'ON' : 'OFF'}`;
      }
      if (action === '-printTopology') {
        const byRack = new Map<string, string[]>();
        this.nameNode.getDataNodes().forEach((d) => {
          if (!byRack.has(d.rackId)) byRack.set(d.rackId, []);
          byRack.get(d.rackId)!.push(`   127.0.0.1:9866 (${d.hostname}) ${d.state === 'RUNNING' ? 'In Service' : 'Dead'}`);
        });
        return [...byRack.entries()].sort().map(([r, ns]) => `Rack: ${r}\n${ns.join('\n')}\n`).join('\n');
      }
      return `Usage: hdfs dfsadmin
\t[-report [-live] [-dead]]
\t[-safemode <enter | leave | get | wait | forceExit>]
\t[-saveNamespace]
\t[-refreshNodes]
\t[-setQuota <quota> <dirname>...<dirname>]
\t[-setSpaceQuota <quota> <dirname>...<dirname>]
\t[-printTopology]
\t[-metasave filename]`;
    }

    if (subcommand === 'version') {
      return this.hadoopVersionText();
    }

    return `Unsupported hdfs subcommand: ${subcommand}`;
  }

  private hadoopVersionText(): string {
    return `Hadoop 3.3.6
Source code repository: git -r 1be78238728da9266a4f88195058f08fd012bf9c
Compiled by ubuntu on 2023-06-18T08:22Z
Compiled on platform linux-x86_64
Compiled with protoc 3.7.1
From source with checksum 5652179ad55f76cb287d9c633bb53bbd
This command was run using /home/Hacker/hadoop/share/hadoop/common/hadoop-common-3.3.6.jar`;
  }

  private formatNameNode(force: boolean): string {
    const root = this.nameNode.getNamespace().getRoot();
    const hasData = root.children.size > 0;
    const nameDir = this.getConf('dfs.namenode.name.dir', 'file:///home/Hacker/hadoop/data/dfs/name').replace(/^file:\/\//, '');
    if (hasData && !force) {
      return `Re-format filesystem in Storage Directory root= ${nameDir}; location= null ? (Y or N)
HDFS already holds data and formatting ERASES all of it.
To keep your files, simply run 'start-dfs.sh' (no format needed).
To erase everything and start fresh, run: hdfs namenode -format -force`;
    }
    if (hasData) {
      this.nameNode.listFilesUnder('/').forEach((f) => this.db.deleteHdfsFileContent(f.path));
      const dirs: string[] = [];
      const walk = (n: any) => {
        if (n.type === 'DIRECTORY') {
          if (n.path !== '/') dirs.push(n.path);
          n.children.forEach((c: any) => walk(c));
        }
      };
      walk(root);
      dirs.forEach((d) => this.db.deleteHdfsFileContent(d));
      this.nameNode.formatNamespace();
      this.scheduleMetaSave();
    }
    this.isNameNodeFormatted = true;
    this.persistShellState();
    const d = new Date();
    const two = (n: number) => String(n).padStart(2, '0');
    const ts = `${String(d.getFullYear()).slice(2)}/${two(d.getMonth() + 1)}/${two(d.getDate())} ${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
    const hex = (n: number) => Math.random().toString(16).slice(2, 2 + n).padEnd(n, '0');
    const cid = `CID-${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}`;
    return `${ts} INFO namenode.NameNode: STARTUP_MSG:
/************************************************************
STARTUP_MSG: Starting NameNode
STARTUP_MSG:   host = localhost/127.0.0.1
STARTUP_MSG:   args = [-format]
STARTUP_MSG:   version = 3.3.6
************************************************************/
${ts} INFO namenode.FSNamesystem: Formatting NameNode in ${nameDir}
${ts} INFO common.Storage: Storage directory ${nameDir} has been successfully formatted.
${ts} INFO namenode.FSImage: Creating initial FSImage checkpoint with Cluster ID: ${cid}
${ts} INFO namenode.NameNode: SHUTDOWN_MSG:
/************************************************************
SHUTDOWN_MSG: Shutting down NameNode at localhost/127.0.0.1
************************************************************/
✓ NameNode formatted successfully. Ready for 'start-dfs.sh'.`;
  }

  private runFsck(pathArg: string, flags: Set<string>): string {
    const path = normalizeHdfsPath(pathArg);
    const ns = this.nameNode.getNamespace();
    const start = new Date();
    const header = [
      `Connecting to namenode via http://localhost:9870/fsck?ugi=Hacker&path=${encodeURIComponent(path)}`,
      `FSCK started by Hacker (auth:SIMPLE) from /127.0.0.1 for path ${path} at ${start.toString()}`
    ];
    const target = ns.resolvePath(path);
    if (!target) {
      return [...header, `FSCK ended at ${new Date().toString()} in 1 milliseconds`, `Path '${path}' does not exist`, '', `Fsck on path '${path}' FAILED`].join('\n');
    }
    const showFiles = flags.has('-files');
    const showBlocks = flags.has('-blocks');
    const showLoc = flags.has('-locations');
    const showRacks = flags.has('-racks');
    const lines: string[] = [];
    let dirs = 0, files = 0, blocks = 0, size = 0, replicas = 0, under = 0, over = 0, missing = 0, corrupt = 0, minRepl = 0, missingReplicas = 0;
    const defaultRep = this.confInt('dfs.replication', 3);
    const dnById = new Map(this.nameNode.getDataNodes().map((d) => [d.id, d]));
    const walk = (n: any) => {
      if (n.type === 'DIRECTORY') {
        dirs++;
        if (showFiles) lines.push(`${n.path} <dir>`);
        [...n.children.values()].sort((a: any, b: any) => a.name.localeCompare(b.name)).forEach(walk);
        return;
      }
      files++;
      size += n.sizeBytes;
      const fileBlocks = this.nameNode.getBlocksOf(n.path);
      let fileStatus = 'OK';
      const blockLines: string[] = [];
      fileBlocks.forEach((b, i) => {
        blocks++;
        const live = b.replicas.filter((r) => dnById.get(r.nodeId)?.state === 'RUNNING');
        replicas += live.length;
        if (live.length >= 1) minRepl++;
        if (b.checksumStatus === 'CORRUPTED') {
          corrupt++;
          fileStatus = 'CORRUPT';
        }
        if (live.length === 0) {
          missing++;
          fileStatus = 'MISSING';
        } else if (live.length < n.replicationFactor) {
          under++;
          missingReplicas += n.replicationFactor - live.length;
          if (fileStatus === 'OK') fileStatus = `Under replicated ${b.id}. Target Replicas is ${n.replicationFactor} but found ${live.length} live replica(s)`;
        } else if (live.length > n.replicationFactor) over++;
        if (showBlocks) {
          const loc = showLoc || showRacks
            ? ' [' + live.map((r) => {
                const dn = dnById.get(r.nodeId);
                return `DatanodeInfoWithStorage[127.0.0.1:9866,DS-${r.nodeId},DISK]${showRacks ? ` ${dn?.rackId}/${dn?.hostname}` : ''}`;
              }).join(', ') + ']'
            : '';
          blockLines.push(`${i}. BP-1350457290-127.0.0.1-${this.clusterTimestamp}:${b.id}_${b.generationStamp} len=${b.sizeBytes} Live_repl=${live.length}${loc}`);
        }
      });
      if (showFiles) {
        lines.push(`${n.path} ${n.sizeBytes} bytes, replicated: replication=${n.replicationFactor}, ${fileBlocks.length} block(s):  ${fileStatus}`);
        lines.push(...blockLines);
        if (blockLines.length) lines.push('');
      }
    };
    walk(target);
    const pct = (x: number) => (blocks ? ((x / blocks) * 100).toFixed(1) : '0.0');
    const healthy = missing === 0 && corrupt === 0;
    const racks = new Set(this.nameNode.getDataNodes().map((d) => d.rackId)).size;
    const avgBlock = blocks ? Math.round(size / blocks) : 0;
    const status = healthy ? 'HEALTHY' : 'CORRUPT';
    const totalRepl = replicas + missingReplicas;
    const body = [
      '',
      `Status: ${status}`,
      ` Number of data-nodes:\t${this.nameNode.getDataNodes().filter((d) => d.state === 'RUNNING').length}`,
      ` Number of racks:\t\t${racks}`,
      ` Total dirs:\t\t\t${dirs}`,
      ` Total symlinks:\t\t0`,
      '',
      'Replicated Blocks:',
      ` Total size:\t${size} B`,
      ` Total files:\t${files}`,
      ` Total blocks (validated):\t${blocks} (avg. block size ${avgBlock} B)`,
      ` Minimally replicated blocks:\t${minRepl} (${pct(minRepl)} %)`,
      ` Over-replicated blocks:\t${over} (${pct(over)} %)`,
      ` Under-replicated blocks:\t${under} (${pct(under)} %)`,
      ` Mis-replicated blocks:\t\t0 (0.0 %)`,
      ` Default replication factor:\t${defaultRep}`,
      ` Average block replication:\t${blocks ? (replicas / blocks).toFixed(1) : '0.0'}`,
      ` Missing blocks:\t\t${missing}`,
      ` Corrupt blocks:\t\t${corrupt}`,
      ` Missing replicas:\t\t${missingReplicas} (${totalRepl ? ((missingReplicas / totalRepl) * 100).toFixed(1) : '0.0'} %)`,
      ` Blocks queued for replication:\t${under}`,
      `FSCK ended at ${new Date().toString()} in ${Math.max(1, Date.now() - start.getTime())} milliseconds`,
      '',
      `The filesystem under path '${path}' is ${status}`
    ];
    return [...header, ...lines, ...body].join('\n');
  }

  private dfsadminReport(): string {
    const dns = this.nameNode.getDataNodes();
    const cap = dns.reduce((a, n) => a + n.storageCapacityBytes, 0);
    const used = dns.reduce((a, n) => a + n.storageUsedBytes, 0);
    const live = dns.filter((d) => d.state === 'RUNNING');
    const dead = dns.filter((d) => d.state !== 'RUNNING');
    let under = 0;
    let missing = 0;
    this.nameNode.listFilesUnder('/').forEach((f) =>
      this.nameNode.getBlocksOf(f.path).forEach((b) => {
        const n = b.replicas.filter((r) => dns.find((d) => d.id === r.nodeId)?.state === 'RUNNING').length;
        if (n === 0) missing++;
        else if (n < f.replicationFactor) under++;
      })
    );
    const h = (n: number) => `${n} (${n < 1024 ? n + ' B' : humanSize(n) + 'B'})`;
    const pct = (a: number, b: number) => (b ? ((a / b) * 100).toFixed(2) : '0.00');
    const out = [
      `Configured Capacity: ${h(cap)}`,
      `Present Capacity: ${h(cap)}`,
      `DFS Remaining: ${h(cap - used)}`,
      `DFS Used: ${h(used)}`,
      `DFS Used%: ${pct(used, cap)}%`,
      `Replicated Blocks:`,
      `\tUnder replicated blocks: ${under}`,
      `\tBlocks with corrupt replicas: 0`,
      `\tMissing blocks: ${missing}`,
      `\tMissing blocks (with replication factor 1): 0`,
      `\tLow redundancy blocks with highest priority to recover: ${missing}`,
      `\tPending deletion blocks: 0`,
      '',
      '-------------------------------------------------',
      `Live datanodes (${live.length}):`,
      ''
    ];
    const describe = (dn: any) => {
      out.push(
        `Name: 127.0.0.1:9866 (${dn.hostname})`,
        `Hostname: ${dn.hostname}`,
        `Rack: ${dn.rackId}`,
        `Decommission Status : Normal`,
        `Configured Capacity: ${h(dn.storageCapacityBytes)}`,
        `DFS Used: ${h(dn.storageUsedBytes)}`,
        `Non DFS Used: 0 (0 B)`,
        `DFS Remaining: ${h(dn.storageCapacityBytes - dn.storageUsedBytes)}`,
        `DFS Used%: ${pct(dn.storageUsedBytes, dn.storageCapacityBytes)}%`,
        `DFS Remaining%: ${pct(dn.storageCapacityBytes - dn.storageUsedBytes, dn.storageCapacityBytes)}%`,
        `Num of Blocks: ${dn.blocks.length}`,
        `Last contact: ${new Date().toString()}`,
        ''
      );
    };
    live.forEach(describe);
    if (dead.length) {
      out.push(`Dead datanodes (${dead.length}):`, '');
      dead.forEach(describe);
    }
    return out.join('\n');
  }

  private runGetconf(action: string | undefined, args: string[]): string {
    switch (action) {
      case '-confKey':
        if (!args[0]) return 'Usage: hdfs getconf -confKey [key]';
        return this.getConf(args[0], '') || `Configuration ${args[0]} is missing.`;
      case '-namenodes':
      case '-secondaryNameNodes':
        return 'localhost';
      case '-backupNodes':
      case '-journalNodes':
        return '';
      case '-nnRpcAddresses':
        return this.getConf('fs.defaultFS', 'hdfs://localhost:9000').replace(/^hdfs:\/\//, '');
      default:
        return `hdfs getconf is utility for getting configuration information from the config file.

hadoop getconf
\t[-namenodes]\t\t\tgets list of namenodes in the cluster.
\t[-secondaryNameNodes]\t\tgets list of secondary namenodes in the cluster.
\t[-backupNodes]\t\t\tgets list of backup nodes in the cluster.
\t[-nnRpcAddresses]\t\tgets the namenode rpc addresses
\t[-confKey [key]]\t\tgets a specific key from the configuration`;
    }
  }

  /** Read every file under HDFS input paths (file, directory, comma list) like FileInputFormat. */
  private readJobInput(inputArgs: string[]): { texts: string[]; paths: string[] } | string {
    const texts: string[] = [];
    const paths: string[] = [];
    for (const arg of inputArgs) {
      for (const one of arg.split(',')) {
        const path = normalizeHdfsPath(one);
        const node = this.nameNode.getNamespace().resolvePath(path);
        if (!node) {
          return `Exception in thread "main" org.apache.hadoop.mapreduce.lib.input.InvalidInputException: Input path does not exist: hdfs://localhost:9000${path}`;
        }
        this.nameNode
          .listFilesUnder(path)
          .filter((f) => !f.name.startsWith('_') && !f.name.startsWith('.'))
          .sort((a, b) => a.path.localeCompare(b.path))
          .forEach((f) => {
            texts.push(this.nameNode.readFileContent(f.path) ?? '');
            paths.push(f.path);
          });
      }
    }
    return { texts, paths };
  }

  private jobLog(jobName: string, inputCount: number, result: JobResult, outputDir: string | null, numReduces = 1): string {
    this.jobSequence++;
    const seq = String(this.jobSequence).padStart(4, '0');
    const jobId = `job_${this.clusterTimestamp}_${seq}`;
    const appId = `application_${this.clusterTimestamp}_${seq}`;
    const startTime = Date.now();
    const stamp = () => {
      const t = new Date();
      const two = (n: number) => String(n).padStart(2, '0');
      return `${t.getFullYear()}-${two(t.getMonth() + 1)}-${two(t.getDate())} ${two(t.getHours())}:${two(t.getMinutes())}:${two(t.getSeconds())},${String(t.getMilliseconds()).padStart(3, '0')}`;
    };
    const c = result.counters;
    const splits = Math.max(1, inputCount);
    const lines = [
      `${stamp()} INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at localhost/127.0.0.1:8032`,
      `${stamp()} INFO input.FileInputFormat: Total input files to process : ${inputCount}`,
      `${stamp()} INFO mapreduce.JobSubmitter: number of splits:${splits}`,
      `${stamp()} INFO mapreduce.JobSubmitter: Submitting tokens for job: ${jobId}`,
      `${stamp()} INFO impl.YarnClientImpl: Submitted application ${appId}`,
      `${stamp()} INFO mapreduce.Job: The url to track the job: http://localhost:8088/proxy/${appId}/`,
      `${stamp()} INFO mapreduce.Job: Running job: ${jobId}`,
      `${stamp()} INFO mapreduce.Job: Job ${jobId} running in uber mode : false`,
      `${stamp()} INFO mapreduce.Job:  map 0% reduce 0%`,
      `${stamp()} INFO mapreduce.Job:  map 100% reduce 0%`,
      `${stamp()} INFO mapreduce.Job:  map 100% reduce 100%`,
      `${stamp()} INFO mapreduce.Job: Job ${jobId} completed successfully`,
      `${stamp()} INFO mapreduce.Job: Counters: 54`,
      `\tFile System Counters`,
      `\t\tHDFS: Number of bytes read=${c.bytesRead}`,
      `\t\tHDFS: Number of bytes written=${c.bytesWritten}`,
      `\t\tHDFS: Number of read operations=${3 * splits + 3}`,
      `\t\tHDFS: Number of write operations=${outputDir ? 2 : 0}`,
      `\tJob Counters `,
      `\t\tLaunched map tasks=${splits}`,
      `\t\tLaunched reduce tasks=${numReduces}`,
      `\t\tData-local map tasks=${splits}`,
      `\tMap-Reduce Framework`,
      `\t\tMap input records=${c.mapInputRecords}`,
      `\t\tMap output records=${c.mapOutputRecords}`,
      `\t\tReduce input groups=${c.reduceInputGroups}`,
      `\t\tReduce output records=${c.reduceOutputRecords}`,
      `\t\tShuffled Maps =${splits * numReduces}`,
      `\t\tFailed Shuffles=0`,
      `\tFile Input Format Counters `,
      `\t\tBytes Read=${c.bytesRead}`,
      `\tFile Output Format Counters `,
      `\t\tBytes Written=${c.bytesWritten}`
    ];
    if (result.summary) lines.push(result.summary);
    if (outputDir) lines.push(`✓ Output written to HDFS: ${outputDir}/part-r-00000  (view it: hdfs dfs -cat ${outputDir}/part-r-00000)`);
    this.finishedJobs.push({ jobId, appId, name: jobName, user: 'Hacker', queue: 'default', state: 'SUCCEEDED', startTime, finishTime: Date.now(), output: outputDir || '' });
    if (this.finishedJobs.length > 200) this.finishedJobs.shift();
    return lines.join('\n');
  }

  private writeJobOutput(outputDir: string, content: string): void {
    const rep = this.confInt('dfs.replication', 3);
    const bs = this.confInt('dfs.blocksize', 134217728);
    this.nameNode.getNamespace().mkdir(outputDir, true);
    this.db.saveHdfsFileContent(outputDir, '__HADOOP_DIR__');
    this.nameNode.createAndWriteFile(`${outputDir}/_SUCCESS`, '', rep, bs);
    this.db.saveHdfsFileContent(`${outputDir}/_SUCCESS`, '');
    this.nameNode.createAndWriteFile(`${outputDir}/part-r-00000`, content, rep, bs);
    this.db.saveHdfsFileContent(`${outputDir}/part-r-00000`, content);
    this.scheduleMetaSave();
  }

  private checkOutputDir(outArg: string | undefined): string | { dir: string } {
    if (!outArg) return 'Usage: output directory missing';
    const dir = normalizeHdfsPath(outArg);
    if (this.nameNode.getNamespace().resolvePath(dir)) {
      return `Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://localhost:9000${dir} already exists
Hint: remove it first with  hdfs dfs -rm -r ${dir}   or use a new output directory.`;
    }
    return { dir };
  }

  /** hadoop jar / yarn jar */
  private runJar(args: string[], raw: string): string {
    if (args.length === 0) return 'RunJar jarFile [mainClass] args...';
    if (!this.isHDFSStarted) return `java.net.ConnectException: Call From localhost/127.0.0.1 to localhost:9000 failed on connection exception: Connection refused\nRun 'start-dfs.sh' first.`;
    if (this.safeModeManual) return 'org.apache.hadoop.hdfs.server.namenode.SafeModeException: Name node is in safe mode.';
    const jar = args[0];
    if (/streaming/i.test(jar)) return this.runStreaming(raw);
    let rest = args.slice(1);
    if (rest[0] && /^org\.apache\.hadoop\.examples\./.test(rest[0])) rest = [rest[0].split('.').pop()!.toLowerCase(), ...rest.slice(1)];
    let program = (rest[0] || '').toLowerCase();
    if (!/examples/i.test(jar) && /wordcount/i.test(program)) program = 'wordcount';
    if (program === 'quasimontecarlo') program = 'pi';
    const progArgs = rest.slice(1);
    const usage = `An example program must be given as the first argument.
Valid program names are:
  grep: A map/reduce program that counts the matches of a regex in the input.
  pi: A map/reduce program that estimates Pi using a quasi-Monte Carlo method.
  sort: A map/reduce program that sorts the data written by the random writer.
  wordcount: A map/reduce program that counts the words in the input files.
  wordmean: A map/reduce program that counts the average length of the words in the input files.`;
    if (!program) return usage;
    switch (program) {
      case 'wordcount':
      case 'wordmean':
      case 'sort': {
        if (progArgs.length < 2) return `Usage: ${program} <in> [<in>...] <out>`;
        const out = this.checkOutputDir(progArgs[progArgs.length - 1]);
        if (typeof out === 'string') return out;
        const input = this.readJobInput(progArgs.slice(0, -1));
        if (typeof input === 'string') return input;
        const result = program === 'wordcount' ? wordCount(input.texts) : program === 'wordmean' ? wordMean(input.texts) : sortLines(input.texts);
        this.writeJobOutput(out.dir, result.output);
        return this.jobLog(program === 'wordcount' ? 'word count' : program, input.paths.length, result, out.dir);
      }
      case 'grep': {
        if (progArgs.length < 3) return 'Grep <inDir> <outDir> <regex> [<group>]';
        const out = this.checkOutputDir(progArgs[1]);
        if (typeof out === 'string') return out;
        const input = this.readJobInput([progArgs[0]]);
        if (typeof input === 'string') return input;
        try {
          const result = grepJob(input.texts, progArgs[2], parseInt(progArgs[3] || '0', 10) || 0);
          this.writeJobOutput(out.dir, result.output);
          return this.jobLog('grep-sort', input.paths.length, result, out.dir);
        } catch (err: any) {
          return `Exception in thread "main" ${err.message}`;
        }
      }
      case 'pi': {
        const maps = parseInt(progArgs[0], 10);
        const samples = parseInt(progArgs[1], 10);
        if (!(maps > 0) || !(samples > 0)) return 'Usage: org.apache.hadoop.examples.QuasiMonteCarlo <nMaps> <nSamples>';
        if (maps * samples > 50000000) return 'This simulator caps pi at 50,000,000 total samples so your browser stays responsive.';
        const result = piEstimate(maps, samples);
        const wrote = Array.from({ length: Math.min(maps, 20) }, (_, i) => `Wrote input for Map #${i}`).join('\n');
        return `Number of Maps  = ${maps}\nSamples per Map = ${samples}\n${wrote}\nStarting Job\n${this.jobLog('QuasiMonteCarlo', maps, result, null)}`;
      }
      default:
        if (/examples/i.test(jar)) return `Unknown program '${rest[0]}' chosen.\n${usage}`;
        return `Exception in thread "main" java.lang.ClassNotFoundException: ${rest[0] || jar}
This simulator cannot run compiled Java classes. Use the examples jar
  (hadoop jar hadoop-mapreduce-examples.jar wordcount <in> <out>)
or Hadoop Streaming with Python:
  hadoop jar hadoop-streaming.jar -input <in> -output <out> -mapper "python3 mapper.py" -reducer "python3 reducer.py"`;
    }
  }

  /** Hadoop Streaming with Python (or cat / wc / sort / uniq) mappers and reducers. */
  private runStreaming(raw: string): string {
    const tokens = (raw.match(/"[^"]*"|'[^']*'|\S+/g) || []).map((t) => t.replace(/^["']|["']$/g, ''));
    const opt = (name: string) => {
      const vals: string[] = [];
      tokens.forEach((t, i) => {
        if (t === name && tokens[i + 1] !== undefined) vals.push(tokens[i + 1]);
      });
      return vals;
    };
    const inputs = opt('-input');
    const outArg = opt('-output')[0];
    const mapper = opt('-mapper')[0] || 'cat';
    const reducer = opt('-reducer')[0];
    if (!inputs.length || !outArg) return 'Usage: hadoop jar hadoop-streaming.jar -input <in> -output <out> -mapper <cmd> [-reducer <cmd>] [-file <f>]';
    const out = this.checkOutputDir(outArg);
    if (typeof out === 'string') return out;
    const input = this.readJobInput(inputs);
    if (typeof input === 'string') return input;
    const runStage = (command: string, stdin: string): { out: string } | { err: string } => {
      const parts = command.trim().split(/\s+/);
      const exe = parts[0];
      if (exe === 'cat' || exe === '/bin/cat') return { out: stdin };
      if (exe === 'wc' || exe === '/usr/bin/wc') {
        const lines = stdin.split('\n').length - (stdin.endsWith('\n') ? 1 : 0);
        const words = stdin.split(/\s+/).filter(Boolean).length;
        if (parts.includes('-l')) return { out: `${lines}\n` };
        if (parts.includes('-w')) return { out: `${words}\n` };
        return { out: `${lines} ${words} ${new TextEncoder().encode(stdin).length}\n` };
      }
      if (exe === 'sort') return { out: streamingShuffle(stdin) };
      if (exe === 'uniq') {
        const ls = stdin.split('\n').filter((l) => l !== '');
        const res = ls.filter((l, i) => i === 0 || l !== ls[i - 1]);
        return { out: res.join('\n') + (res.length ? '\n' : '') };
      }
      let script = exe;
      let scriptArgs = parts.slice(1);
      if (/^python3?$/.test(exe)) {
        script = parts[1];
        scriptArgs = parts.slice(2);
      }
      const code = this.getLocalFileContent(script);
      if (code === undefined) return { err: `Cannot run program "${command}": error=2, No such file or directory (create ${script} with vim first)` };
      const res = this.pythonEngine.runWithStdin(code, stdin, [script, ...scriptArgs], this.localFiles);
      if (!res) return { err: 'Python is still loading. Wait a few seconds and run the job again.' };
      if (res.exitCode !== 0) return { err: `PipeMapRed.waitOutputThreads(): subprocess failed with code 1\n${res.stderr}` };
      return { out: res.stdout };
    };
    const mapRes = runStage(mapper, input.texts.join(''));
    if ('err' in mapRes) return `ERROR streaming.StreamJob: Job not successful!\n${mapRes.err}`;
    const shuffled = streamingShuffle(mapRes.out);
    let finalOut = shuffled;
    if (reducer && reducer !== 'NONE') {
      const redRes = runStage(reducer, shuffled);
      if ('err' in redRes) return `ERROR streaming.StreamJob: Job not successful!\n${redRes.err}`;
      finalOut = redRes.out;
    }
    this.writeJobOutput(out.dir, finalOut);
    const allInput = input.texts.join('');
    const result: JobResult = {
      output: finalOut,
      counters: {
        mapInputRecords: allInput.split('\n').filter((l) => l !== '').length,
        mapOutputRecords: mapRes.out.split('\n').filter(Boolean).length,
        reduceInputGroups: new Set(shuffled.split('\n').filter(Boolean).map((l) => l.split('\t')[0])).size,
        reduceOutputRecords: finalOut.split('\n').filter(Boolean).length,
        bytesRead: new TextEncoder().encode(allInput).length,
        bytesWritten: new TextEncoder().encode(finalOut).length
      }
    };
    return `packageJobJar: [] [/home/Hacker/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar] /tmp/streamjob.jar tmpDir=null\n${this.jobLog('streamjob', input.paths.length, result, out.dir)}\nINFO streaming.StreamJob: Output directory: ${out.dir}`;
  }

  private mapredCommand(cmd: ParsedCommand): string {
    if (cmd.subcommand === 'job') {
      const action = cmd.action || '';
      if (action === '-list') {
        const all = cmd.positionalArgs[0] === 'all';
        const jobs = all ? this.finishedJobs : [];
        const lines = [`Total jobs:${jobs.length}`, `                  JobId\t     JobName\t     State\t     StartTime\t    UserName\t       Queue\t    Priority`];
        jobs.forEach((j) => lines.push(` ${j.jobId}\t  ${j.name}\t ${j.state}\t${j.startTime}\t${j.user}\t${j.queue}\tDEFAULT`));
        if (!all && this.finishedJobs.length) lines.push(`(${this.finishedJobs.length} finished job(s) hidden; use: mapred job -list all)`);
        return lines.join('\n');
      }
      if (action === '-status') {
        const j = this.finishedJobs.find((x) => x.jobId === cmd.positionalArgs[0]);
        if (!j) return `Could not find job ${cmd.positionalArgs[0] || ''}`;
        return `Job: ${j.jobId}\nJob File: hdfs://localhost:9000/tmp/hadoop-yarn/staging/Hacker/.staging/${j.jobId}/job.xml\nJob Tracking URL : http://localhost:19888/jobhistory/job/${j.jobId}\nUber job : false\nNumber of maps: 1\nNumber of reduces: 1\nmap() completion: 1.0\nreduce() completion: 1.0\nJob state: ${j.state}\nretired: false\nreason for failure: \nOutput: ${j.output}`;
      }
      if (action === '-kill') return `Could not find job ${cmd.positionalArgs[0] || ''} (it has already finished)`;
      return 'Usage: mapred job [-list [all]] [-status <job-id>] [-kill <job-id>]';
    }
    if (cmd.subcommand === 'version') return this.hadoopVersionText();
    return `Usage: mapred [--config confdir] COMMAND
  job          manipulate MapReduce jobs (mapred job -list all, mapred job -status <id>)
  version      print the version
  --daemon start historyserver   start the JobHistory server`;
  }

  /** hdfs/yarn/mapred --daemon start|stop <name>, hadoop-daemon.sh, mr-jobhistory-daemon.sh */
  private runDaemonCommand(op: string, name: string): string {
    const daemon = (name || '').toLowerCase();
    const start = op === 'start';
    if (op === 'status') {
      const up = ['namenode', 'datanode', 'secondarynamenode'].includes(daemon) ? this.isHDFSStarted
        : ['resourcemanager', 'nodemanager'].includes(daemon) ? this.isYARNStarted
        : daemon === 'historyserver' ? this.historyServerRunning : false;
      return up ? `${daemon} is running as process ${18000 + daemon.length * 37}.` : `${daemon} is not running.`;
    }
    if (op !== 'start' && op !== 'stop') return 'Usage: --daemon (start|status|stop) <daemon>';
    if (['namenode', 'datanode', 'secondarynamenode'].includes(daemon)) {
      if (start && !this.isNameNodeFormatted) return `Error: NameNode is not formatted! Please run 'hdfs namenode -format' first.`;
      this.isHDFSStarted = start;
      this.persistShellState();
      return `✓ ${start ? 'Started' : 'Stopped'} ${daemon} (HDFS daemons are started/stopped together in this simulator).`;
    }
    if (['resourcemanager', 'nodemanager'].includes(daemon)) {
      this.isYARNStarted = start;
      this.persistShellState();
      return `✓ ${start ? 'Started' : 'Stopped'} ${daemon} (YARN daemons are started/stopped together in this simulator).`;
    }
    if (daemon === 'historyserver') {
      this.historyServerRunning = start;
      return start
        ? 'starting historyserver, logging to /home/Hacker/hadoop/logs/mapred-Hacker-historyserver-localhost.out\n✓ JobHistoryServer started (web UI port 19888).'
        : 'stopping historyserver\n✓ JobHistoryServer stopped.';
    }
    return `ERROR: Cannot find daemon '${name}'. Known: namenode, datanode, secondarynamenode, resourcemanager, nodemanager, historyserver`;
  }

  private executeHadoopCommand(cmd: ParsedCommand): string {
    if (cmd.subcommand === 'distcp') {
      const src = cmd.positionalArgs[0] || 'hdfs://nn1/src';
      const dst = cmd.positionalArgs[1] || 'hdfs://nn2/dst';
      return `[DistCp Engine] Submitting MapReduce DistCp job across virtual network links...
[MapReduce DistCp] 4 Map tasks copying data chunks from [${src}] to [${dst}]
✓ Inter-cluster dataset replication completed successfully.`;
    }

    if (cmd.subcommand === 'version') {
      return this.hadoopVersionText();
    }

    if (cmd.subcommand === 'jar') {
      return this.runJar([...cmd.positionalArgs], cmd.raw);
    }

    if (cmd.subcommand === 'classpath') {
      return '/home/Hacker/hadoop/etc/hadoop:/home/Hacker/hadoop/share/hadoop/common/lib/*:/home/Hacker/hadoop/share/hadoop/common/*:/home/Hacker/hadoop/share/hadoop/hdfs/*:/home/Hacker/hadoop/share/hadoop/mapreduce/*:/home/Hacker/hadoop/share/hadoop/yarn/*';
    }

    if (cmd.subcommand === 'checknative') {
      return 'Native library checking:\nhadoop:  false\nzlib:    false\nzstd  :  false\nsnappy:  false\nlz4:     false\nbzip2:   false\nopenssl: false\n(Browser simulator: native libraries are not used.)';
    }

    if (cmd.subcommand === '__legacy_jar__') {
      if (cmd.positionalArgs.length < 3) {
        return `Usage: hadoop jar <jarfile> [mainclass] <input> <output>`;
      }
      const jarName = cmd.positionalArgs[0];

      if (jarName.includes('streaming')) {
        return `[Hadoop Streaming Engine] Executing Polyglot MapReduce Pipeline...
[Map Phase] Executing Node.js / Polyglot mapper script...
[Shuffle & Sort] Partitioning intermediate key-value pairs...
[Reduce Phase] Executing Node.js / Polyglot reducer script...
✓ Hadoop Streaming Job Completed Successfully. Output written to HDFS.`;
      }

      const inputPath = cmd.positionalArgs[1];
      const outputPath = cmd.positionalArgs[2];

      try {
        const job = this.mapReduceEngine.submitJob(`WordCount_${Date.now()}`, inputPath, outputPath);
        return `Submitted MapReduce Job ${job.id} (${job.name}) to YARN queue '${job.queue}'
✓ Container allocated
✓ Mappers running
✓ Shuffle & Sort active
✓ Reducers processing
✓ Output written to HDFS: ${outputPath}/part-r-00000`;
      } catch (err: any) {
        return `Hadoop Jar Error: ${err.message}`;
      }
    }

    return `Usage: hadoop [OPTIONS] SUBCOMMAND [SUBCOMMAND OPTIONS]
  fs / dfs     run a generic filesystem user client (same as hdfs dfs)
  jar <jar>    run a jar file (hadoop-mapreduce-examples.jar wordcount|grep|pi|wordmean|sort, hadoop-streaming.jar)
  distcp       copy file or directories recursively
  version      print the version
  classpath    prints the class path needed to get the Hadoop jar
  checknative  check native Hadoop and compression libraries availability`;
  }

  private executeYARNCommand(cmd: ParsedCommand): string {
    if (cmd.subcommand === 'scm') {
      return `[YARN Shared Cache Manager] Cleaning expired dependency artifacts from shared cache...
✓ Reclaimed 512 MB from YARN shared cache.`;
    }

    if (cmd.subcommand === 'rmadmin') {
      if (cmd.action === '-refreshSuperUserGroupsConfiguration') {
        return `[YARN RMAdmin] Re-reading core-site.xml superuser and proxyuser group permissions...
✓ Superuser proxy group ACL configuration refreshed.`;
      }
      if (cmd.action === '-refreshNodes') {
        return `[YARN RMAdmin] Re-reading include/exclude host files...
✓ NodeManager membership refreshed successfully.`;
      }
    }

    if (cmd.subcommand === 'container') {
      const attId = cmd.positionalArgs[0] || 'appattempt_1700000000_0001_000001';
      return `Total Containers for ${attId} : 2
Container-Id                            Node-Id                     Node-Http-Address    LOG-PATH
container_e01_1700000000_0001_01_000001  datanode1.hadoop.local:8041 datanode1:8042       /tmp/logs/container_01
container_e01_1700000000_0001_01_000002  datanode2.hadoop.local:8041 datanode2:8042       /tmp/logs/container_02`;
    }

    if (cmd.subcommand === 'applicationattempt') {
      const appId = cmd.positionalArgs[0] || 'application_1700000000_0001';
      return `Total Application Attempts for ${appId} : 1
ApplicationAttempt-Id               State      AM-Container-Id                            Node-Id
appattempt_1700000000_0001_000001  FINISHED   container_e01_1700000000_0001_01_000001   datanode1.hadoop.local:8041`;
    }

    if (cmd.subcommand === 'application') {
      if (cmd.action === '-kill') {
        const appId = cmd.positionalArgs[0] || 'application_1700000000_0001';
        return `Killing application ${appId}...
✓ Application ${appId} killed by user Hacker.`;
      }

      if (cmd.action === '-list') {
        // Like YARN: without -appStates only SUBMITTED/ACCEPTED/RUNNING apps are listed.
        const statesArg = (cmd.positionalArgs.find((a) => /^[A-Z_,]+$/.test(a)) || '').toUpperCase();
        const wantAll = statesArg.includes('ALL') || statesArg.includes('FINISHED');
        const apps = wantAll ? this.finishedJobs : [];
        const states = wantAll ? (statesArg.includes('ALL') ? '[NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED]' : '[FINISHED]') : '[SUBMITTED, ACCEPTED, RUNNING]';
        const lines = [
          `Total number of applications (application-types: [], states: ${states} and tags: []):${apps.length}`,
          `                Application-Id\t    Application-Name\t    Application-Type\t      User\t     Queue\t             State\t       Final-State\t       Progress\t                       Tracking-URL`
        ];
        apps.forEach((j) => {
          lines.push(`${j.appId}\t          ${j.name}\t           MAPREDUCE\t    ${j.user}\t   ${j.queue}\t          FINISHED\t         ${j.state}\t           100%\thttp://localhost:19888/jobhistory/job/${j.jobId}`);
        });
        if (!wantAll && this.finishedJobs.length) lines.push(`(${this.finishedJobs.length} finished application(s) hidden; use: yarn application -list -appStates ALL)`);
        return lines.join('\n');
      }
    }

    if (cmd.subcommand === 'node') {
      if (cmd.action === '-list') {
        if (cmd.raw.includes('DECOMMISSIONED')) {
          return `Total Decommissioned Nodes: 0`;
        }
        return `Total Nodes: 3
Node-Id                        Node-State Node-Http-Address     Number-of-Running-Containers
datanode1.hadoop.local:8041   RUNNING    datanode1:8042        2
datanode2.hadoop.local:8041   RUNNING    datanode2:8042        1
datanode3.hadoop.local:8041   RUNNING    datanode3:8042        1`;
      }
      if (cmd.action === '-status') {
        const nodeArg = cmd.positionalArgs[0] || 'datanode1.hadoop.local';
        return `Node Report:
Node-Id : ${nodeArg}:8041
Rack : /rack-01
Node-State : RUNNING
Memory-Used : 2048MB
Memory-Capacity : 8192MB
vCores-Used : 2
vCores-Capacity : 8`;
      }
    }

    if (cmd.subcommand === 'queue') {
      const qName = cmd.positionalArgs[1] || 'root.default';
      return `Queue Name: ${qName}
State: RUNNING
Capacity: 50.0%
Absolute Capacity: 50.0%
Current Capacity: 0.0%
Maximum Capacity: 100.0%
Num Applications: 0`;
    }

    if (cmd.subcommand === 'logs') {
      const appIdArg = cmd.positionalArgs[0] || 'application_1700000000_0001';
      return `================================================================================
YARN CONTAINER LOG ENGINE - APP ID: ${appIdArg}
================================================================================
Container: container_e01_1700000000_0001_01_000001 on datanode1.hadoop.local
================================================================================
LogType: stdout
Log Upload Time: ${new Date().toISOString()}
Log Length: 512 B

[Mapper Task 0] Read InputSplit (file /Hacker/about.txt) -> Output: 342 key-value pairs
[Mapper Task 1] Read InputSplit (file /Hacker/about.txt) -> Output: 289 key-value pairs
[Shuffle] Transferred 631 map output records across VirtualNetwork links (Rack distance: 1)
[Reducer Task 0] Sort & Merge complete. Reducing keys...
[Reducer Task 0] Successfully written output to HDFS /output/part-r-00000
================================================================================
LogType: stderr
[INFO] Job completed successfully with exit code 0.`;
    }

    return `YARN CLI commands:\n  yarn application -list\n  yarn application -kill <id>\n  yarn applicationattempt -list <id>\n  yarn container -list <attemptId>\n  yarn logs -applicationId <id>\n  yarn queue -status <queue>\n  yarn node -list\n  yarn scm -run\n  yarn rmadmin -refreshNodes`;
  }

  private resolveLocalPath(pathStr: string): string {
    if (!pathStr || pathStr === '~') return '/home/Hacker';
    if (pathStr.startsWith('~/')) pathStr = '/home/Hacker/' + pathStr.slice(2);

    const raw = pathStr.startsWith('/') ? pathStr : `${this.localDir}/${pathStr}`;
    const parts = raw.split('/').filter(Boolean);
    const stack: string[] = [];

    for (const part of parts) {
      if (part === '.') continue;
      if (part === '..') {
        if (stack.length > 0) stack.pop();
      } else {
        stack.push(part);
      }
    }

    return '/' + stack.join('/');
  }

  /** Load the Python runtime before running a script (the UI awaits this). */
  public async preparePython(): Promise<void> {
    await this.pythonEngine.initPyodide();
  }

  private executePython(code: string, inputs: string[], scriptName: string = '<string>'): string {
    const onSaveFile = (filePath: string, content: string) => {
      this.saveLocalFileContent(filePath, content);
    };

    if (this.pythonEngine) {
      const pyodideRes = this.pythonEngine.runSync(code, inputs, this.localFiles, onSaveFile, scriptName, [scriptName, ...inputs]);
      if (pyodideRes) {
        return pyodideRes.stderr ? `${pyodideRes.stdout}\n${pyodideRes.stderr}`.trim() : pyodideRes.stdout;
      }
      const fallbackRes = this.pythonEngine.fallbackExecuteSync(code, inputs, this.localFiles, onSaveFile);
      if (fallbackRes) {
        return fallbackRes.stderr ? `${fallbackRes.stdout}\n${fallbackRes.stderr}`.trim() : fallbackRes.stdout;
      }
    }
    return this.executePythonSync(code, inputs);
  }

  private executePythonSync(code: string, inputs: string[]): string {
    const logs: string[] = [];
    let inputIdx = 0;
    let needInputPrompt: string | null = null;

    const mockInput = (promptMsg: string = ''): string => {
      if (inputIdx < inputs.length) {
        const val = inputs[inputIdx++];
        if (promptMsg) logs.push(`${promptMsg}${val}`);
        return val;
      }
      needInputPrompt = promptMsg || 'Enter input: ';
      throw new Error('__NEED_INPUT__');
    };

    try {
      const jsCode = transpilePythonToJS(code);

      const pyPrint = (...args: any[]) => {
        const line = args.map((a) => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ');
        logs.push(line);
      };

      const pyInput = (promptStr: string = ''): string => {
        return mockInput(promptStr);
      };

      const pyOpen = (path: string, mode: string = 'r') => {
        const resolvedPath = this.resolveLocalPath(path);
        const basename = path.split('/').pop() || path;

        if (mode.includes('w') || mode.includes('a')) {
          let fileData = mode.includes('a') ? (this.getLocalFileContent(resolvedPath) || '') : '';
          return {
            write: (str: string) => {
              fileData += String(str);
              this.saveLocalFileContent(resolvedPath, fileData);
            },
            writelines: (lines: string[]) => {
              fileData += lines.join('');
              this.saveLocalFileContent(resolvedPath, fileData);
            },
            read: () => fileData,
            close: () => {},
            __enter__: function() { return this; },
            __exit__: function() {}
          };
        } else {
          const content = this.getLocalFileContent(resolvedPath) ?? this.getLocalFileContent(basename) ?? '';
          return {
            read: () => content,
            readline: () => content.split('\n')[0] || '',
            readlines: () => content.split('\n'),
            write: () => {},
            close: () => {},
            __enter__: function() { return this; },
            __exit__: function() {}
          };
        }
      };

      const pyImport = (moduleName: string) => {
        const modFile = `${moduleName}.py`;
        const content = this.getLocalFileContent(modFile) || this.getLocalFileContent(`/home/Hacker/${modFile}`);
        if (content === undefined) {
          throw new Error(`No module named '${moduleName}'`);
        }
        const modSandbox: any = {
          Math, JSON, parseInt, parseFloat, String, Number, Array, Object,
          True: true, False: false, None: null
        };
        const modJs = transpilePythonToJS(content);
        const fn = new Function(...Object.keys(modSandbox), modJs);
        fn(...Object.values(modSandbox));
        return modSandbox;
      };

      const sandbox = {
        __py_print__: pyPrint,
        __py_input__: pyInput,
        __py_open__: pyOpen,
        __py_import__: pyImport,
        Math,
        JSON,
        parseInt,
        parseFloat,
        String,
        Number,
        Array,
        Object,
        True: true,
        False: false,
        None: null,
        len: (obj: any) => (obj && obj.length !== undefined ? obj.length : 0),
        range: (start: number, stop?: number, step: number = 1) => {
          if (stop === undefined) {
            stop = start;
            start = 0;
          }
          const arr = [];
          for (let i = start; i < stop; i += step) arr.push(i);
          return arr;
        },
        str: (v: any) => String(v),
        int: (v: any) => parseInt(v, 10) || 0,
        float: (v: any) => parseFloat(v) || 0.0,
        list: (v: any) => Array.from(v || [])
      };

      const fnKeys = Object.keys(sandbox);
      const fnVals = Object.values(sandbox);

      const fn = new Function(...fnKeys, jsCode);
      fn(...fnVals);

      return logs.length > 0 ? logs.join('\n') : '';
    } catch (err: any) {
      if (err && err.message === '__NEED_INPUT__') {
        return `__NEED_INPUT__:${needInputPrompt || 'Enter input: '}`;
      }
      return logs.length > 0
        ? `${logs.join('\n')}\nPython SyntaxError / RuntimeError: ${err.message}`
        : `Python SyntaxError / RuntimeError: ${err.message}`;
    }
  }

  private getGeneralHelpText(): string {
    return `Hadoop Practice Laboratory Terminal Help:
Hadoop Daemon Controls:
  start-dfs.sh / start-all.sh   Start HDFS & YARN cluster daemons
  stop-dfs.sh / stop-all.sh     Stop cluster daemons

Linux Commands & Text Editor:
  ls, pwd, cd, mkdir, rmdir, echo, cat, touch, rm, whoami
  grep <pattern> <file>         Search text in local files
  wc -l <file>                  Count lines/words/chars
  head / tail <file>            Display start/end of file
  ps -ef                        Display process listing
  kill -9 <PID> / killall java  Terminate processes or daemons
  ifconfig, ping, netstat -tuln Network Diagnostics & Listening Ports
  top, df -h, free -m, uname -a Linux System Monitoring Tools
  history                       Display command history
  vim <file> / vi <file>        Open interactive Vim text editor (:wq to save & exit)

Node.js & Python Engines & Polyglot Hadoop Streaming:
  python / python3               Open interactive Python REPL (>>> prompt)
  python script.py / python3    Execute Python file (Pyodide Wasm Engine + input() support)
  python -c "print('hello')"    Evaluate Python code inline
  python -v                     Display Python runtime version
  node -v                       Display Node.js runtime version
  node script.js                Execute JavaScript file
  node -e "console.log('test')" Evaluate JavaScript code inline
  hadoop jar streaming.jar -mapper "python mapper.py" -reducer "python reducer.py"

HDFS Operations (Local <-> HDFS & Inter-HDFS):
  hdfs dfs -put <local> <hdfs>         Copy file from Local Linux FS -> HDFS
  hdfs dfs -get <hdfs> <local>         Copy file from HDFS -> Local Linux FS
  hdfs dfs -cp <src> <dst>             Copy file/directory within HDFS
  hdfs dfs -mv <src> <dst>             Move/rename file/directory within HDFS
  hdfs dfs -chmod <mode> <path>        Change HDFS file permissions (e.g. 755)
  hdfs dfs -chown <owner[:grp]> <path> Change HDFS file owner/group
  hdfs dfs -find <path> -name <p>      Search files recursively in HDFS
  hdfs dfs -count <path>               Count dirs, files, and bytes in HDFS
  hdfs dfs -checksum <path>            Compute file MD5 checksum
  hdfs fsck /file -files -blocks -locations Inspect HDFS Block IDs & DataNodes
  hdfs dfsadmin -triggerBlockReport dn Force Full Block Report to NameNode
  hdfs dfsadmin -metasave metasave.log Dump NameNode Memory State
  hdfs dfsadmin -refreshNodes          Refresh DataNode membership
  hdfs storagepolicies -isSatisfierRunning Check Storage Policy Satisfier Daemon State
  hdfs ec -enablePolicy / -disablePolicy Enable/disable Erasure Coding policies
  hdfs dfs -getfacl / -setfacl         Inspect & set Granular ACL permissions
  hdfs dfs -expunge                    Empty HDFS trash (.Trash)
  hadoop distcp <src> <dst>            Distributed Copy across clusters
  hdfs dfsadmin -setQuota 1000 /dir    Set file count quota
  hdfs dfsadmin -setSpaceQuota 10g     Set space quota
  hdfs dfsadmin -saveNamespace         Save NameNode fsimage checkpoint
  hdfs dfsadmin -enterMaintenance dn   DataNode Maintenance Window
  hdfs crypto -listZones               Inspect KMS Encryption Zones
  hdfs crypto -createZone              Create KMS Encryption Zone
  hdfs dfs -allowSnapshot <dir>        Allow HDFS Snapshots
  hdfs dfs -createSnapshot <dir> <s1>  Create HDFS Snapshot
  hdfs dfs -deleteSnapshot <dir> <s1>  Delete HDFS Snapshot
  hdfs dfs -touchz <path>              Create empty 0-byte file in HDFS
  hdfs dfs -ls <path>                  List HDFS directory contents
  hdfs dfs -cat <path>                 Display HDFS file content
  hdfs dfs -tail / -head <path>        Display tail/head of file
  hdfs fsck /                          Run HDFS File System Check
  hdfs dfsadmin -report                Display DataNode cluster report
  hdfs haadmin -failover nn1 nn2       NameNode HA Failover
  hdfs storagepolicies -listPolicies   Storage Policies (HOT/WARM/COLD)
  hdfs storagepolicies -satisfyStoragePolicy Satisfy storage policy
  hdfs ec -listPolicies               Erasure Coding Policies
  hdfs cacheadmin -addDirective ...   Centralized Cache Directives
  hdfs balancer -threshold 10         HDFS Cluster Balancer
  hdfs diskbalancer -plan datanode1   DataNode Disk Balancer

MapReduce, Tez, YARN & Kyuubi:
  hadoop jar wordcount.jar <in> <out>  Submit MapReduce Job
  tez-job.sh --submit dag.xml          Submit Apache Tez DAG job
  yarn application -list               List YARN applications
  yarn application -kill <appId>       Kill YARN application
  yarn applicationattempt -list <id>   List YARN application attempts
  yarn container -list <attemptId>     List YARN container allocations
  yarn logs -applicationId <id>        View MapReduce job container logs
  yarn queue -status root.default      Check YARN Queue Status
  yarn node -list                      List live YARN NodeManagers
  yarn node -list -states DECOMMISSIONED List decommissioned nodes
  yarn scm -run                        Run YARN Shared Cache Manager
  yarn rmadmin -refreshNodes           Refresh NodeManagers in YARN
  yarn rmadmin -refreshSuperUserGroupsConfiguration Refresh Superuser ACLs
  kyuubi start                         Start Apache Kyuubi Gateway

Hadoop Ecosystem Commands:
  hive -e "SELECT ... FROM ..."        Execute Hive SQL queries over HDFS data
  hive -e "ALTER TABLE sales ADD COLUMNS (...)" Alter Hive Metastore Schema
  hive -e "DROP TABLE sales"          Drop Hive Metastore Table
  hive --service llap                  Start Hive LLAP In-Memory Query Accelerator
  impala-shell -q "SELECT ..."         Execute Impala MPP SQL query
  presto --execute "SELECT ..."        Execute Presto/Trino SQL query
  pig -e "LOAD ... DUMP ..."           Execute Apache Pig Latin statements
  pig -f script.pig                    Run a Pig Latin script file
  phoenix-sqlline -e "SELECT ..."      Execute Apache Phoenix SQL over HBase
  nifi.sh start                        Start Apache NiFi Flow Engine
  zeppelin-daemon.sh start             Start Apache Zeppelin notebook server
  hbase shell                          Open interactive HBase NoSQL shell
  hbase shell -c "list"                Run HBase shell command directly
  pyspark / spark / spark-shell        Interactive PySpark REPL shell (>>> prompt)
  hive / pig / ksql                    Interactive KSQL WASM Database shell (>> prompt)
  spark-submit app.py                  Submit PySpark job execution
  get                                  Import local file from host OS into terminal storage
  download <file>                      Export terminal file to host OS Downloads folder
  about / cat about.txt                Display developer, architecture, and system details
  sqoop import --table tbl --target-dir hdfs_dir  Import database tables into HDFS
  kafka-topics.sh --create --topic logs Create Kafka topic
  flume-ng agent --conf-file flume.conf  Run Flume stream ingestion to HDFS
  oozie job -config job.properties -run Run Oozie DAG Workflow
  ranger policy -list                  Inspect Ranger Security Policies
  kinit Hacker@HADOOP.LOCAL            Authenticate with Kerberos
  klist / kdestroy                     Inspect or clear Kerberos tickets
  clear                                Clear terminal screen`;
  }
}
