import { describe, it, expect } from 'vitest';
import { SimulatorBackend } from '../core/backend/hadoopBackend';

/**
 * Regression tests for the BDA lab manual (ref/BDA.pdf) and the real-Hadoop behaviour
 * the experiments rely on. Python/PySpark parts need Pyodide and are checked in the browser.
 */
function cluster(): SimulatorBackend {
  const b = new SimulatorBackend();
  b.executeCLI('hdfs namenode -format');
  b.executeCLI('start-dfs.sh');
  return b;
}

describe('Experiment 1 & 2: HDFS setup and file operations', () => {
  it('formats, starts and lists daemons', () => {
    const b = new SimulatorBackend();
    expect(b.executeCLI('start-dfs.sh')).toContain('not formatted');
    expect(b.executeCLI('hdfs namenode -format')).toContain('successfully formatted');
    expect(b.executeCLI('start-dfs.sh')).toContain('HDFS daemons initialized');
    expect(b.executeCLI('jps')).toContain('NameNode');
  });

  it('runs the manual: mkdir, put, ls, cat, setrep, rm', () => {
    const b = cluster();
    b.saveLocalFile('k.txt', 'Hello Hacker !');
    expect(b.executeCLI('hdfs dfs -mkdir Hacker')).toContain('Created HDFS directory');
    expect(b.executeCLI('hdfs dfs -ls')).toContain('Hacker');
    expect(b.executeCLI('hdfs dfs -put k.txt /Hacker/k.txt')).toContain('Copied from Local');
    expect(b.executeCLI('hdfs dfs -ls /Hacker')).toContain('/Hacker/k.txt');
    expect(b.executeCLI('hdfs dfs -cat /Hacker/k.txt')).toBe('Hello Hacker !');
    expect(b.executeCLI('hdfs dfs -setrep -w 2 /Hacker/k.txt')).toContain('Replication 2 set');
    expect(b.executeCLI('hdfs dfs -ls /Hacker')).toContain('-rw-r--r--   2 ');
    expect(b.executeCLI('hdfs dfs -rm /Hacker/k.txt')).toContain('Deleted /Hacker/k.txt');
    expect(b.executeCLI('hdfs dfs -ls /Hacker')).toBe('Found 0 items');
  });

  it('follows Hadoop semantics for put/rm/mkdir errors', () => {
    const b = cluster();
    b.saveLocalFile('k.txt', 'x');
    expect(b.executeCLI('hdfs dfs -mkdir /a/b')).toContain('No such file or directory');
    expect(b.executeCLI('hdfs dfs -mkdir -p /a/b')).toContain('Created');
    expect(b.executeCLI('hdfs dfs -put k.txt /a/b/')).toContain('/a/b/k.txt');
    expect(b.executeCLI('hdfs dfs -put k.txt /a/b/')).toContain('File exists');
    expect(b.executeCLI('hdfs dfs -put missing.txt /a/')).toContain('No such file or directory');
    expect(b.executeCLI('hdfs dfs -rm /a')).toContain('Is a directory');
    expect(b.executeCLI('hdfs dfs -rm -r /a')).toContain('Deleted /a');
  });

  it('supports globs, recursion, chmod and find', () => {
    const b = cluster();
    b.saveLocalFile('a.txt', 'one');
    b.saveLocalFile('b.txt', 'two');
    b.executeCLI('hdfs dfs -mkdir -p /in');
    b.executeCLI('hdfs dfs -put a.txt b.txt /in');
    expect(b.executeCLI('hdfs dfs -cat /in/*.txt')).toBe('one\ntwo');
    expect(b.executeCLI('hdfs dfs -ls -R /')).toContain('/in/b.txt');
    b.executeCLI('hdfs dfs -chmod 750 /in/a.txt');
    expect(b.executeCLI('hdfs dfs -ls /in/a.txt')).toContain('-rwxr-x---');
    expect(b.executeCLI('hdfs dfs -find / -name "a*"')).toBe('/in/a.txt');
    expect(b.executeCLI('hdfs dfs -stat "%r %b %n" /in/a.txt')).toBe('3 3 a.txt');
  });
});

describe('Experiment 3 context: MapReduce on YARN', () => {
  it('computes WordCount from the real input', () => {
    const b = cluster();
    b.executeCLI('start-yarn.sh');
    b.saveLocalFile('w.txt', 'hello world hello\nbye world\n');
    b.executeCLI('hdfs dfs -mkdir -p /in');
    b.executeCLI('hdfs dfs -put w.txt /in');
    expect(b.executeCLI('hadoop jar hadoop-mapreduce-examples-3.3.6.jar wordcount /in /out')).toContain('completed successfully');
    expect(b.executeCLI('hdfs dfs -cat /out/part-r-00000')).toBe('bye\t1\nhello\t2\nworld\t2');
    expect(b.executeCLI('hadoop jar hadoop-mapreduce-examples.jar wordcount /in /out')).toContain('already exists');
  });

  it('estimates pi exactly like Hadoop', () => {
    const b = cluster();
    b.executeCLI('start-yarn.sh');
    expect(b.executeCLI('hadoop jar hadoop-mapreduce-examples.jar pi 2 10')).toContain('Estimated value of Pi is 3.80000000000000000000');
  });
});
