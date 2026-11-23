import { HadoopDB } from '../../storage/hadoopDB';
import { PigLatin, PIG_HELP, splitTopLevel } from '../pig/pigLatin';
import { HiveSession, registerHiveFunctions } from './hiveQL';

/** Results bigger than this are cut in the terminal (a browser cannot render millions of lines). */
const MAX_DISPLAY_ROWS = 1000;

export interface KSQLExecutionResult {
  output: string;
  isError: boolean;
}

export class KSQLEngine {
  private sqlLib: any = null;
  private db: any = null;
  private initPromise: Promise<void> | null = null;
  private hadoopDB: HadoopDB = new HadoopDB();
  private pig: PigLatin = new PigLatin(() => this.db);
  private hive: HiveSession = new HiveSession(() => this.db, {
    readLocal: async (path: string) => {
      const files = await this.hadoopDB.loadAllLocalFiles();
      const abs = path.startsWith('/') ? path : `/home/Hacker/${path.replace(/^\.\//, '')}`;
      if (files.has(abs)) return files.get(abs);
      const under = [...files.keys()].filter((k) => k.startsWith(abs.replace(/\/+$/, '') + '/')).sort();
      if (under.length) return under.map((k) => files.get(k) || '').join('');
      return undefined;
    },
    readHdfs: (path: string) => {
      const w: any = typeof window !== 'undefined' ? window : null;
      return w && w.__hadoop_hdfs_read ? w.__hadoop_hdfs_read(path) : null;
    },
    deleteHdfs: (path: string) => {
      const w: any = typeof window !== 'undefined' ? window : null;
      if (w && w.__hadoop_hdfs_delete) w.__hadoop_hdfs_delete(path);
    },
    hdfsShell: (command: string) => {
      const w: any = typeof window !== 'undefined' ? window : null;
      return w && w.__hadoop_cli ? String(w.__hadoop_cli(command)) : 'HDFS shell is not available here.';
    }
  });
  private persistTimer: ReturnType<typeof setTimeout> | null = null;

  public saveLocalFileCallback?: (filename: string, content: string) => void;

  constructor() {
    if (typeof window !== 'undefined') {
      // Save pending SQL changes when the tab is hidden or closed.
      const flush = () => {
        if (this.persistTimer) {
          clearTimeout(this.persistTimer);
          this.persistTimer = null;
          this.persistDatabase();
        }
      };
      window.addEventListener('pagehide', flush);
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'hidden') flush();
      });
    }
  }

  /** Saving exports the whole database, so batch quick successive changes into one save. */
  private schedulePersist(): void {
    if (this.persistTimer) clearTimeout(this.persistTimer);
    this.persistTimer = setTimeout(() => {
      this.persistTimer = null;
      this.persistDatabase();
    }, 800);
  }

  public async init(): Promise<void> {
    if (typeof window === 'undefined') return;
    if (this.db) return;

    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = new Promise<void>((resolve) => {
      const loadScriptAndInit = async () => {
        try {
          if (!(window as any).initSqlJs) {
            const script = document.createElement('script');
            script.src = './sql/sql-wasm.js';
            script.async = true;
            document.head.appendChild(script);
            await new Promise((res, rej) => {
              script.onload = res;
              script.onerror = rej;
            });
          }

          if ((window as any).initSqlJs) {
            this.sqlLib = await (window as any).initSqlJs({
              locateFile: (file: string) => {
                if (file.endsWith('.wasm')) {
                  return './sql/sql-wasm.wasm';
                }
                return `./sql/${file}`;
              }
            });

            await this.hadoopDB.waitReady();
            const savedBytes = await this.hadoopDB.loadKSQLDatabase();
            if (savedBytes && savedBytes.length > 0) {
              try {
                this.db = new this.sqlLib.Database(savedBytes);
              } catch {
                // A damaged saved database must not lock the student out: start fresh.
                this.db = new this.sqlLib.Database();
              }
            } else {
              this.db = new this.sqlLib.Database();
            }
            registerHiveFunctions(this.db);
          }
          resolve();
        } catch (err: any) {
          console.error('KSQL Engine init error:', err);
          resolve();
        }
      };

      loadScriptAndInit();
    });

    return this.initPromise;
  }

  public getDatabase(): any {
    return this.db;
  }

  public getSqlLib(): any {
    return this.sqlLib;
  }

  public async persistDatabase(): Promise<void> {
    if (!this.db) return;
    try {
      const data = this.db.export();
      await this.hadoopDB.saveKSQLDatabase(data);
    } catch {}
  }

  private async handleLoadCommand(rawQuery: string): Promise<KSQLExecutionResult> {
    let filePath = '';
    let tableName = '';
    let query = rawQuery.trim().replace(/;\s*$/, '').trim();

    // Pig schema: AS (name:chararray, price:int)
    let schema: { name: string; type: string }[] = [];
    const schemaMatch = query.match(/\s+AS\s*\(([^)]*)\)\s*$/i);
    if (schemaMatch) {
      schema = schemaMatch[1].split(',').map((f) => f.trim()).filter(Boolean).map((f) => {
        const [n, t] = f.split(':').map((x) => x.trim());
        return { name: n.replace(/[^a-zA-Z0-9_]/g, '_'), type: (t || '').toLowerCase() };
      });
      query = query.slice(0, schemaMatch.index).trim();
    }

    const assignMatch = query.match(/^([a-zA-Z0-9_]+)\s*=\s*LOAD\s+['"]?([^'"]+)['"]?/i);
    const intoMatch = query.match(/^LOAD\s+['"]?([^'"]+)['"]?\s+(?:USING\s+[^\s]+\s+)?INTO\s+([a-zA-Z0-9_]+)/i);
    const simpleMatch = query.match(/^LOAD\s+['"]?([^'"]+)['"]?/i);

    const isPigAssign = !!assignMatch;
    if (assignMatch) {
      tableName = assignMatch[1].trim();
      filePath = assignMatch[2].trim();
    } else if (intoMatch) {
      filePath = intoMatch[1].trim();
      tableName = intoMatch[2].trim();
    } else if (simpleMatch) {
      filePath = simpleMatch[1].trim();
      tableName = filePath.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_');
    } else {
      return { output: "Usage: load 'file.csv' into table_name OR table_name = load 'file.csv'", isError: true };
    }

    const allFiles = await this.hadoopDB.loadAllLocalFiles();
    let fileContent: string | undefined = undefined;

    const cleanTarget = filePath.replace(/^\.\//, '').replace(/^\/home\/Hacker\//, '');
    for (const [key, val] of allFiles.entries()) {
      const cleanKey = key.replace(/^\.\//, '').replace(/^\/home\/Hacker\//, '');
      if (cleanKey === cleanTarget || key === filePath) {
        fileContent = val;
        break;
      }
    }

    if (!fileContent) {
      // Pig usually loads from HDFS: try the HDFS path too.
      const w: any = typeof window !== 'undefined' ? window : null;
      const fromHdfs = w && w.__hadoop_hdfs_read ? w.__hadoop_hdfs_read(filePath) : null;
      if (fromHdfs !== null && fromHdfs !== undefined) fileContent = String(fromHdfs);
    }

    if (!fileContent) {
      return {
        output: `Error: "${filePath}" was not found in the local folder or in HDFS. Create it with vim ${filePath} (or upload it with hdfs dfs -put).`,
        isError: true
      };
    }

    const lines = fileContent.split(/\r?\n/).map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length === 0) {
      return { output: `File "${filePath}" is empty.`, isError: true };
    }

    const delimiter = lines[0].includes('\t') ? '\t' : ',';
    const firstRowTokens = lines[0].split(delimiter).map(s => s.trim().replace(/^['"]|['"]$/g, ''));

    let headers: string[] = [];
    let startIdx = 0;

    const isFirstRowHeader = firstRowTokens.every(t => isNaN(Number(t)));
    if (isFirstRowHeader && lines.length > 1) {
      headers = firstRowTokens.map(h => h.replace(/[^a-zA-Z0-9_]/g, '_'));
      startIdx = 1;
    } else {
      headers = firstRowTokens.map((_, i) => `col_${i + 1}`);
      startIdx = 0;
    }

    if (schema.length > 0) {
      headers = headers.map((h, i) => (schema[i] ? schema[i].name : h));
    }

    // Detect column types so numeric filters and ORDER BY compare numbers, not text.
    const dataRows = lines.slice(startIdx).map(l => l.split(delimiter).map(v => v.trim().replace(/^['"]|['"]$/g, '')));
    const colTypes = headers.map((_, colIdx) => {
      const pigT = schema[colIdx]?.type;
      if (pigT) {
        if (['int', 'long', 'biginteger'].includes(pigT)) return 'INTEGER';
        if (['float', 'double', 'bigdecimal'].includes(pigT)) return 'REAL';
        return 'TEXT';
      }
      const vals = dataRows.map(r => r[colIdx] ?? '').filter(v => v !== '');
      if (vals.length === 0) return 'TEXT';
      if (vals.every(v => /^-?\d+$/.test(v))) return 'INTEGER';
      if (vals.every(v => v !== '' && !isNaN(Number(v)))) return 'REAL';
      return 'TEXT';
    });

    try {
      const colDefs = headers.map((h, i) => `${h} ${colTypes[i]}`).join(', ');
      if (isPigAssign) {
        // Pig semantics: "a = LOAD ..." redefines relation a.
        this.db.exec(`DROP TABLE IF EXISTS ${tableName};`);
      }
      this.db.exec(`CREATE TABLE IF NOT EXISTS ${tableName} (${colDefs});`);

      let insertedCount = 0;
      // One prepared statement inside one transaction: 100,000 rows load in well under a second.
      const stmt = this.db.prepare(`INSERT INTO ${tableName} VALUES (${headers.map(() => '?').join(', ')})`);
      this.db.exec('BEGIN');
      try {
        for (const row of dataRows) {
          stmt.run(headers.map((_, colIdx) => {
            const raw = row[colIdx] ?? '';
            if (raw === '') return null;
            if (colTypes[colIdx] === 'INTEGER' && /^-?\d+$/.test(raw)) return Number(raw);
            if (colTypes[colIdx] === 'REAL' && !isNaN(Number(raw))) return Number(raw);
            return raw;
          }));
          insertedCount++;
        }
        this.db.exec('COMMIT');
      } catch (err) {
        this.db.exec('ROLLBACK');
        throw err;
      } finally {
        stmt.free();
      }

      this.schedulePersist();
      if (isPigAssign) this.pig.registerLoad(tableName, filePath);
      return {
        output: `Successfully loaded ${insertedCount} records from "${filePath}" into table "${tableName}".`,
        isError: false
      };
    } catch (e: any) {
      return { output: 'Error loading CSV into table: ' + e.message, isError: true };
    }
  }

  private async handleStoreSaveCommand(query: string): Promise<KSQLExecutionResult> {
    const trimmed = query.trim().replace(/;\s*$/, '').trim();
    const storeMatch = trimmed.match(/^(?:STORE|SAVE)\s+([a-zA-Z0-9_]+)\s+INTO\s+['"]?([^'"]+)['"]?/i);

    if (storeMatch) {
      const tableName = storeMatch[1].trim();
      let filename = storeMatch[2].trim();
      if (!filename.includes('.')) filename += '.csv';

      try {
        const result = this.db.exec(`SELECT * FROM ${tableName};`);
        if (result.length === 0) {
          return { output: `Table "${tableName}" is empty or does not exist.`, isError: true };
        }

        const columns = result[0].columns;
        const rows = result[0].values;

        let csvContent = columns.join(',') + '\n';
        rows.forEach((row: any[]) => {
          const line = row.map(v => (v === null ? '' : String(v).includes(',') ? `"${v}"` : String(v))).join(',');
          csvContent += line + '\n';
        });

        if (this.saveLocalFileCallback) {
          this.saveLocalFileCallback(filename, csvContent);
        } else {
          await this.hadoopDB.saveLocalFile(filename, csvContent);
        }

        return {
          output: `Successfully stored ${rows.length} rows from table "${tableName}" into "${filename}".`,
          isError: false
        };
      } catch (e: any) {
        return { output: `Error storing table "${tableName}": ` + e.message, isError: true };
      }
    }

    if (!this.db) return { output: 'Database empty.', isError: true };
    const parts = trimmed.split(/\s+/);
    let filename = parts[1] || 'database.db';
    if (!filename.endsWith('.db')) filename += '.db';

    try {
      const data = this.db.export();
      let binary = '';
      const bytes = new Uint8Array(data);
      for (let i = 0; i < bytes.byteLength; i++) {
        binary += String.fromCharCode(bytes[i]);
      }

      if (this.saveLocalFileCallback) {
        this.saveLocalFileCallback(filename, binary);
      } else {
        await this.hadoopDB.saveLocalFile(filename, binary);
      }

      return { output: `Database saved as "${filename}".`, isError: false };
    } catch (e: any) {
      return { output: 'Error saving database: ' + e.message, isError: true };
    }
  }

  public async executeCommand(query: string): Promise<KSQLExecutionResult> {
    await this.init();

    if (!this.db && this.sqlLib) {
      try {
        this.db = new this.sqlLib.Database();
        registerHiveFunctions(this.db);
      } catch {}
    }

    const trimmed = query.trim();
    const lowerQuery = trimmed.toLowerCase();

    if (!trimmed) {
      return { output: '', isError: false };
    }

    // Several statements on one line / in one script: run them one by one, like the Hive and Pig shells.
    const statements = splitTopLevel(trimmed.replace(/^\s*--.*$/gm, ''), ';');
    if (statements.length > 1) {
      const outputs: string[] = [];
      for (const st of statements) {
        const res = await this.executeCommand(st);
        if (res.output) outputs.push(res.output);
        if (res.isError) return { output: outputs.join('\n'), isError: true };
      }
      return { output: outputs.join('\n'), isError: false };
    }

    // DESCRIBE x: Pig schema for Pig relations, Hive layout for tables created with SQL/HiveQL.
    const describeMatch = trimmed.replace(/;\s*$/, '').match(/^(describe|desc)\s+(\w+)$/i);
    if (describeMatch && !this.pig.knows(describeMatch[2]) && describeMatch[2].toLowerCase() !== 'table') {
      return this.hive.describePlain(describeMatch[2]);
    }

    if (PigLatin.isPigStatement(trimmed)) {
      const res = this.pig.execute(trimmed);
      if (!res.isError) this.schedulePersist();
      return res;
    }

    if (this.db) {
      try {
        const hiveRes = await this.hive.tryExecute(trimmed);
        if (hiveRes) {
          if (!hiveRes.isError) this.schedulePersist();
          return hiveRes;
        }
      } catch (e: any) {
        return { output: 'FAILED: ' + (e?.message || String(e)), isError: true };
      }
    }

    if (lowerQuery === 'pig help' || lowerQuery === 'help pig' || lowerQuery === '\\h') {
      return { output: PIG_HELP, isError: false };
    }

    if (lowerQuery === 'help') {
      return {
        output: `commands added:
1.clear => to clear screen
2.describe table => to get structure of table
3.save => save database (.db) or save table to CSV (e.g. save users into 'out.csv')
4.store => store table into CSV (e.g. store users into 'out.csv' using PigStorage(','))
5.load => load CSV file into table (e.g. load 'sales.csv' into sales or sales = load 'sales.csv')
6.add => to add data from an existing .db file
7.tables => to show all tables
8.ask => ask your doubts (answered offline)
9.edit => to create a sql file
10.run => to run the sql file 
11.help => to see all commands
12.pig help => Apache Pig Latin commands (DUMP, DESCRIBE, FILTER, FOREACH, GROUP,
   ORDER, LIMIT, DISTINCT, JOIN, UNION, CROSS, SAMPLE, SPLIT, ILLUSTRATE, EXPLAIN, ALIASES)

${PIG_HELP}`,
        isError: false
      };
    }

    if (lowerQuery.startsWith('load ') || /^\w+\s*=\s*load\s/.test(lowerQuery)) {
      return this.handleLoadCommand(trimmed);
    }

    if (lowerQuery.startsWith('store ') || (lowerQuery.startsWith('save ') && lowerQuery.includes(' into '))) {
      return this.handleStoreSaveCommand(trimmed);
    }

    if (lowerQuery.startsWith('save')) {
      return this.handleStoreSaveCommand(trimmed);
    }

    if (lowerQuery === 'tables') {
      if (!this.db) return { output: 'No tables found in the database.', isError: false };
      try {
        const result = this.db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%';");
        if (result.length > 0 && result[0].values.length > 0) {
          let outputText = 'Tables:\n';
          result[0].values.forEach((row: any[]) => {
            outputText += '- ' + row[0] + '\n';
          });
          return { output: outputText.trim(), isError: false };
        } else {
          return { output: 'No tables found in the database.', isError: false };
        }
      } catch (e: any) {
        return { output: 'Error: ' + e.message, isError: true };
      }
    }

    if (lowerQuery.startsWith('describe table') || lowerQuery.startsWith('desc ')) {
      if (!this.db) return { output: 'Database not initialized.', isError: true };
      const parts = trimmed.split(/\s+/);
      const tableName = parts[parts.length - 1];
      try {
        const result = this.db.exec(`PRAGMA table_info(${tableName});`);
        if (result.length > 0 && result[0].values.length > 0) {
          let outputText = 'Column Name | Type | Not Null | Default Value | Primary Key\n';
          outputText += '-'.repeat(60) + '\n';
          result[0].values.forEach((row: any[]) => {
            outputText += row.join(' | ') + '\n';
          });
          return { output: outputText.trim(), isError: false };
        } else {
          return { output: `Error: Table '${tableName}' does not exist.`, isError: true };
        }
      } catch (e: any) {
        return { output: 'Error: ' + e.message, isError: true };
      }
    }

    if (lowerQuery.startsWith('ask ')) {
      const match = query.match(/^ask\s+"(.+)"$/i) || query.match(/^ask\s+(.+)$/i);
      if (!match) {
        return { output: 'Usage: ask "your question"', isError: true };
      }
      const userPrompt = match[1].replace(/^["']|["']$/g, '');

      // Answered locally - this shell never sends requests outside the app.
      return { output: offlineAnswer(userPrompt), isError: false };
    }

    if (!this.db) return { output: 'Database not initialized.', isError: true };

    try {
      const result = this.db.exec(this.hive.translate(query));
      if (!/^\s*(select|with|pragma|explain)\b/i.test(trimmed)) this.schedulePersist();

      if (result.length > 0) {
        let outputText = '';
        result.forEach((table: any) => {
          outputText += table.columns.join(' | ') + '\n';
          outputText += '-'.repeat(table.columns.join(' | ').length) + '\n';
          const shown = table.values.slice(0, MAX_DISPLAY_ROWS);
          shown.forEach((row: any[]) => {
            outputText += row.map((v: any) => (v === null ? 'NULL' : v)).join(' | ') + '\n';
          });
          if (table.values.length > MAX_DISPLAY_ROWS) {
            outputText += `... ${table.values.length - MAX_DISPLAY_ROWS} more row(s) not shown (${table.values.length} total). Add LIMIT, or save them with: store <table> into 'out.csv'\n`;
          }
        });
        return { output: outputText.trim(), isError: false };
      } else {
        return { output: 'Query executed successfully.', isError: false };
      }
    } catch (e: any) {
      return { output: 'Error: ' + e.message, isError: true };
    }
  }
}

/** Small built-in help desk for "ask" so the shell works fully offline. */
const ASK_TOPICS: { keys: string[]; answer: string }[] = [
  { keys: ['dump'], answer: "DUMP prints every tuple of a relation: a = LOAD 'sales.csv' USING PigStorage(','); DUMP a;" },
  { keys: ['describe', 'schema'], answer: 'DESCRIBE a; shows the schema (field names and types) of relation a. For SQL tables use: describe table a' },
  { keys: ['filter', 'where'], answer: 'b = FILTER a BY price > 100;  (Pig)   or   SELECT * FROM a WHERE price > 100;  (SQL)' },
  { keys: ['foreach', 'generate', 'project'], answer: 'b = FOREACH a GENERATE product, price * 2 AS double_price;' },
  { keys: ['group', 'count', 'sum', 'avg', 'aggregate'], answer: 'g = GROUP a BY product; s = FOREACH g GENERATE group, COUNT(a) AS cnt, SUM(a.price) AS total; DUMP s;' },
  { keys: ['order', 'sort'], answer: 'o = ORDER a BY price DESC;  (Pig)   or   SELECT * FROM a ORDER BY price DESC;' },
  { keys: ['limit', 'top'], answer: 'l = LIMIT a 5;  (Pig)   or   SELECT * FROM a LIMIT 5;' },
  { keys: ['distinct', 'duplicate', 'unique'], answer: 'd = DISTINCT a;  removes duplicate tuples.' },
  { keys: ['join'], answer: 'j = JOIN a BY id, b BY id;  fields are named a::col and b::col. LEFT join: JOIN a BY id LEFT OUTER, b BY id;' },
  { keys: ['union', 'combine', 'append'], answer: 'u = UNION a, b;  (both relations need the same number of fields)' },
  { keys: ['cross', 'cartesian'], answer: 'x = CROSS a, b;  every tuple of a paired with every tuple of b.' },
  { keys: ['split'], answer: 'SPLIT a INTO cheap IF price < 100, costly OTHERWISE;' },
  { keys: ['sample', 'random'], answer: 's = SAMPLE a 0.3;  keeps about 30% of the tuples.' },
  { keys: ['explain', 'plan'], answer: 'EXPLAIN a; shows the chain of operators that produced relation a.' },
  { keys: ['illustrate', 'example'], answer: 'ILLUSTRATE a; shows the schema with a few sample tuples.' },
  { keys: ['load', 'import', 'csv'], answer: "a = LOAD 'file.csv' USING PigStorage(',') AS (name:chararray, price:int);  The file must exist in the terminal (create it with vim)." },
  { keys: ['store', 'save', 'export'], answer: "STORE a INTO 'out.csv' USING PigStorage(',');  or  save  (whole database as .db)" },
  { keys: ['table', 'tables', 'show'], answer: 'tables  lists all tables. describe table <name>  shows its columns.' },
  { keys: ['create', 'insert'], answer: "CREATE TABLE t (id INTEGER, name TEXT); INSERT INTO t VALUES (1, 'a');" },
  { keys: ['delete', 'drop', 'remove'], answer: 'DROP TABLE t;  removes a table.  DELETE FROM t WHERE id = 1;  removes rows.' },
  { keys: ['update'], answer: "UPDATE t SET name = 'b' WHERE id = 1;" },
  { keys: ['exit', 'quit'], answer: 'Type exit or quit to return to the Linux shell.' },
];

function offlineAnswer(prompt: string): string {
  const words: string[] = prompt.toLowerCase().match(/[a-z_]+/g) || [];
  let best: { score: number; answer: string } = { score: 0, answer: '' };
  for (const topic of ASK_TOPICS) {
    const score = topic.keys.filter((k) => words.includes(k)).length;
    if (score > best.score) best = { score, answer: topic.answer };
  }
  return best.score > 0 ? best.answer : 'ASK Ksql RELATED THINGS.... (try: ask how to group, ask dump, ask join). Type pig help for all Pig commands.';
}
