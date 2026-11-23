/**
 * HiveQL on top of the browser SQLite engine.
 *
 * Accepts the Hive syntax students learn (ROW FORMAT DELIMITED, LOAD DATA [LOCAL] INPATH,
 * SHOW DATABASES, DESCRIBE FORMATTED, INSERT OVERWRITE, SORT BY, Hive functions, ...),
 * follows Hive semantics (default \001 delimiter, LOAD DATA INPATH moves the HDFS file,
 * bad values become NULL) and translates the rest to SQLite. No network access.
 */

export interface HiveResult {
  output: string;
  isError: boolean;
}

export interface HiveHooks {
  readLocal(path: string): Promise<string | undefined>;
  readHdfs(path: string): string | null;
  deleteHdfs(path: string): void;
  hdfsShell(command: string): string;
}

interface TableMeta {
  delimiter: string;
  location?: string;
  external?: boolean;
  skipHeader?: number;
  hiveTypes: Record<string, string>;
  partitionCols: string[];
  comment?: string;
}

const META_TABLE = '__hive_meta';

function q(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

function unquote(s: string): string {
  const t = s.trim();
  if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith('"') && t.endsWith('"'))) return t.slice(1, -1);
  return t;
}

/** Decode Hive delimiter literals: '\t', '\001', '\u0001', ','. */
function decodeDelimiter(raw: string): string {
  const s = unquote(raw);
  if (s === '\\t') return '\t';
  if (/^\\0*\d{1,3}$/.test(s)) return String.fromCharCode(parseInt(s.replace(/^\\/, ''), 8));
  if (/^\\u[0-9a-fA-F]{4}$/.test(s)) return String.fromCharCode(parseInt(s.slice(2), 16));
  return s;
}

/** Split text at top-level commas (not inside parentheses or angle brackets or quotes). */
function splitTop(text: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let quote: string | null = null;
  let cur = '';
  for (const ch of text) {
    if (quote) {
      cur += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === "'" || ch === '"' || ch === '`') {
      quote = ch;
      cur += ch;
      continue;
    }
    if (ch === '(' || ch === '<') depth++;
    if (ch === ')' || ch === '>') depth--;
    if (ch === ',' && depth === 0) {
      out.push(cur.trim());
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

export function hiveToSqliteType(hiveType: string): string {
  const t = hiveType.trim().toUpperCase();
  if (/^(TINYINT|SMALLINT|INT|INTEGER|BIGINT)\b/.test(t)) return 'INTEGER';
  if (/^(FLOAT|DOUBLE|DECIMAL|NUMERIC|REAL)\b/.test(t)) return 'REAL';
  if (/^BOOLEAN\b/.test(t)) return 'BOOLEAN';
  return 'TEXT';
}

function sqliteToHiveType(sqliteType: string): string {
  const t = (sqliteType || '').toUpperCase();
  if (t.includes('INT')) return 'int';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB') || t.includes('NUM') || t.includes('DEC')) return 'double';
  if (t.includes('BOOL')) return 'boolean';
  return 'string';
}

/** Convert one text field to the column type the way Hive does: invalid -> NULL. */
function convertField(value: string | undefined, hiveType: string): any {
  if (value === undefined) return null;
  const t = hiveType.toUpperCase();
  if (value === '\\N') return null;
  if (/^(TINYINT|SMALLINT|INT|INTEGER|BIGINT)/.test(t)) {
    const v = value.trim();
    return /^[+-]?\d+$/.test(v) ? Number(v) : null;
  }
  if (/^(FLOAT|DOUBLE|DECIMAL|NUMERIC|REAL)/.test(t)) {
    const v = value.trim();
    return v !== '' && !isNaN(Number(v)) ? Number(v) : null;
  }
  if (/^BOOLEAN/.test(t)) {
    const v = value.trim().toLowerCase();
    return v === 'true' ? 1 : v === 'false' ? 0 : null;
  }
  return value;
}

/** Register Hive built-in functions that SQLite lacks. */
/**
 * sql.js reads a function's argument count from its declared parameters, so register one
 * wrapper per arity. sql.js keeps only the last JS callback per name, so every wrapper
 * forwards exactly the arguments SQLite passed (via `arguments`).
 */
const ARITY: ((f: (...a: any[]) => any) => (...a: any[]) => any)[] = [
  (f) => function () { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any, _d: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any, _d: any, _e: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any, _d: any, _e: any, _g: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any, _d: any, _e: any, _g: any, _h: any) { return f.apply(null, Array.prototype.slice.call(arguments)); },
  (f) => function (_a: any, _b: any, _c: any, _d: any, _e: any, _g: any, _h: any, _i: any) { return f.apply(null, Array.prototype.slice.call(arguments)); }
];

export function registerHiveFunctions(db: any): void {
  const fn = (name: string, f: (...a: any[]) => any, arities?: number[]) => {
    for (const n of arities || [f.length]) {
      try {
        db.create_function(name, ARITY[n](f));
      } catch {
        // Older sql.js builds may refuse a name; the query will report it.
      }
    }
  };
  const many = (from: number) => Array.from({ length: ARITY.length - from }, (_, i) => i + from);
  fn('concat', (...a: any[]) => (a.some((x) => x === null) ? null : a.map(String).join('')), many(1));
  fn('concat_ws', (sep: any, ...a: any[]) => a.filter((x) => x !== null).map(String).join(String(sep)), many(2));
  fn('nvl', (a: any, b: any) => (a === null ? b : a));
  fn('if', (c: any, a: any, b: any) => (c ? a : b));
  fn('size', (s: any) => (s === null ? -1 : String(s).split(',').length));
  fn('rand', () => Math.random(), [0, 1]);
  fn('year', (d: any) => (d === null ? null : new Date(String(d)).getFullYear() || null));
  fn('month', (d: any) => (d === null ? null : new Date(String(d)).getMonth() + 1 || null));
  fn('day', (d: any) => (d === null ? null : new Date(String(d)).getDate() || null));
  fn('to_date', (d: any) => (d === null ? null : String(d).slice(0, 10)));
  fn('datediff', (a: any, b: any) => Math.round((Date.parse(String(a).slice(0, 10)) - Date.parse(String(b).slice(0, 10))) / 86400000));
  fn('date_add', (d: any, n: any) => new Date(Date.parse(String(d).slice(0, 10)) + Number(n) * 86400000).toISOString().slice(0, 10));
  fn('date_sub', (d: any, n: any) => new Date(Date.parse(String(d).slice(0, 10)) - Number(n) * 86400000).toISOString().slice(0, 10));
  fn('unix_timestamp', (d?: any) => Math.floor((d === undefined ? Date.now() : Date.parse(String(d))) / 1000), [0, 1]);
  fn('from_unixtime', (s: any) => new Date(Number(s) * 1000).toISOString().replace('T', ' ').slice(0, 19));
  fn('regexp_replace', (s: any, p: any, r: any) => (s === null ? null : String(s).replace(new RegExp(String(p), 'g'), String(r).replace(/\$(\d)/g, '$$$1'))));
  fn('regexp_extract', (s: any, p: any, i: any) => {
    if (s === null) return null;
    const m = String(s).match(new RegExp(String(p)));
    return m ? m[Number(i ?? 1)] ?? '' : '';
  }, [2, 3]);
  fn('split_part', (s: any, sep: any, i: any) => (s === null ? null : String(s).split(String(sep))[Number(i) - 1] ?? null));
  fn('initcap', (s: any) => (s === null ? null : String(s).toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase())));
  fn('reverse', (s: any) => (s === null ? null : String(s).split('').reverse().join('')));
  fn('lpad', (s: any, n: any, p: any) => (s === null ? null : String(s).padStart(Number(n), String(p)).slice(0, Number(n))));
  fn('rpad', (s: any, n: any, p: any) => (s === null ? null : String(s).padEnd(Number(n), String(p)).slice(0, Number(n))));
  fn('floor', (x: any) => (x === null ? null : Math.floor(Number(x))));
  fn('ceil', (x: any) => (x === null ? null : Math.ceil(Number(x))));
  fn('pow', (a: any, b: any) => Math.pow(Number(a), Number(b)));
  fn('sqrt', (a: any) => Math.sqrt(Number(a)));
  fn('log10', (a: any) => Math.log10(Number(a)));
  try {
    const agg = (name: string, unique: boolean) =>
      db.create_aggregate(name, {
        init: () => [] as any[],
        step: (state: any[], value: any) => {
          if (value !== null && (!unique || !state.includes(value))) state.push(value);
          return state;
        },
        finalize: (state: any[]) => '[' + state.map((v) => (typeof v === 'string' ? `"${v}"` : String(v))).join(',') + ']'
      });
    agg('collect_set', true);
    agg('collect_list', false);
  } catch {
    // create_aggregate needs sql.js >= 1.7.
  }
}

export class HiveSession {
  private currentDb = 'default';

  constructor(private getDb: () => any, private hooks: HiveHooks) {}

  public getCurrentDatabase(): string {
    return this.currentDb;
  }

  private ensureMeta(): void {
    this.getDb().exec(`CREATE TABLE IF NOT EXISTS ${META_TABLE} (kind TEXT, name TEXT, value TEXT, PRIMARY KEY (kind, name));`);
  }

  private metaGet(kind: string, name: string): string | null {
    this.ensureMeta();
    const st = this.getDb().prepare(`SELECT value FROM ${META_TABLE} WHERE kind = ? AND name = ?`);
    st.bind([kind, name]);
    const v = st.step() ? st.get()[0] : null;
    st.free();
    return v;
  }

  private metaSet(kind: string, name: string, value: string | null): void {
    this.ensureMeta();
    if (value === null) {
      this.getDb().run(`DELETE FROM ${META_TABLE} WHERE kind = ? AND name = ?`, [kind, name]);
    } else {
      this.getDb().run(`INSERT OR REPLACE INTO ${META_TABLE} (kind, name, value) VALUES (?, ?, ?)`, [kind, name, value]);
    }
  }

  private metaList(kind: string): string[] {
    this.ensureMeta();
    const r = this.getDb().exec(`SELECT name FROM ${META_TABLE} WHERE kind = '${kind}' ORDER BY name`);
    return r.length ? r[0].values.map((v: any[]) => String(v[0])) : [];
  }

  private tableMeta(table: string): TableMeta | null {
    const raw = this.metaGet('table', table.toLowerCase());
    return raw ? (JSON.parse(raw) as TableMeta) : null;
  }

  private tableExists(table: string): boolean {
    const r = this.getDb().exec(`SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND lower(name) = lower('${table.replace(/'/g, "''")}')`);
    return r.length > 0 && r[0].values.length > 0;
  }

  private columns(table: string): { name: string; type: string }[] {
    const r = this.getDb().exec(`PRAGMA table_info(${q(table)});`);
    return r.length ? r[0].values.map((v: any[]) => ({ name: String(v[1]), type: String(v[2] || '') })) : [];
  }

  public userTables(): string[] {
    const r = this.getDb().exec("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE '\\_\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%' ORDER BY name");
    return r.length ? r[0].values.map((v: any[]) => String(v[0])) : [];
  }

  private ok(extra = '', start = Date.now()): HiveResult {
    const t = ((Date.now() - start) / 1000 + 0.01).toFixed(3);
    return { output: `${extra ? extra + '\n' : ''}OK\nTime taken: ${t} seconds`, isError: false };
  }

  private fail(msg: string): HiveResult {
    return { output: `FAILED: ${msg}`, isError: true };
  }

  /** Returns null when the statement is plain SQL that SQLite should run (after translate()). */
  public async tryExecute(stmt: string): Promise<HiveResult | null> {
    const s = stmt.trim().replace(/;\s*$/, '').trim();
    const lower = s.toLowerCase();
    const start = Date.now();
    let m: RegExpMatchArray | null;

    if (/^set\s+[\w.]+\s*=/.test(lower) || /^set\s*$/.test(lower) || /^add\s+(jar|file|archive)\b/.test(lower) || /^reset$/.test(lower)) {
      return { output: '', isError: false };
    }
    if ((m = s.match(/^dfs\s+([\s\S]+)$/i))) {
      return { output: this.hooks.hdfsShell(`hdfs dfs ${m[1]}`), isError: false };
    }
    if ((m = s.match(/^create\s+(database|schema)\s+(if\s+not\s+exists\s+)?(\w+)/i))) {
      const name = m[3].toLowerCase();
      if (name === 'default' || this.metaGet('db', name) !== null) {
        if (m[2]) return this.ok('', start);
        return this.fail(`Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Database ${name} already exists`);
      }
      this.metaSet('db', name, '1');
      return this.ok('', start);
    }
    if ((m = s.match(/^(show)\s+(databases|schemas)(\s+like\s+(.+))?$/i))) {
      let dbs = ['default', ...this.metaList('db')];
      if (m[4]) {
        const re = new RegExp('^' + unquote(m[4]).replace(/\*/g, '.*') + '$', 'i');
        dbs = dbs.filter((d) => re.test(d));
      }
      return { output: `${dbs.join('\n')}\nOK\nTime taken: 0.012 seconds, Fetched: ${dbs.length} row(s)`, isError: false };
    }
    if ((m = s.match(/^use\s+(\w+)$/i))) {
      const name = m[1].toLowerCase();
      if (name !== 'default' && this.metaGet('db', name) === null) return this.fail(`SemanticException [Error 10072]: Database does not exist: ${name}`);
      this.currentDb = name;
      return this.ok('', start);
    }
    if ((m = s.match(/^drop\s+(database|schema)\s+(if\s+exists\s+)?(\w+)(\s+cascade)?/i))) {
      const name = m[3].toLowerCase();
      if (this.metaGet('db', name) === null) {
        return m[2] ? this.ok('', start) : this.fail(`SemanticException [Error 10072]: Database does not exist: ${name}`);
      }
      this.metaSet('db', name, null);
      if (this.currentDb === name) this.currentDb = 'default';
      return this.ok('', start);
    }
    if ((m = s.match(/^show\s+tables(\s+(in|from)\s+\w+)?(\s+like\s+(.+))?$/i))) {
      let tables = this.userTables();
      if (m[4]) {
        const re = new RegExp('^' + unquote(m[4]).replace(/\*/g, '.*') + '$', 'i');
        tables = tables.filter((t) => re.test(t));
      }
      return { output: `${tables.join('\n')}${tables.length ? '\n' : ''}OK\nTime taken: 0.021 seconds, Fetched: ${tables.length} row(s)`, isError: false };
    }
    if ((m = s.match(/^show\s+create\s+table\s+(\w+)$/i))) {
      return this.showCreate(m[1]);
    }
    if ((m = s.match(/^show\s+partitions\s+(\w+)$/i))) {
      const meta = this.tableMeta(m[1]);
      if (!meta || !meta.partitionCols.length) return this.fail(`SemanticException [Error 10241]: Table ${m[1]} is not a partitioned table`);
      const cols = meta.partitionCols;
      const r = this.getDb().exec(`SELECT DISTINCT ${cols.map(q).join(', ')} FROM ${q(m[1])} ORDER BY ${cols.map(q).join(', ')}`);
      const parts = r.length ? r[0].values.map((v: any[]) => cols.map((c, i) => `${c}=${v[i]}`).join('/')) : [];
      return { output: `${parts.join('\n')}${parts.length ? '\n' : ''}OK\nTime taken: 0.018 seconds, Fetched: ${parts.length} row(s)`, isError: false };
    }
    if ((m = s.match(/^(describe|desc)\s+(formatted|extended)\s+(\w+)$/i))) {
      return this.describe(m[3], true);
    }
    if ((m = s.match(/^(describe|desc)\s+(database|schema)\s+(\w+)$/i))) {
      const name = m[3].toLowerCase();
      if (name !== 'default' && this.metaGet('db', name) === null) return this.fail(`SemanticException [Error 10072]: Database does not exist: ${name}`);
      return { output: `${name}\t\thdfs://localhost:9000/user/hive/warehouse${name === 'default' ? '' : '/' + name + '.db'}\tHacker\tUSER\nOK`, isError: false };
    }
    if ((m = s.match(/^create\s+(external\s+)?table\s+(if\s+not\s+exists\s+)?(\w+)\s*\(([\s\S]*)$/i))) {
      return this.createTable(s, !!m[1], !!m[2], m[3]);
    }
    if ((m = s.match(/^create\s+table\s+(if\s+not\s+exists\s+)?(\w+)\s+like\s+(\w+)$/i))) {
      if (!this.tableExists(m[3])) return this.fail(`SemanticException [Error 10001]: Table not found ${m[3]}`);
      if (this.tableExists(m[2])) return m[1] ? this.ok('', start) : this.fail(`Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. AlreadyExistsException(message:Table ${m[2]} already exists)`);
      this.getDb().exec(`CREATE TABLE ${q(m[2])} AS SELECT * FROM ${q(m[3])} WHERE 0`);
      const meta = this.tableMeta(m[3]);
      if (meta) this.metaSet('table', m[2].toLowerCase(), JSON.stringify(meta));
      return this.ok('', start);
    }
    if ((m = s.match(/^load\s+data\s+(local\s+)?inpath\s+('[^']*'|"[^"]*"|\S+)\s+(overwrite\s+)?into\s+table\s+(\w+)(\s+partition\s*\(([^)]*)\))?$/i))) {
      return this.loadData(!!m[1], unquote(m[2]), !!m[3], m[4], m[6]);
    }
    if ((m = s.match(/^truncate\s+table\s+(\w+)$/i))) {
      if (!this.tableExists(m[1])) return this.fail(`SemanticException [Error 10001]: Table not found ${m[1]}`);
      this.getDb().exec(`DELETE FROM ${q(m[1])}`);
      return this.ok('', start);
    }
    if ((m = s.match(/^alter\s+table\s+(\w+)\s+add\s+columns\s*\(([\s\S]*)\)$/i))) {
      const meta = this.tableMeta(m[1]);
      for (const def of splitTop(m[2])) {
        const [col, ...typeParts] = def.replace(/\s+comment\s+'[^']*'/i, '').trim().split(/\s+/);
        const hiveType = typeParts.join(' ') || 'string';
        this.getDb().exec(`ALTER TABLE ${q(m[1])} ADD COLUMN ${q(col)} ${hiveToSqliteType(hiveType)}`);
        if (meta) meta.hiveTypes[col.toLowerCase()] = hiveType.toLowerCase();
      }
      if (meta) this.metaSet('table', m[1].toLowerCase(), JSON.stringify(meta));
      return this.ok('', start);
    }
    if ((m = s.match(/^alter\s+table\s+(\w+)\s+change\s+(column\s+)?(\w+)\s+(\w+)\s+(\w+)/i))) {
      if (m[3] !== m[4]) this.getDb().exec(`ALTER TABLE ${q(m[1])} RENAME COLUMN ${q(m[3])} TO ${q(m[4])}`);
      return this.ok('', start);
    }
    if ((m = s.match(/^alter\s+table\s+(\w+)\s+rename\s+to\s+(\w+)$/i))) {
      this.getDb().exec(`ALTER TABLE ${q(m[1])} RENAME TO ${q(m[2])}`);
      const meta = this.metaGet('table', m[1].toLowerCase());
      if (meta) {
        this.metaSet('table', m[2].toLowerCase(), meta);
        this.metaSet('table', m[1].toLowerCase(), null);
      }
      return this.ok('', start);
    }
    if ((m = s.match(/^drop\s+table\s+(if\s+exists\s+)?(\w+)(\s+purge)?$/i))) {
      if (!this.tableExists(m[2])) return m[1] ? this.ok('', start) : this.fail(`SemanticException [Error 10001]: Table not found ${m[2]}`);
      this.getDb().exec(`DROP TABLE ${q(m[2])}`);
      this.metaSet('table', m[2].toLowerCase(), null);
      return this.ok('', start);
    }
    if ((m = s.match(/^insert\s+overwrite\s+table\s+(\w+)(\s+partition\s*\([^)]*\))?\s+([\s\S]+)$/i))) {
      if (!this.tableExists(m[1])) return this.fail(`SemanticException [Error 10001]: Table not found ${m[1]}`);
      const select = this.translate(m[3]);
      this.getDb().exec('BEGIN');
      try {
        this.getDb().exec(`DELETE FROM ${q(m[1])}`);
        this.getDb().exec(`INSERT INTO ${q(m[1])} ${select}`);
        this.getDb().exec('COMMIT');
      } catch (e) {
        this.getDb().exec('ROLLBACK');
        throw e;
      }
      return this.ok(`Loading data to table ${this.currentDb}.${m[1]}`, start);
    }
    if ((m = s.match(/^insert\s+overwrite\s+(local\s+)?directory\s+('[^']*'|"[^"]*")\s+([\s\S]+)$/i))) {
      return this.fail('SemanticException: INSERT OVERWRITE DIRECTORY is not supported in this simulator. Use: store <table> into \'file.csv\' (or Pig STORE).');
    }
    if (/^explain\s+/i.test(s)) {
      const r = this.getDb().exec('EXPLAIN QUERY PLAN ' + this.translate(s.replace(/^explain\s+(extended\s+|formatted\s+)?/i, '')));
      const lines = r.length ? r[0].values.map((v: any[]) => `  ${v[v.length - 1]}`) : [];
      return { output: `STAGE PLANS:\n  Stage: Stage-0 (local SQLite execution)\n${lines.join('\n')}\nOK`, isError: false };
    }
    return null;
  }

  /** Translate HiveQL-only syntax inside a query to SQLite. */
  public translate(sql: string): string {
    let out = sql;
    out = out.replace(/\binsert\s+into\s+table\s+/gi, 'INSERT INTO ');
    out = out.replace(/(insert\s+into\s+\w+)\s+partition\s*\([^)]*\)/gi, '$1');
    out = out.replace(/\bsort\s+by\b/gi, 'ORDER BY');
    out = out.replace(/\bcluster\s+by\b/gi, 'ORDER BY');
    out = out.replace(/\bdistribute\s+by\s+[\w.,\s]+?(?=\s+(order|sort|limit)\b|\s*$)/gi, '');
    out = out.replace(/\bcast\s*\(([^()]*?)\s+as\s+string\s*\)/gi, 'CAST($1 AS TEXT)');
    out = out.replace(/\bcast\s*\(([^()]*?)\s+as\s+(bigint|tinyint|smallint|int)\s*\)/gi, 'CAST($1 AS INTEGER)');
    out = out.replace(/\bcast\s*\(([^()]*?)\s+as\s+(double|float|decimal(\(\d+,\s*\d+\))?)\s*\)/gi, 'CAST($1 AS REAL)');
    out = out.replace(/`([^`]+)`/g, '"$1"');
    return out;
  }

  private createTable(stmt: string, external: boolean, ifNotExists: boolean, table: string): HiveResult {
    const start = Date.now();
    const open = stmt.indexOf('(');
    let depth = 0;
    let close = -1;
    for (let i = open; i < stmt.length; i++) {
      if (stmt[i] === '(') depth++;
      if (stmt[i] === ')') {
        depth--;
        if (depth === 0) {
          close = i;
          break;
        }
      }
    }
    if (close < 0) return this.fail("ParseException line 1: missing ')' in column list");
    const colDefs = splitTop(stmt.slice(open + 1, close));
    const rest = stmt.slice(close + 1);
    if (this.tableExists(table)) {
      if (ifNotExists) return this.ok('', start);
      return this.fail(`Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. AlreadyExistsException(message:Table ${this.currentDb}.${table} already exists)`);
    }
    const hiveTypes: Record<string, string> = {};
    const sqlCols: string[] = [];
    for (const def of colDefs) {
      const clean = def.replace(/\s+comment\s+'[^']*'/i, '').trim();
      const mm = clean.match(/^`?(\w+)`?\s+([\s\S]+)$/);
      if (!mm) return this.fail(`ParseException: cannot recognize column definition '${def}'`);
      hiveTypes[mm[1].toLowerCase()] = mm[2].trim().toLowerCase();
      sqlCols.push(`${q(mm[1])} ${hiveToSqliteType(mm[2])}`);
    }
    const partitionCols: string[] = [];
    const pm = rest.match(/partitioned\s+by\s*\(([^)]*)\)/i);
    if (pm) {
      for (const def of splitTop(pm[1])) {
        const [col, ...typeParts] = def.trim().split(/\s+/);
        hiveTypes[col.toLowerCase()] = typeParts.join(' ').toLowerCase() || 'string';
        partitionCols.push(col);
        sqlCols.push(`${q(col)} ${hiveToSqliteType(typeParts.join(' '))}`);
      }
    }
    const fm = rest.match(/fields\s+terminated\s+by\s+('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")/i);
    const lm = rest.match(/location\s+('[^']*'|"[^"]*")/i);
    const hm = rest.match(/skip\.header\.line\.count['"]?\s*=\s*['"]?(\d+)/i);
    const cm = rest.match(/^\s*comment\s+'([^']*)'/i);
    const meta: TableMeta = {
      delimiter: fm ? decodeDelimiter(fm[1]) : '\u0001',
      location: lm ? unquote(lm[1]) : undefined,
      external,
      skipHeader: hm ? parseInt(hm[1], 10) : 0,
      hiveTypes,
      partitionCols,
      comment: cm ? cm[1] : undefined
    };
    this.getDb().exec(`CREATE TABLE ${q(table)} (${sqlCols.join(', ')})`);
    this.metaSet('table', table.toLowerCase(), JSON.stringify(meta));
    if (meta.location) {
      const content = this.hooks.readHdfs(meta.location);
      if (content !== null && content !== '') {
        const n = this.insertText(table, meta, content, {});
        return this.ok(`Table ${table} reads its data from ${meta.location}: ${n} row(s)`, start);
      }
    }
    return this.ok('', start);
  }

  private insertText(table: string, meta: TableMeta, content: string, partition: Record<string, string>): number {
    const cols = this.columns(table);
    const dataCols = cols.filter((c) => !meta.partitionCols.map((p) => p.toLowerCase()).includes(c.name.toLowerCase()));
    let lines = content.split(/\r?\n/);
    if (lines.length && lines[lines.length - 1] === '') lines.pop();
    if (meta.skipHeader) lines = lines.slice(meta.skipHeader);
    const db = this.getDb();
    const st = db.prepare(`INSERT INTO ${q(table)} (${cols.map((c) => q(c.name)).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`);
    db.exec('BEGIN');
    let n = 0;
    try {
      for (const line of lines) {
        const fields = line.split(meta.delimiter);
        const values = cols.map((c) => {
          const pIdx = meta.partitionCols.findIndex((p) => p.toLowerCase() === c.name.toLowerCase());
          if (pIdx >= 0) return convertField(partition[meta.partitionCols[pIdx].toLowerCase()], meta.hiveTypes[c.name.toLowerCase()] || c.type);
          const i = dataCols.indexOf(c);
          return convertField(fields[i], meta.hiveTypes[c.name.toLowerCase()] || c.type);
        });
        st.run(values);
        n++;
      }
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    } finally {
      st.free();
    }
    return n;
  }

  private async loadData(local: boolean, path: string, overwrite: boolean, table: string, partitionSpec?: string): Promise<HiveResult> {
    const start = Date.now();
    if (!this.tableExists(table)) return this.fail(`SemanticException [Error 10001]: Line 1:${path.length + 30} Table not found '${table}'`);
    const meta: TableMeta = this.tableMeta(table) || { delimiter: '\u0001', hiveTypes: {}, partitionCols: [] };
    let content: string | null | undefined;
    const cleanPath = path.replace(/^file:\/\//, '');
    if (local) content = await this.hooks.readLocal(cleanPath);
    else content = this.hooks.readHdfs(cleanPath);
    if (content === null || content === undefined) {
      return this.fail(`SemanticException Line 1:23 Invalid path '${path}': No files matching path ${local ? 'file:' : 'hdfs://localhost:9000'}${cleanPath.startsWith('/') ? '' : '/'}${cleanPath}`);
    }
    const partition: Record<string, string> = {};
    if (partitionSpec) {
      for (const kv of splitTop(partitionSpec)) {
        const [k, v] = kv.split('=');
        partition[k.trim().toLowerCase()] = unquote(v || '');
      }
    }
    if (overwrite) {
      const where = Object.keys(partition).length ? ' WHERE ' + Object.entries(partition).map(([k, v]) => `${q(k)} = '${v.replace(/'/g, "''")}'`).join(' AND ') : '';
      this.getDb().exec(`DELETE FROM ${q(table)}${where}`);
    }
    const n = this.insertText(table, meta, content, partition);
    if (!local) this.hooks.deleteHdfs(cleanPath);
    const firstLine = content.split(/\r?\n/)[0] || '';
    let hint = '';
    if (meta.delimiter === '\u0001' && !firstLine.includes('\u0001') && /[,\t|]/.test(firstLine)) {
      hint = `\nNote: table ${table} has no ROW FORMAT DELIMITED clause, so Hive used its default field delimiter \\001 (Ctrl-A) and each line went into the first column.\nRecreate it with: ROW FORMAT DELIMITED FIELDS TERMINATED BY ','`;
    }
    const moved = local ? '' : `\n(As in Hive, the HDFS file was moved into the table; it no longer exists at ${cleanPath}.)`;
    return this.ok(`Loading data to table ${this.currentDb}.${table}${partitionSpec ? ' partition (' + partitionSpec + ')' : ''}\n${n} row(s) loaded.${moved}${hint}`, start);
  }

  private describe(table: string, formatted: boolean): HiveResult {
    if (!this.tableExists(table)) return this.fail(`SemanticException [Error 10001]: Table not found ${table}`);
    const meta = this.tableMeta(table);
    const cols = this.columns(table);
    const typeOf = (c: { name: string; type: string }) => meta?.hiveTypes[c.name.toLowerCase()] || sqliteToHiveType(c.type);
    const dataCols = cols.filter((c) => !meta?.partitionCols.map((p) => p.toLowerCase()).includes(c.name.toLowerCase()));
    const w = Math.max(8, ...cols.map((c) => c.name.length)) + 1;
    const lines: string[] = [];
    if (formatted) lines.push('# col_name            \tdata_type           \tcomment             ', '');
    dataCols.forEach((c) => lines.push(`${c.name.padEnd(w)}\t${typeOf(c).padEnd(20)}\t`));
    if (meta?.partitionCols.length) {
      lines.push('', '# Partition Information', '# col_name            \tdata_type           \tcomment             ', '');
      meta.partitionCols.forEach((p) => lines.push(`${p.padEnd(w)}\t${(meta.hiveTypes[p.toLowerCase()] || 'string').padEnd(20)}\t`));
    }
    if (formatted) {
      const count = this.getDb().exec(`SELECT COUNT(*) FROM ${q(table)}`)[0].values[0][0];
      lines.push(
        '',
        '# Detailed Table Information',
        `Database:           \t${this.currentDb}`,
        `OwnerType:          \tUSER`,
        `Owner:              \tHacker`,
        `Location:           \t${meta?.location ? 'hdfs://localhost:9000' + meta.location : `hdfs://localhost:9000/user/hive/warehouse/${table.toLowerCase()}`}`,
        `Table Type:         \t${meta?.external ? 'EXTERNAL_TABLE' : 'MANAGED_TABLE'}`,
        `Table Parameters:`,
        `\tnumRows             \t${count}`,
        ...(meta?.comment ? [`\tcomment             \t${meta.comment}`] : []),
        ...(meta?.skipHeader ? [`\tskip.header.line.count\t${meta.skipHeader}`] : []),
        '',
        '# Storage Information',
        `SerDe Library:      \torg.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`,
        `InputFormat:        \torg.apache.hadoop.mapred.TextInputFormat`,
        `OutputFormat:       \torg.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat`,
        `Storage Desc Params:`,
        `\tfield.delim         \t${meta ? JSON.stringify(meta.delimiter).slice(1, -1).replace('\\u0001', '\\001') : '\\001'}`
      );
    }
    return { output: `${lines.join('\n')}\nOK\nTime taken: 0.034 seconds, Fetched: ${lines.length} row(s)`, isError: false };
  }

  /** Hive-style DESCRIBE for a plain table (used when it is not a Pig relation). */
  public describePlain(table: string): HiveResult {
    return this.describe(table, false);
  }

  private showCreate(table: string): HiveResult {
    if (!this.tableExists(table)) return this.fail(`SemanticException [Error 10001]: Table not found ${table}`);
    const meta = this.tableMeta(table);
    const cols = this.columns(table).filter((c) => !meta?.partitionCols.map((p) => p.toLowerCase()).includes(c.name.toLowerCase()));
    const typeOf = (c: { name: string; type: string }) => meta?.hiveTypes[c.name.toLowerCase()] || sqliteToHiveType(c.type);
    const delim = meta ? JSON.stringify(meta.delimiter).slice(1, -1).replace('\\u0001', '\\001') : '\\001';
    const out = [
      `CREATE ${meta?.external ? 'EXTERNAL ' : ''}TABLE \`${table}\`(`,
      cols.map((c) => `  \`${c.name}\` ${typeOf(c)}`).join(',\n') + ')',
      ...(meta?.partitionCols.length ? [`PARTITIONED BY (\n${meta.partitionCols.map((p) => `  \`${p}\` ${meta.hiveTypes[p.toLowerCase()] || 'string'}`).join(',\n')})`] : []),
      `ROW FORMAT SERDE`,
      `  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'`,
      `WITH SERDEPROPERTIES (`,
      `  'field.delim'='${delim}',`,
      `  'serialization.format'='${delim}')`,
      `STORED AS INPUTFORMAT`,
      `  'org.apache.hadoop.mapred.TextInputFormat'`,
      `OUTPUTFORMAT`,
      `  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'`,
      `LOCATION`,
      `  'hdfs://localhost:9000${meta?.location || '/user/hive/warehouse/' + table.toLowerCase()}'`
    ];
    return { output: out.join('\n') + '\nOK', isError: false };
  }
}
