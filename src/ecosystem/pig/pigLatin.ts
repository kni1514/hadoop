/**
 * Apache Pig Latin layer for the KSQL (SQLite WebAssembly) shell.
 *
 * Every Pig relation (alias) is stored as a real SQLite table, so relations
 * survive reloads with the rest of the database and can also be queried with
 * plain SQL. GROUP results are kept as metadata (source relation + keys)
 * because SQLite has no bag type; DUMP/DESCRIBE/FOREACH understand them.
 *
 * Everything runs locally in the browser. No network requests.
 */

export interface PigResult {
  output: string;
  isError: boolean;
}

interface GroupMeta {
  source: string;
  keys: string[]; // source column names; empty for GROUP ... ALL
}

interface ColumnInfo {
  name: string;
  type: string;
}

const PIG_KEYWORDS = /^(dump|describe|illustrate|explain|split|aliases|filter|foreach|group|cogroup|order|limit|distinct|join|union|cross|sample)\b/i;

export const PIG_HELP = `Apache Pig Latin commands (end statements with ; - several per line are allowed):
  a = LOAD 'sales.csv' USING PigStorage(',') AS (product:chararray, price:int);
  DUMP a;                                   print every tuple of a relation
  DESCRIBE a;                               show the schema of a relation
  b = FILTER a BY price > 100 AND product matches 'Lap.*';
  c = FOREACH a GENERATE product, price * 2 AS double_price;
  g = GROUP a BY product;    (or GROUP a ALL)
  s = FOREACH g GENERATE group, COUNT(a) AS cnt, SUM(a.price) AS total;
  o = ORDER a BY price DESC;
  l = LIMIT o 3;
  d = DISTINCT a;
  j = JOIN a BY product, b BY product;      columns become a::product, b::price ...
  u = UNION a, b;
  x = CROSS a, b;
  r = SAMPLE a 0.5;
  SPLIT a INTO cheap IF price < 100, costly OTHERWISE;
  ILLUSTRATE a;                             schema plus a few sample tuples
  EXPLAIN a;                                logical plan that built the relation
  ALIASES;                                  list all Pig relations
  STORE a INTO 'out.csv' USING PigStorage(',');
Positional fields work too: $0, $1 ...  Functions: COUNT SUM AVG MIN MAX UPPER LOWER TRIM SIZE CONCAT ROUND ABS`;

function q(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** Split on a separator that is not inside quotes or brackets. */
export function splitTopLevel(text: string, sep: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let quote: string | null = null;
  let current = '';
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      current += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === '(' || ch === '{' || ch === '[') depth++;
    if (ch === ')' || ch === '}' || ch === ']') depth--;
    if (ch === sep && depth === 0) {
      parts.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  parts.push(current);
  return parts.map((p) => p.trim()).filter((p) => p.length > 0);
}

function pigType(sqlType: string): string {
  const t = (sqlType || '').toUpperCase();
  if (t.includes('INT')) return 'int';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB') || t === 'NUM' || t.includes('NUMERIC')) return 'double';
  if (t.includes('TEXT') || t.includes('CHAR')) return 'chararray';
  return 'bytearray';
}

function formatValue(v: any): string {
  if (v === null || v === undefined) return '';
  if (typeof v === 'number' && !Number.isInteger(v)) return String(Math.round(v * 1e6) / 1e6);
  return String(v);
}

export class PigLatin {
  private groups = new Map<string, GroupMeta>();
  private plans = new Map<string, string[]>();

  constructor(private getDb: () => any) {}

  /** True when the statement is Pig Latin handled by this class. */
  public static isPigStatement(stmt: string): boolean {
    const s = stmt.trim().replace(/;\s*$/, '');
    if (/^describe\s+table\b/i.test(s)) return false;
    if (/^[a-zA-Z_]\w*\s*=\s*(filter|foreach|group|cogroup|order|limit|distinct|join|union|cross|sample)\b/i.test(s)) return true;
    if (/^(dump|illustrate|explain)\s+[a-zA-Z_]\w*$/i.test(s)) return true;
    if (/^describe\s+[a-zA-Z_]\w*$/i.test(s)) return true;
    if (/^split\s+[a-zA-Z_]\w*\s+into\b/i.test(s)) return true;
    if (/^aliases$/i.test(s)) return true;
    return false;
  }

  /** Statement looks like Pig (including LOAD/STORE, handled elsewhere). */
  public static looksLikePig(stmt: string): boolean {
    const s = stmt.trim();
    return PigLatin.isPigStatement(s) || /^[a-zA-Z_]\w*\s*=\s*load\b/i.test(s) || /^store\s+\w+\s+into\b/i.test(s) || PIG_KEYWORDS.test(s);
  }

  /** Called by LOAD so EXPLAIN/ALIASES know about loaded relations. */
  public registerLoad(alias: string, path: string): void {
    this.groups.delete(alias);
    this.plans.set(alias, [`LOAD '${path}'  ->  ${alias}`]);
  }

  public execute(rawStmt: string): PigResult {
    const db = this.getDb();
    if (!db) return { output: 'Database not initialized.', isError: true };
    const stmt = rawStmt.trim().replace(/;\s*$/, '').trim();

    try {
      let m: RegExpMatchArray | null;

      if ((m = stmt.match(/^dump\s+(\w+)$/i))) return this.dump(m[1]);
      if ((m = stmt.match(/^describe\s+(\w+)$/i))) return this.describe(m[1]);
      if ((m = stmt.match(/^illustrate\s+(\w+)$/i))) return this.illustrate(m[1]);
      if ((m = stmt.match(/^explain\s+(\w+)$/i))) return this.explain(m[1]);
      if (/^aliases$/i.test(stmt)) return this.aliases();
      if ((m = stmt.match(/^split\s+(\w+)\s+into\s+([\s\S]+)$/i))) return this.split(m[1], m[2]);

      const assign = stmt.match(/^(\w+)\s*=\s*(\w+)\s+([\s\S]*)$/);
      if (!assign) return { output: `Pig: could not parse statement: ${stmt}`, isError: true };
      const target = assign[1];
      const op = assign[2].toLowerCase();
      const rest = assign[3].trim();

      switch (op) {
        case 'filter': return this.filter(target, rest);
        case 'foreach': return this.foreach(target, rest);
        case 'group':
        case 'cogroup': return this.group(target, rest);
        case 'order': return this.order(target, rest);
        case 'limit': return this.limit(target, rest);
        case 'distinct': return this.distinct(target, rest);
        case 'join': return this.join(target, rest);
        case 'union': return this.union(target, rest);
        case 'cross': return this.cross(target, rest);
        case 'sample': return this.sample(target, rest);
        default: return { output: `Pig: unsupported operator ${op.toUpperCase()}`, isError: true };
      }
    } catch (e: any) {
      return { output: 'Pig error: ' + (e?.message || String(e)), isError: true };
    }
  }

  // ---------------------------------------------------------------- helpers

  private exists(alias: string): boolean {
    const r = this.getDb().exec(`SELECT name FROM sqlite_master WHERE (type='table' OR type='view') AND name = '${alias.replace(/'/g, "''")}';`);
    return r.length > 0 && r[0].values.length > 0;
  }

  private columns(alias: string): ColumnInfo[] {
    const r = this.getDb().exec(`PRAGMA table_info(${q(alias)});`);
    if (r.length === 0) return [];
    return r[0].values.map((row: any[]) => {
      const name = String(row[1]);
      let type = String(row[2] || '');
      if (!type || type === 'NUM') {
        // Computed columns (price * qty, COUNT ...) have no declared type: look at the data.
        try {
          const t = this.getDb().exec(`SELECT typeof(${q(name)}) FROM ${q(alias)} WHERE ${q(name)} IS NOT NULL LIMIT 1;`);
          const kind = t.length ? String(t[0].values[0][0]) : '';
          if (kind === 'integer') type = 'INTEGER';
          else if (kind === 'real') type = 'REAL';
          else if (kind === 'text') type = 'TEXT';
        } catch {}
      }
      return { name, type };
    });
  }

  private requireRelation(alias: string): void {
    if (this.groups.has(alias)) return;
    if (!this.exists(alias)) throw new Error(`Undefined alias: ${alias}`);
  }

  private requirePlain(alias: string, op: string): void {
    if (this.groups.has(alias)) {
      throw new Error(`${op} on grouped relation "${alias}" is not supported here; use FOREACH ${alias} GENERATE ... first`);
    }
    if (!this.exists(alias)) throw new Error(`Undefined alias: ${alias}`);
  }

  private store(target: string, selectSql: string, plan: string, sources: string[]): number {
    const db = this.getDb();
    const tmp = `__pig_tmp_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
    db.exec(`CREATE TABLE ${q(tmp)} AS ${selectSql};`);
    db.exec(`DROP TABLE IF EXISTS ${q(target)};`);
    db.exec(`ALTER TABLE ${q(tmp)} RENAME TO ${q(target)};`);
    this.groups.delete(target);
    const lineage: string[] = [];
    for (const s of sources) lineage.push(...(this.plans.get(s) || [`(table ${s})`]));
    this.plans.set(target, [...lineage, `${plan}  ->  ${target}`]);
    const r = db.exec(`SELECT COUNT(*) FROM ${q(target)};`);
    return r.length ? Number(r[0].values[0][0]) : 0;
  }

  private ok(target: string, count: number): PigResult {
    return { output: `Relation "${target}" defined (${count} record${count === 1 ? '' : 's'}). Use DUMP ${target}; to view it.`, isError: false };
  }

  /**
   * Translate a Pig expression into SQLite.
   * cols: columns of the input relation. group: grouping context when the input is a GROUP.
   */
  private translate(expr: string, cols: string[], group?: { bag: string; keys: string[] }): string {
    let e = expr;

    // regex match -> LIKE
    e = e.replace(/(\S+)\s+matches\s+'([^']*)'/gi, (_all, lhs, re) => {
      let pat = String(re).replace(/^\^/, '').replace(/\$$/, '');
      pat = pat.replace(/\.\*/g, '%').replace(/\.\+/g, '_%').replace(/\./g, '_');
      return `${lhs} LIKE '${pat}'`;
    });

    if (group) {
      const bag = group.bag;
      e = e.replace(new RegExp(`\\b(COUNT|COUNT_STAR)\\s*\\(\\s*${bag}\\s*\\)`, 'gi'), 'COUNT(*)');
    }

    const lowerCols = new Map(cols.map((c) => [c.toLowerCase(), c]));
    const suffix = (name: string): string | null => {
      const hits = cols.filter((c) => c.toLowerCase().endsWith('::' + name.toLowerCase()));
      return hits.length === 1 ? hits[0] : null;
    };

    // Work only outside of string literals.
    const pieces = e.split(/('(?:[^']|'')*')/);
    for (let i = 0; i < pieces.length; i += 2) {
      let p = pieces[i];
      p = p.replace(/==/g, '=');
      p = p.replace(/"[^"]*"|\$\d+|\b\w+::\w+\b|\b\w+\.\w+\b|\b[A-Za-z_]\w*\b/g, (tok, offset: number, whole: string) => {
        if (tok.startsWith('"')) return tok;
        if (tok.startsWith('$')) {
          const idx = Number(tok.slice(1));
          if (group) {
            if (idx === 0) return group.keys.length === 1 ? q(group.keys[0]) : "'all'";
          }
          if (idx >= cols.length) throw new Error(`Positional field ${tok} out of range (relation has ${cols.length} fields)`);
          return q(cols[idx]);
        }
        if (tok.includes('::')) {
          const hit = lowerCols.get(tok.toLowerCase());
          return hit ? q(hit) : q(tok);
        }
        if (tok.includes('.')) {
          if (/^\d+\.\d+$/.test(tok)) return tok;
          const [left, right] = tok.split('.');
          if (group && (left.toLowerCase() === group.bag.toLowerCase())) {
            const hit = lowerCols.get(right.toLowerCase()) || suffix(right);
            return hit ? q(hit) : q(right);
          }
          if (group && left.toLowerCase() === 'group') return q(right);
          const prefixed = lowerCols.get(`${left}::${right}`.toLowerCase());
          if (prefixed) return q(prefixed);
          return tok;
        }
        // plain identifier
        const next = whole.slice(offset + tok.length).trimStart();
        if (next.startsWith('(')) return tok; // function call
        if (group && tok.toLowerCase() === 'group') {
          if (group.keys.length === 0) return "'all'";
          if (group.keys.length === 1) return q(group.keys[0]);
          return group.keys.map(q).join(" || ',' || ");
        }
        const hit = lowerCols.get(tok.toLowerCase()) || suffix(tok);
        return hit ? q(hit) : tok;
      });
      // Pig functions -> SQLite
      p = p.replace(/\bSIZE\s*\(/gi, 'LENGTH(');
      p = p.replace(/\bCOUNT_STAR\s*\(/gi, 'COUNT(');
      pieces[i] = p;
    }
    e = pieces.join('');

    // CONCAT(a, b, ...) -> (a || b || ...)
    for (let guard = 0; guard < 10 && /\bCONCAT\s*\(/i.test(e); guard++) {
      e = e.replace(/\bCONCAT\s*\(([^()]*)\)/i, (_all, args) => `(${splitTopLevel(args, ',').join(' || ')})`);
    }
    return e;
  }

  private relationBy(spec: string): { alias: string; keys: string[] } {
    // "a BY key" or "a BY (k1, k2)" (optionally followed by LEFT/RIGHT/OUTER)
    const m = spec.match(/^(\w+)\s+by\s+([\s\S]+?)(\s+(left|right|full)(\s+outer)?)?$/i);
    if (!m) throw new Error(`Expected "<alias> BY <key>" but got "${spec}"`);
    let keyText = m[2].trim();
    if (keyText.startsWith('(') && keyText.endsWith(')')) keyText = keyText.slice(1, -1);
    return { alias: m[1], keys: splitTopLevel(keyText, ',') };
  }

  private resolveKey(key: string, cols: string[]): string {
    const k = key.trim();
    if (/^\$\d+$/.test(k)) {
      const idx = Number(k.slice(1));
      if (idx >= cols.length) throw new Error(`Positional field ${k} out of range`);
      return cols[idx];
    }
    const hit = cols.find((c) => c.toLowerCase() === k.toLowerCase()) || cols.find((c) => c.toLowerCase().endsWith('::' + k.toLowerCase()));
    if (!hit) throw new Error(`Column "${k}" not found. Available: ${cols.join(', ')}`);
    return hit;
  }

  // -------------------------------------------------------------- commands

  /** DUMP alias */
  private dump(alias: string): PigResult {
    const db = this.getDb();
    const g = this.groups.get(alias);
    if (g) {
      const cols = this.columns(g.source).map((c) => c.name);
      const order = g.keys.length ? ` ORDER BY ${g.keys.map(q).join(', ')}` : '';
      const r = db.exec(`SELECT * FROM ${q(g.source)}${order};`);
      const rows: any[][] = r.length ? r[0].values : [];
      const keyIdx = g.keys.map((k) => cols.indexOf(k));
      const bags = new Map<string, string[]>();
      for (const row of rows) {
        const keyVals = keyIdx.map((i) => formatValue(row[i]));
        const key = g.keys.length === 0 ? 'all' : keyVals.length === 1 ? keyVals[0] : `(${keyVals.join(',')})`;
        if (!bags.has(key)) bags.set(key, []);
        bags.get(key)!.push(`(${row.map(formatValue).join(',')})`);
      }
      if (bags.size === 0) return { output: '(no records)', isError: false };
      const out: string[] = [];
      bags.forEach((tuples, key) => out.push(`(${key},{${tuples.join(',')}})`));
      return { output: out.join('\n'), isError: false };
    }
    this.requireRelation(alias);
    const r = db.exec(`SELECT * FROM ${q(alias)};`);
    if (r.length === 0 || r[0].values.length === 0) return { output: '(no records)', isError: false };
    return { output: r[0].values.map((row: any[]) => `(${row.map(formatValue).join(',')})`).join('\n'), isError: false };
  }

  private schemaString(alias: string): string {
    const g = this.groups.get(alias);
    if (g) {
      const inner = this.columns(g.source).map((c) => `${c.name}: ${pigType(c.type)}`).join(',');
      let groupType = 'chararray';
      if (g.keys.length === 1) {
        const c = this.columns(g.source).find((x) => x.name === g.keys[0]);
        groupType = pigType(c?.type || '');
      } else if (g.keys.length > 1) {
        groupType = `(${g.keys.join(',')})`;
      }
      return `${alias}: {group: ${groupType},${g.source}: {(${inner})}}`;
    }
    const cols = this.columns(alias).map((c) => `${c.name}: ${pigType(c.type)}`).join(',');
    return `${alias}: {${cols}}`;
  }

  /** DESCRIBE alias */
  private describe(alias: string): PigResult {
    this.requireRelation(alias);
    return { output: this.schemaString(alias), isError: false };
  }

  /** ILLUSTRATE alias */
  private illustrate(alias: string): PigResult {
    this.requireRelation(alias);
    const g = this.groups.get(alias);
    const src = g ? g.source : alias;
    const cols = this.columns(src).map((c) => c.name);
    const r = this.getDb().exec(`SELECT * FROM ${q(src)} LIMIT 3;`);
    const rows: any[][] = r.length ? r[0].values : [];
    const widths = cols.map((c, i) => Math.max(c.length, ...rows.map((row) => formatValue(row[i]).length)));
    const line = '-'.repeat(widths.reduce((a, b) => a + b + 3, 1));
    const fmt = (vals: string[]) => '| ' + vals.map((v, i) => v.padEnd(widths[i])).join(' | ') + ' |';
    const out = [line, `| ${alias}${g ? ` (grouped from ${src})` : ''}`, line, fmt(cols), line];
    rows.forEach((row) => out.push(fmt(row.map(formatValue))));
    out.push(line);
    out.push(this.schemaString(alias));
    return { output: out.join('\n'), isError: false };
  }

  /** EXPLAIN alias */
  private explain(alias: string): PigResult {
    this.requireRelation(alias);
    const plan = this.plans.get(alias) || [`(table ${alias})`];
    const out = ['#-----------------------------------------------', `# Logical Plan for ${alias}:`, '#-----------------------------------------------'];
    plan.forEach((step, i) => out.push(`${'  '.repeat(i)}|---${step}`));
    out.push('', '# Executed locally by the SQLite WebAssembly engine (no cluster, no network).');
    return { output: out.join('\n'), isError: false };
  }

  /** ALIASES */
  private aliases(): PigResult {
    const names = new Set<string>([...this.plans.keys(), ...this.groups.keys()]);
    const live = [...names].filter((n) => this.groups.has(n) || this.exists(n));
    if (live.length === 0) return { output: 'No Pig relations defined yet. Start with: a = LOAD \'file.csv\' USING PigStorage(\',\');', isError: false };
    return { output: 'Pig relations:\n' + live.map((n) => '- ' + this.schemaString(n)).join('\n'), isError: false };
  }

  /** y = FILTER x BY cond */
  private filter(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)\s+by\s+([\s\S]+)$/i);
    if (!m) return { output: 'Usage: b = FILTER a BY <condition>;', isError: true };
    this.requirePlain(m[1], 'FILTER');
    const cols = this.columns(m[1]).map((c) => c.name);
    const cond = this.translate(m[2], cols);
    const n = this.store(target, `SELECT * FROM ${q(m[1])} WHERE ${cond}`, `FILTER ${m[1]} BY ${m[2].trim()}`, [m[1]]);
    return this.ok(target, n);
  }

  /** y = FOREACH x GENERATE e1 [AS n1], ... */
  private foreach(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)\s+generate\s+([\s\S]+)$/i);
    if (!m) return { output: 'Usage: b = FOREACH a GENERATE col1, col2 AS name;', isError: true };
    const source = m[1];
    this.requireRelation(source);
    const g = this.groups.get(source);
    const baseTable = g ? g.source : source;
    const cols = this.columns(baseTable).map((c) => c.name);
    const ctx = g ? { bag: g.source, keys: g.keys } : undefined;

    const items: string[] = [];
    const used = new Set<string>();
    const uniq = (name: string) => {
      let n = name.replace(/[^A-Za-z0-9_:]/g, '_').replace(/^_+|_+$/g, '') || 'col';
      let k = n;
      let i = 2;
      while (used.has(k.toLowerCase())) k = `${n}_${i++}`;
      used.add(k.toLowerCase());
      return k;
    };

    for (const rawItem of splitTopLevel(m[2], ',')) {
      let item = rawItem.trim();
      let alias: string | null = null;
      const asM = item.match(/^([\s\S]+?)\s+as\s+\(?\s*(\w+)(?:\s*:\s*\w+)?\s*\)?$/i);
      if (asM) {
        item = asM[1].trim();
        alias = asM[2];
      }
      if (item === '*') {
        cols.forEach((c) => items.push(`${q(c)} AS ${q(uniq(c))}`));
        continue;
      }
      const flat = item.match(/^flatten\s*\(\s*group\s*\)$/i);
      if (flat && g) {
        g.keys.forEach((k) => items.push(`${q(k)} AS ${q(uniq(k))}`));
        continue;
      }
      const sql = this.translate(item.replace(/^flatten\s*\(([\s\S]*)\)$/i, '$1'), cols, ctx);
      let name = alias;
      if (!name) {
        const simple = item.match(/^(?:\w+\.|\w+::)?(\w+)$/);
        if (simple) name = simple[1];
        else if (/^\$\d+$/.test(item) && !g) name = cols[Number(item.slice(1))] || 'col';
        else {
          const fn = item.match(/^(\w+)\s*\(\s*(?:\w+\.)?(\w*)/);
          name = fn ? `${fn[1].toLowerCase()}${fn[2] ? '_' + fn[2] : ''}` : 'col';
        }
      }
      items.push(`${sql} AS ${q(uniq(name))}`);
    }

    let sql = `SELECT ${items.join(', ')} FROM ${q(baseTable)}`;
    if (g && g.keys.length) sql += ` GROUP BY ${g.keys.map(q).join(', ')} ORDER BY ${g.keys.map(q).join(', ')}`;
    const n = this.store(target, sql, `FOREACH ${source} GENERATE ${m[2].trim()}`, [source]);
    return this.ok(target, n);
  }

  /** y = GROUP x BY key | (k1,k2) | ALL */
  private group(target: string, rest: string): PigResult {
    const all = rest.match(/^(\w+)\s+all$/i);
    let source: string;
    let keys: string[] = [];
    if (all) {
      source = all[1];
      this.requirePlain(source, 'GROUP');
    } else {
      const spec = this.relationBy(rest);
      source = spec.alias;
      this.requirePlain(source, 'GROUP');
      const cols = this.columns(source).map((c) => c.name);
      keys = spec.keys.map((k) => this.resolveKey(k, cols));
    }
    this.getDb().exec(`DROP TABLE IF EXISTS ${q(target)};`);
    this.groups.set(target, { source, keys });
    const lineage = this.plans.get(source) || [`(table ${source})`];
    this.plans.set(target, [...lineage, `GROUP ${source} ${keys.length ? 'BY ' + keys.join(', ') : 'ALL'}  ->  ${target}`]);
    const r = this.getDb().exec(keys.length ? `SELECT COUNT(*) FROM (SELECT 1 FROM ${q(source)} GROUP BY ${keys.map(q).join(', ')});` : `SELECT 1;`);
    const n = r.length ? Number(r[0].values[0][0]) : 0;
    return { output: `Relation "${target}" defined (${n} group${n === 1 ? '' : 's'}). Use DUMP ${target}; or FOREACH ${target} GENERATE group, COUNT(${source});`, isError: false };
  }

  /** y = ORDER x BY c [ASC|DESC], ... */
  private order(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)\s+by\s+([\s\S]+)$/i);
    if (!m) return { output: 'Usage: b = ORDER a BY col DESC;', isError: true };
    this.requirePlain(m[1], 'ORDER');
    const cols = this.columns(m[1]).map((c) => c.name);
    const parts = splitTopLevel(m[2], ',').map((p) => {
      const pm = p.match(/^(\S+)(?:\s+(asc|desc))?$/i);
      if (!pm) throw new Error(`Bad ORDER key: ${p}`);
      return `${q(this.resolveKey(pm[1], cols))} ${(pm[2] || 'ASC').toUpperCase()}`;
    });
    const n = this.store(target, `SELECT * FROM ${q(m[1])} ORDER BY ${parts.join(', ')}`, `ORDER ${m[1]} BY ${m[2].trim()}`, [m[1]]);
    return this.ok(target, n);
  }

  /** y = LIMIT x n */
  private limit(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)\s+(\d+)$/i);
    if (!m) return { output: 'Usage: b = LIMIT a 10;', isError: true };
    this.requirePlain(m[1], 'LIMIT');
    const n = this.store(target, `SELECT * FROM ${q(m[1])} LIMIT ${Number(m[2])}`, `LIMIT ${m[1]} ${m[2]}`, [m[1]]);
    return this.ok(target, n);
  }

  /** y = DISTINCT x */
  private distinct(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)$/);
    if (!m) return { output: 'Usage: b = DISTINCT a;', isError: true };
    this.requirePlain(m[1], 'DISTINCT');
    const n = this.store(target, `SELECT DISTINCT * FROM ${q(m[1])}`, `DISTINCT ${m[1]}`, [m[1]]);
    return this.ok(target, n);
  }

  private prefixedSelect(aliases: string[]): string {
    return aliases
      .map((a) => this.columns(a).map((c) => `${q(a)}.${q(c.name)} AS ${q(c.name.includes('::') ? c.name : `${a}::${c.name}`)}`).join(', '))
      .join(', ');
  }

  /** y = JOIN a BY k [LEFT [OUTER]], b BY k */
  private join(target: string, rest: string): PigResult {
    const specs = splitTopLevel(rest, ',').map((s) => this.relationBy(s));
    if (specs.length !== 2) return { output: 'Usage: j = JOIN a BY key, b BY key;  (two relations)', isError: true };
    const [a, b] = specs;
    this.requirePlain(a.alias, 'JOIN');
    this.requirePlain(b.alias, 'JOIN');
    if (a.alias === b.alias) return { output: 'Pig: self-join needs two different aliases (copy it first with FOREACH a GENERATE *).', isError: true };
    if (a.keys.length !== b.keys.length) return { output: 'Pig: both sides of JOIN need the same number of keys.', isError: true };
    const aCols = this.columns(a.alias).map((c) => c.name);
    const bCols = this.columns(b.alias).map((c) => c.name);
    const on = a.keys.map((k, i) => `${q(a.alias)}.${q(this.resolveKey(k, aCols))} = ${q(b.alias)}.${q(this.resolveKey(b.keys[i], bCols))}`).join(' AND ');
    const left = /\bleft\b/i.test(rest.split(',')[0]) ? 'LEFT JOIN' : 'JOIN';
    const sql = `SELECT ${this.prefixedSelect([a.alias, b.alias])} FROM ${q(a.alias)} ${left} ${q(b.alias)} ON ${on}`;
    const n = this.store(target, sql, `JOIN ${rest.trim()}`, [a.alias, b.alias]);
    return this.ok(target, n);
  }

  /** y = UNION a, b, ... */
  private union(target: string, rest: string): PigResult {
    const names = splitTopLevel(rest.replace(/^onschema\s+/i, ''), ',');
    if (names.length < 2) return { output: 'Usage: u = UNION a, b;', isError: true };
    names.forEach((n) => this.requirePlain(n, 'UNION'));
    const width = this.columns(names[0]).length;
    for (const n of names) {
      if (this.columns(n).length !== width) return { output: `Pig: UNION needs relations with the same number of fields (${names[0]} has ${width}, ${n} has ${this.columns(n).length}).`, isError: true };
    }
    const sql = names.map((n) => `SELECT * FROM ${q(n)}`).join(' UNION ALL ');
    const count = this.store(target, sql, `UNION ${names.join(', ')}`, names);
    return this.ok(target, count);
  }

  /** y = CROSS a, b */
  private cross(target: string, rest: string): PigResult {
    const names = splitTopLevel(rest, ',');
    if (names.length < 2) return { output: 'Usage: x = CROSS a, b;', isError: true };
    names.forEach((n) => this.requirePlain(n, 'CROSS'));
    if (new Set(names).size !== names.length) return { output: 'Pig: CROSS needs different aliases.', isError: true };
    const sql = `SELECT ${this.prefixedSelect(names)} FROM ${names.map(q).join(' CROSS JOIN ')}`;
    const count = this.store(target, sql, `CROSS ${names.join(', ')}`, names);
    return this.ok(target, count);
  }

  /** y = SAMPLE x fraction */
  private sample(target: string, rest: string): PigResult {
    const m = rest.match(/^(\w+)\s+([0-9]*\.?[0-9]+)$/);
    if (!m) return { output: 'Usage: s = SAMPLE a 0.5;   (fraction between 0 and 1)', isError: true };
    const frac = Number(m[2]);
    if (!(frac >= 0 && frac <= 1)) return { output: 'Pig: SAMPLE fraction must be between 0 and 1.', isError: true };
    this.requirePlain(m[1], 'SAMPLE');
    const sql = `SELECT * FROM ${q(m[1])} WHERE (ABS(RANDOM()) % 1000000) < ${Math.round(frac * 1000000)}`;
    const n = this.store(target, sql, `SAMPLE ${m[1]} ${m[2]}`, [m[1]]);
    return this.ok(target, n);
  }

  /** SPLIT x INTO a IF cond, b IF cond [, c OTHERWISE] */
  private split(source: string, rest: string): PigResult {
    this.requirePlain(source, 'SPLIT');
    const cols = this.columns(source).map((c) => c.name);
    const branches = splitTopLevel(rest, ',');
    const conds: string[] = [];
    const out: string[] = [];
    for (const br of branches) {
      const ifM = br.match(/^(\w+)\s+if\s+([\s\S]+)$/i);
      const otherM = br.match(/^(\w+)\s+otherwise$/i);
      if (ifM) {
        const cond = this.translate(ifM[2], cols);
        conds.push(`(${cond})`);
        const n = this.store(ifM[1], `SELECT * FROM ${q(source)} WHERE ${cond}`, `SPLIT ${source} IF ${ifM[2].trim()}`, [source]);
        out.push(`Relation "${ifM[1]}" defined (${n} records).`);
      } else if (otherM) {
        const cond = conds.length ? `NOT (${conds.join(' OR ')})` : '1';
        const n = this.store(otherM[1], `SELECT * FROM ${q(source)} WHERE ${cond}`, `SPLIT ${source} OTHERWISE`, [source]);
        out.push(`Relation "${otherM[1]}" defined (${n} records).`);
      } else {
        return { output: `Usage: SPLIT a INTO x IF cond, y IF cond, z OTHERWISE;  (bad branch: ${br})`, isError: true };
      }
    }
    return { output: out.join('\n'), isError: false };
  }
}
