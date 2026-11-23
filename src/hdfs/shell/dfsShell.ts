/**
 * `hdfs dfs` / `hadoop fs` shell, implemented against the simulated NameNode.
 *
 * Every command works on the real in-browser HDFS state (namespace + blocks), follows
 * Apache Hadoop 3.3 semantics and error messages, and persists changes through the
 * callbacks in DfsContext. Nothing here touches the network.
 */
import { NameNode } from '../namenode/nameNode';
import { INode, INodeDirectory, INodeFile } from '../../core/domain/types';

export interface DfsContext {
  nameNode: NameNode;
  user: string;
  /** Local (Linux) file system helpers, paths may be relative to the shell's cwd. */
  readLocal(path: string): string | undefined;
  writeLocal(path: string, content: string): void;
  deleteLocal(path: string): boolean;
  isLocalDir(path: string): boolean;
  makeLocalDir(path: string): void;
  /** Absolute paths of local files under a local directory (recursive). */
  listLocalFilesUnder(path: string): string[];
  resolveLocal(path: string): string;
  /** Persistence of HDFS content. */
  persistFile(path: string, content: string): void;
  persistDir(path: string): void;
  forget(path: string): void;
  persistMeta(): void;
  /** Effective configuration values (core-site/hdfs-site). */
  conf(key: string, fallback: string): string;
  inSafeMode(): boolean;
  setExitCode(code: number): void;
}

type Flags = Set<string>;

const DIR_MARK = '__HADOOP_DIR__';

export function normalizeHdfsPath(p: string): string {
  let path = (p || '').trim();
  path = path.replace(/^hdfs:\/\/[^/]*/i, '');
  if (path === '' || path === '.') return '/';
  if (!path.startsWith('/')) path = '/' + path;
  const parts: string[] = [];
  for (const part of path.split('/')) {
    if (!part || part === '.') continue;
    if (part === '..') parts.pop();
    else parts.push(part);
  }
  return '/' + parts.join('/');
}

function pad2(n: number): string {
  return n < 10 ? '0' + n : String(n);
}

/** Hadoop prints local time as yyyy-MM-dd HH:mm. */
export function hdfsDate(ms: number, withSeconds = false): string {
  const d = new Date(ms);
  const base = `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
  return withSeconds ? `${base}:${pad2(d.getSeconds())}` : base;
}

export function humanSize(bytes: number): string {
  if (bytes < 1024) return String(bytes);
  const units = ['K', 'M', 'G', 'T', 'P'];
  let v = bytes;
  let i = -1;
  do {
    v /= 1024;
    i++;
  } while (v >= 1024 && i < units.length - 1);
  return `${v.toFixed(1)} ${units[i]}`;
}

function globToRegExp(glob: string): RegExp {
  let re = '';
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i];
    if (c === '*') re += '[^/]*';
    else if (c === '?') re += '[^/]';
    else if (c === '[') {
      const end = glob.indexOf(']', i);
      if (end > i) {
        re += '[' + glob.slice(i + 1, end).replace(/^!/, '^').replace(/\\/g, '\\\\') + ']';
        i = end;
      } else re += '\\[';
    } else if (c === '{') {
      const end = glob.indexOf('}', i);
      if (end > i) {
        re += '(' + glob.slice(i + 1, end).split(',').map((s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')';
        i = end;
      } else re += '\\{';
    } else re += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp('^' + re + '$');
}

const hasGlob = (s: string) => /[*?[{]/.test(s);

/** Convert 755 / u+x,go-w / a=r style modes to rwx strings. */
export function applyMode(current: string, mode: string, isDir: boolean): string {
  const prefix = isDir ? 'd' : '-';
  let bits = current.slice(1).split('');
  while (bits.length < 9) bits.push('-');
  if (/^[0-7]{3,4}$/.test(mode)) {
    const digits = mode.slice(-3).split('').map(Number);
    const out: string[] = [];
    for (const d of digits) {
      out.push(d & 4 ? 'r' : '-', d & 2 ? 'w' : '-', d & 1 ? 'x' : '-');
    }
    if (mode.length === 4 && mode[0] === '1') out[8] = out[8] === 'x' ? 't' : 'T';
    return prefix + out.join('');
  }
  for (const clause of mode.split(',')) {
    const m = clause.match(/^([ugoa]*)([+=-])([rwxXt]*)$/);
    if (!m) throw new Error(`chmod : mode '${mode}' does not match the expected pattern.`);
    const who = m[1] || 'a';
    const targets: number[] = [];
    if (who.includes('u') || who.includes('a')) targets.push(0);
    if (who.includes('g') || who.includes('a')) targets.push(3);
    if (who.includes('o') || who.includes('a')) targets.push(6);
    for (const base of targets) {
      const perms = ['r', 'w', 'x'];
      perms.forEach((p, i) => {
        const want = m[3].includes(p) || (p === 'x' && m[3].includes('X') && (isDir || bits.includes('x')));
        if (m[2] === '=') bits[base + i] = want ? p : '-';
        else if (want) bits[base + i] = m[2] === '+' ? p : '-';
      });
    }
  }
  return prefix + bits.join('');
}

/** Small, dependency-free MD5 used by -checksum. */
function md5(input: string): string {
  const bytes = new TextEncoder().encode(input);
  const words: number[] = [];
  for (let i = 0; i < bytes.length; i++) words[i >> 2] |= bytes[i] << ((i % 4) * 8);
  const bitLen = bytes.length * 8;
  words[bitLen >> 5] |= 0x80 << bitLen % 32;
  words[(((bitLen + 64) >>> 9) << 4) + 14] = bitLen;
  const S = [7, 12, 17, 22, 5, 9, 14, 20, 4, 11, 16, 23, 6, 10, 15, 21];
  const K = Array.from({ length: 64 }, (_, i) => Math.floor(Math.abs(Math.sin(i + 1)) * 2 ** 32) | 0);
  let [a0, b0, c0, d0] = [0x67452301, 0xefcdab89 | 0, 0x98badcfe | 0, 0x10325476];
  for (let i = 0; i < words.length; i += 16) {
    let [a, b, c, d] = [a0, b0, c0, d0];
    for (let j = 0; j < 64; j++) {
      let f: number;
      let g: number;
      if (j < 16) { f = (b & c) | (~b & d); g = j; }
      else if (j < 32) { f = (d & b) | (~d & c); g = (5 * j + 1) % 16; }
      else if (j < 48) { f = b ^ c ^ d; g = (3 * j + 5) % 16; }
      else { f = c ^ (b | ~d); g = (7 * j) % 16; }
      const tmp = d;
      d = c;
      c = b;
      const sum = (a + f + K[j] + (words[i + g] | 0)) | 0;
      const s = S[(j >> 4) * 4 + (j % 4)];
      b = (b + ((sum << s) | (sum >>> (32 - s)))) | 0;
      a = tmp;
    }
    a0 = (a0 + a) | 0;
    b0 = (b0 + b) | 0;
    c0 = (c0 + c) | 0;
    d0 = (d0 + d) | 0;
  }
  return [a0, b0, c0, d0]
    .map((n) => Array.from({ length: 4 }, (_, i) => ((n >>> (i * 8)) & 0xff).toString(16).padStart(2, '0')).join(''))
    .join('');
}

const WRITE_ACTIONS = new Set([
  '-put', '-copyFromLocal', '-moveFromLocal', '-mkdir', '-touchz', '-touch', '-cp', '-mv', '-rm', '-rmr', '-rmdir',
  '-setrep', '-chmod', '-chown', '-chgrp', '-appendToFile', '-truncate'
]);

export class DfsShell {
  constructor(private ctx: DfsContext) {}

  private get ns() {
    return this.ctx.nameNode.getNamespace();
  }

  private node(path: string): INode | undefined {
    return this.ns.resolvePath(normalizeHdfsPath(path));
  }

  /** Expand globs; a path without glob characters is returned as-is (even when missing). */
  private expand(pattern: string): string[] {
    const norm = normalizeHdfsPath(pattern);
    if (!hasGlob(norm)) return [norm];
    const parts = norm.split('/').filter(Boolean);
    let current: string[] = ['/'];
    for (const part of parts) {
      const next: string[] = [];
      for (const base of current) {
        const dir = this.ns.resolvePath(base);
        if (!dir || dir.type !== 'DIRECTORY') continue;
        if (hasGlob(part)) {
          const re = globToRegExp(part);
          [...dir.children.keys()].sort().forEach((name) => {
            if (re.test(name)) next.push((base === '/' ? '' : base) + '/' + name);
          });
        } else if (dir.children.has(part)) {
          next.push((base === '/' ? '' : base) + '/' + part);
        }
      }
      current = next;
    }
    return current;
  }

  private walk(node: INode, visit: (n: INode) => void): void {
    visit(node);
    if (node.type === 'DIRECTORY') {
      [...node.children.values()].sort((a, b) => a.name.localeCompare(b.name)).forEach((c) => this.walk(c, visit));
    }
  }

  private totals(node: INode): { dirs: number; files: number; bytes: number; consumed: number } {
    const t = { dirs: 0, files: 0, bytes: 0, consumed: 0 };
    this.walk(node, (n) => {
      if (n.type === 'DIRECTORY') t.dirs++;
      else {
        t.files++;
        t.bytes += n.sizeBytes;
        t.consumed += n.sizeBytes * n.replicationFactor;
      }
    });
    return t;
  }

  private persistTree(path: string): void {
    const node = this.node(path);
    if (!node) return;
    this.walk(node, (n) => {
      if (n.type === 'FILE') this.ctx.persistFile(n.path, this.ctx.nameNode.readFileContent(n.path) ?? '');
      else if (n.path !== '/') this.ctx.persistDir(n.path);
    });
  }

  private forgetTree(path: string): void {
    const node = this.node(path);
    if (!node) return;
    this.walk(node, (n) => this.ctx.forget(n.path));
  }

  private writeFile(path: string, content: string, replication?: number): INodeFile {
    const rep = replication ?? (parseInt(this.ctx.conf('dfs.replication', '3'), 10) || 3);
    const blockSize = parseInt(this.ctx.conf('dfs.blocksize', '134217728'), 10) || 134217728;
    const file = this.ctx.nameNode.createAndWriteFile(path, content, rep, blockSize);
    file.owner = this.ctx.user;
    this.ctx.persistFile(file.path, content);
    return file;
  }

  private lsLine(n: INode, sizeWidth: number, human: boolean, showPath: string): string {
    const rep = n.type === 'FILE' ? String(n.replicationFactor) : '-';
    const size = n.type === 'FILE' ? n.sizeBytes : 0;
    const sizeStr = human ? humanSize(size) : String(size);
    return `${n.permissions}   ${rep.padStart(1)} ${n.owner} ${n.group} ${sizeStr.padStart(sizeWidth)} ${hdfsDate(n.modifiedAt)} ${showPath}`;
  }

  /** Returns null for actions this module does not implement. */
  public run(action: string, flags: Flags, args: string[]): string | null {
    this.ctx.setExitCode(0);
    const name = action.replace(/^-/, '');
    if (WRITE_ACTIONS.has(action) && this.ctx.inSafeMode()) {
      this.ctx.setExitCode(1);
      return `${name}: Cannot ${name} ${args[args.length - 1] || ''}. Name node is in safe mode.\nIt was turned on manually. Use "hdfs dfsadmin -safemode leave" to turn safe mode off.`;
    }
    const fail = (msg: string) => {
      this.ctx.setExitCode(1);
      return msg;
    };
    try {
      switch (action) {
        case '-ls':
        case '-lsr':
          return this.ls(args, action === '-lsr' ? new Set([...flags, '-R']) : flags, fail);
        case '-mkdir':
          return this.mkdir(args, flags, fail);
        case '-put':
        case '-copyFromLocal':
        case '-moveFromLocal':
          return this.put(action, args, flags, fail);
        case '-get':
        case '-copyToLocal':
          return this.get(args, flags, fail);
        case '-moveToLocal':
          return fail(`moveToLocal: Option '-moveToLocal' is not implemented yet.`);
        case '-cat':
        case '-text':
          return this.cat(name, args, fail);
        case '-head':
        case '-tail':
          return this.headTail(name, args, fail);
        case '-rm':
        case '-rmr':
          return this.rm(args, action === '-rmr' ? new Set([...flags, '-r']) : flags, fail);
        case '-rmdir':
          return this.rmdir(args, flags, fail);
        case '-cp':
          return this.cp(args, flags, fail);
        case '-mv':
          return this.mv(args, fail);
        case '-touchz':
        case '-touch':
          return this.touch(name, args, fail);
        case '-appendToFile':
          return this.append(args, fail);
        case '-getmerge':
          return this.getmerge(args, flags, fail);
        case '-du':
        case '-dus':
          return this.du(args, action === '-dus' ? new Set([...flags, '-s']) : flags, fail);
        case '-count':
          return this.count(args, flags, fail);
        case '-df':
          return this.df(flags);
        case '-stat':
          return this.stat(args, fail);
        case '-test':
          return this.test(flags, args, fail);
        case '-setrep':
          return this.setrep(args, flags, fail);
        case '-chmod':
          return this.chmod(args, flags, fail);
        case '-chown':
          return this.chown(args, flags, fail, false);
        case '-chgrp':
          return this.chown(args, flags, fail, true);
        case '-find':
          return this.find(args, fail);
        case '-checksum':
          return this.checksum(args, fail);
        case '-truncate':
          return this.truncate(args, fail);
        case '-expunge':
          return '';
        case '-usage':
        case '-help':
          return this.help(args[0]);
        default:
          return null;
      }
    } catch (err: any) {
      return fail(`${name}: ${err?.message || String(err)}`);
    }
  }

  private ls(args: string[], flags: Flags, fail: (m: string) => string): string {
    const recursive = flags.has('-R');
    const human = flags.has('-h');
    const dirOnly = flags.has('-d');
    const pathOnly = flags.has('-C');
    const sortTime = flags.has('-t');
    const sortSize = flags.has('-S');
    const reverse = flags.has('-r');
    const targets = args.length ? args : ['.'];
    const out: string[] = [];
    for (const pattern of targets) {
      const matches = this.expand(pattern);
      const existing = matches.filter((p) => this.node(p));
      if (existing.length === 0) {
        out.push(fail(`ls: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of existing) {
        const n = this.node(p)!;
        let entries: INode[];
        if (n.type === 'FILE' || dirOnly) entries = [n];
        else if (recursive) {
          entries = [];
          this.walk(n, (c) => {
            if (c !== n) entries.push(c);
          });
        } else entries = [...n.children.values()].sort((a, b) => a.name.localeCompare(b.name));
        if (sortTime) entries.sort((a, b) => b.modifiedAt - a.modifiedAt);
        if (sortSize) entries.sort((a, b) => (b.type === 'FILE' ? b.sizeBytes : 0) - (a.type === 'FILE' ? a.sizeBytes : 0));
        if (reverse) entries.reverse();
        const width = Math.max(1, ...entries.map((e) => (human ? humanSize(e.type === 'FILE' ? e.sizeBytes : 0) : String(e.type === 'FILE' ? e.sizeBytes : 0)).length));
        if (n.type === 'DIRECTORY' && !dirOnly && !recursive && !pathOnly) out.push(`Found ${entries.length} items`);
        // Like Hadoop: a relative argument lists relative paths, an absolute one lists full paths.
        const relative = !pattern.startsWith('/') && !/^hdfs:/i.test(pattern);
        const show = (e: INode) => (relative ? e.path.replace(/^\//, '') : e.path);
        for (const e of entries) out.push(pathOnly ? show(e) : this.lsLine(e, Math.max(width, 10), human, show(e)));
      }
    }
    return out.join('\n');
  }

  private mkdir(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (!args.length) return fail('-mkdir: Not enough arguments: expected 1 but got 0\nUsage: hadoop fs [generic options] -mkdir [-p] <path> ...');
    const out: string[] = [];
    for (const raw of args) {
      const p = normalizeHdfsPath(raw);
      const existing = this.node(p);
      if (existing) {
        if (!flags.has('-p')) out.push(fail(`mkdir: \`${p}': File exists`));
        continue;
      }
      const parent = p.substring(0, p.lastIndexOf('/')) || '/';
      if (!flags.has('-p') && !this.node(parent)) {
        out.push(fail(`mkdir: \`${p}': No such file or directory`));
        continue;
      }
      const dir = this.ns.mkdir(p, true, this.ctx.user);
      dir.owner = this.ctx.user;
      this.persistTree(p);
      out.push(`✓ Created HDFS directory: ${raw}`);
    }
    return out.join('\n');
  }

  private put(action: string, args: string[], flags: Flags, fail: (m: string) => string): string {
    const verb = action.replace(/^-/, '');
    if (args.length < 1) return fail(`-${verb}: Not enough arguments: expected 1 but got 0`);
    const sources = args.length === 1 ? args : args.slice(0, -1);
    const dstArg = args.length === 1 ? '.' : args[args.length - 1];
    const dst = normalizeHdfsPath(dstArg);
    const dstNode = this.node(dst);
    const dstIsDir = (dstNode && dstNode.type === 'DIRECTORY') || dstArg.endsWith('/');
    if (sources.length > 1 && !dstIsDir) return fail(`${verb}: \`${dst}': Is not a directory`);
    if (dstArg.endsWith('/') && !dstNode) return fail(`${verb}: \`${dst}': No such file or directory`);
    const parent = dst.substring(0, dst.lastIndexOf('/')) || '/';
    if (!dstNode && !this.node(parent)) return fail(`${verb}: \`${dst}': No such file or directory`);

    const force = flags.has('-f');
    const out: string[] = [];
    for (const src of sources) {
      const base = src.replace(/\/+$/, '').split('/').pop() || src;
      const target = dstIsDir ? (dst === '/' ? '' : dst) + '/' + base : dst;
      if (this.ctx.isLocalDir(src)) {
        if (this.node(target) && !force) {
          out.push(fail(`${verb}: \`${target}': File exists`));
          continue;
        }
        const root = this.ctx.resolveLocal(src).replace(/\/+$/, '');
        this.ns.mkdir(target, true, this.ctx.user);
        this.ctx.persistDir(target);
        const files = this.ctx.listLocalFilesUnder(src);
        for (const f of files) {
          this.writeFile(target + f.substring(root.length), this.ctx.readLocal(f) ?? '');
        }
        if (action === '-moveFromLocal') files.forEach((f) => this.ctx.deleteLocal(f));
        out.push(`✓ Copied from Local [${src}] -> HDFS [${target}] (${files.length} files)`);
        continue;
      }
      const content = this.ctx.readLocal(src);
      if (content === undefined) {
        out.push(fail(`${verb}: \`${src}': No such file or directory`));
        continue;
      }
      if (this.node(target)) {
        if (!force) {
          out.push(fail(`${verb}: \`${target}': File exists`));
          continue;
        }
        if (this.node(target)!.type === 'DIRECTORY') {
          out.push(fail(`${verb}: \`${target}': Is a directory`));
          continue;
        }
      }
      const file = this.writeFile(target, content);
      if (action === '-moveFromLocal') this.ctx.deleteLocal(src);
      out.push(`✓ Copied from Local [${src}] -> HDFS [${dstArg === dst || !dstIsDir ? dstArg : target}] (${file.replicationFactor} replicas allocated across DataNodes)`);
    }
    return out.join('\n');
  }

  private get(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (args.length < 1) return fail('-get: Not enough arguments: expected 1 but got 0');
    const sources = args.length === 1 ? args : args.slice(0, -1);
    const localDst = args.length === 1 ? '.' : args[args.length - 1];
    const dstIsDir = localDst === '.' || localDst.endsWith('/') || this.ctx.isLocalDir(localDst);
    const out: string[] = [];
    for (const pattern of sources) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (matches.length === 0) {
        out.push(fail(`get: \`${pattern}': No such file or directory`));
        continue;
      }
      if (matches.length > 1 && !dstIsDir) return fail(`get: \`${localDst}': Is not a directory`);
      for (const p of matches) {
        const n = this.node(p)!;
        const target = dstIsDir ? `${localDst.replace(/\/+$/, '') === '.' ? '' : localDst.replace(/\/+$/, '') + '/'}${n.name}` : localDst;
        if (n.type === 'DIRECTORY') {
          this.ctx.makeLocalDir(target);
          this.walk(n, (c) => {
            const rel = target + c.path.substring(n.path.length);
            if (c.type === 'DIRECTORY') this.ctx.makeLocalDir(rel);
            else this.ctx.writeLocal(rel, this.ctx.nameNode.readFileContent(c.path) ?? '');
          });
          out.push(`✓ Copied from HDFS [${p}] -> Local [${target}] successfully.`);
          continue;
        }
        if (this.ctx.readLocal(target) !== undefined && !flags.has('-f')) {
          out.push(fail(`get: \`${target}': File exists`));
          continue;
        }
        this.ctx.writeLocal(target, this.ctx.nameNode.readFileContent(p) ?? '');
        out.push(`✓ Copied from HDFS [${p}] -> Local [${target}] successfully.`);
      }
    }
    return out.join('\n');
  }

  private cat(name: string, args: string[], fail: (m: string) => string): string {
    if (!args.length) return fail(`-${name}: Not enough arguments: expected 1 but got 0`);
    const out: string[] = [];
    for (const pattern of args) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`${name}: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const n = this.node(p)!;
        if (n.type === 'DIRECTORY') {
          out.push(fail(`${name}: \`${p}': Is a directory`));
          continue;
        }
        const content = this.ctx.nameNode.readFileContent(p) ?? '';
        out.push(content.endsWith('\n') ? content.slice(0, -1) : content);
      }
    }
    return out.join('\n');
  }

  private headTail(name: string, args: string[], fail: (m: string) => string): string {
    const pathArg = args.filter((a) => a !== '-f')[0];
    if (!pathArg) return fail(`-${name}: Not enough arguments: expected 1 but got 0`);
    const p = normalizeHdfsPath(pathArg);
    const n = this.node(p);
    if (!n) return fail(`${name}: \`${pathArg}': No such file or directory`);
    if (n.type === 'DIRECTORY') return fail(`${name}: \`${p}': Is a directory`);
    const content = this.ctx.nameNode.readFileContent(p) ?? '';
    // Hadoop shows the first / last kilobyte of the file.
    const part = name === 'head' ? content.slice(0, 1024) : content.slice(-1024);
    return part.endsWith('\n') ? part.slice(0, -1) : part;
  }

  private rm(args: string[], flags: Flags, fail: (m: string) => string): string {
    const recursive = flags.has('-r') || flags.has('-R');
    const force = flags.has('-f');
    if (!args.length) return force ? '' : fail('-rm: Not enough arguments: expected 1 but got 0');
    const out: string[] = [];
    for (const pattern of args) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        if (!force) out.push(fail(`rm: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        if (p === '/') {
          out.push(fail(`rm: Cannot delete root directory`));
          continue;
        }
        const n = this.node(p)!;
        if (n.type === 'DIRECTORY' && !recursive) {
          out.push(fail(`rm: \`${p}': Is a directory`));
          continue;
        }
        this.forgetTree(p);
        this.ctx.nameNode.deletePath(p, true);
        out.push(`✓ Deleted ${p}`);
      }
    }
    this.ctx.persistMeta();
    return out.join('\n');
  }

  private rmdir(args: string[], flags: Flags, fail: (m: string) => string): string {
    const out: string[] = [];
    for (const pattern of args) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`rmdir: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const n = this.node(p)!;
        if (n.type !== 'DIRECTORY') {
          out.push(fail(`rmdir: \`${p}': Is not a directory`));
          continue;
        }
        if (n.children.size > 0) {
          if (!flags.has('--ignore-fail-on-non-empty')) out.push(fail(`rmdir: \`${p}': Directory is not empty`));
          continue;
        }
        this.ctx.forget(p);
        this.ctx.nameNode.deletePath(p, false);
        out.push(`✓ Deleted ${p}`);
      }
    }
    return out.join('\n');
  }

  private cp(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (args.length < 2) return fail('-cp: Not enough arguments: expected 2 but got ' + args.length);
    const dst = normalizeHdfsPath(args[args.length - 1]);
    const dstNode = this.node(dst);
    const sources = args.slice(0, -1).flatMap((s) => {
      const m = this.expand(s).filter((p) => this.node(p));
      return m.length ? m : [normalizeHdfsPath(s)];
    });
    if (sources.length > 1 && (!dstNode || dstNode.type !== 'DIRECTORY')) return fail(`cp: \`${dst}': Is not a directory`);
    const out: string[] = [];
    for (const src of sources) {
      const n = this.node(src);
      if (!n) {
        out.push(fail(`cp: \`${src}': No such file or directory`));
        continue;
      }
      const target = dstNode && dstNode.type === 'DIRECTORY' ? (dst === '/' ? '' : dst) + '/' + n.name : dst;
      if (target === src || target.startsWith(src + '/')) {
        out.push(fail(`cp: \`${src}' to \`${target}': is a subdirectory of itself`));
        continue;
      }
      if (this.node(target) && !flags.has('-f')) {
        out.push(fail(`cp: \`${target}': File exists`));
        continue;
      }
      if (n.type === 'FILE') {
        this.writeFile(target, this.ctx.nameNode.readFileContent(src) ?? '', n.replicationFactor);
      } else {
        this.walk(n, (c) => {
          const rel = target + c.path.substring(src.length);
          if (c.type === 'DIRECTORY') {
            this.ns.mkdir(rel, true, this.ctx.user);
            this.ctx.persistDir(rel);
          } else this.writeFile(rel, this.ctx.nameNode.readFileContent(c.path) ?? '', c.replicationFactor);
        });
      }
      out.push(`✓ Copied in HDFS: [${src}] -> [${target}]`);
    }
    return out.join('\n');
  }

  private mv(args: string[], fail: (m: string) => string): string {
    if (args.length < 2) return fail('-mv: Not enough arguments: expected 2 but got ' + args.length);
    const dst = normalizeHdfsPath(args[args.length - 1]);
    const dstNode = this.node(dst);
    const sources = args.slice(0, -1).flatMap((s) => {
      const m = this.expand(s).filter((p) => this.node(p));
      return m.length ? m : [normalizeHdfsPath(s)];
    });
    if (sources.length > 1 && (!dstNode || dstNode.type !== 'DIRECTORY')) return fail(`mv: \`${dst}': Is not a directory`);
    const out: string[] = [];
    for (const src of sources) {
      const n = this.node(src);
      if (!n) {
        out.push(fail(`mv: \`${src}': No such file or directory`));
        continue;
      }
      const target = dstNode && dstNode.type === 'DIRECTORY' ? (dst === '/' ? '' : dst) + '/' + n.name : dst;
      if (this.node(target)) {
        out.push(fail(`mv: \`${target}': File exists`));
        continue;
      }
      if (target.startsWith(src + '/')) {
        out.push(fail(`mv: \`${src}' to \`${target}': is a subdirectory of itself`));
        continue;
      }
      this.forgetTree(src);
      this.ctx.nameNode.renamePath(src, target);
      this.persistTree(target);
      out.push(`✓ Moved in HDFS: [${src}] -> [${target}]`);
    }
    this.ctx.persistMeta();
    return out.join('\n');
  }

  private touch(name: string, args: string[], fail: (m: string) => string): string {
    if (!args.length) return fail(`-${name}: Not enough arguments: expected 1 but got 0`);
    const out: string[] = [];
    for (const raw of args) {
      const p = normalizeHdfsPath(raw);
      const n = this.node(p);
      if (n) {
        if (n.type === 'DIRECTORY') {
          out.push(fail(`${name}: \`${p}': Is a directory`));
          continue;
        }
        if (name === 'touchz' && n.sizeBytes > 0) {
          out.push(fail(`touchz: \`${p}': Not a zero-length file`));
          continue;
        }
        n.modifiedAt = Date.now();
        this.ctx.persistMeta();
        continue;
      }
      const parent = p.substring(0, p.lastIndexOf('/')) || '/';
      if (!this.node(parent)) {
        out.push(fail(`${name}: \`${p}': No such file or directory`));
        continue;
      }
      this.writeFile(p, '');
      out.push(`✓ Created empty 0-byte HDFS file: ${p}`);
    }
    return out.join('\n');
  }

  private append(args: string[], fail: (m: string) => string): string {
    if (args.length < 2) return fail('-appendToFile: Not enough arguments: expected 2 but got ' + args.length);
    const dst = normalizeHdfsPath(args[args.length - 1]);
    const n = this.node(dst);
    if (n && n.type === 'DIRECTORY') return fail(`appendToFile: \`${dst}': Is a directory`);
    let content = n ? this.ctx.nameNode.readFileContent(dst) ?? '' : '';
    for (const src of args.slice(0, -1)) {
      const data = this.ctx.readLocal(src);
      if (data === undefined) return fail(`appendToFile: \`${src}': No such file or directory`);
      content += data;
    }
    const rep = n && n.type === 'FILE' ? n.replicationFactor : undefined;
    this.writeFile(dst, content, rep);
    return `✓ Appended ${args.length - 1} local file(s) to ${dst}`;
  }

  private getmerge(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (args.length < 2) return fail('-getmerge: Not enough arguments: expected 2 but got ' + args.length);
    const localDst = args[args.length - 1];
    const parts: string[] = [];
    for (const pattern of args.slice(0, -1)) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) return fail(`getmerge: \`${pattern}': No such file or directory`);
      for (const p of matches) {
        const n = this.node(p)!;
        const files = n.type === 'FILE' ? [n] : [...n.children.values()].filter((c): c is INodeFile => c.type === 'FILE').sort((a, b) => a.name.localeCompare(b.name));
        for (const f of files) {
          let c = this.ctx.nameNode.readFileContent(f.path) ?? '';
          if (flags.has('-nl') && !c.endsWith('\n')) c += '\n';
          parts.push(c);
        }
      }
    }
    this.ctx.writeLocal(localDst, parts.join(''));
    return `✓ Merged ${parts.length} file(s) into local file ${localDst}`;
  }

  private du(args: string[], flags: Flags, fail: (m: string) => string): string {
    const human = flags.has('-h');
    const fmt = (n: number) => (human ? humanSize(n) : String(n));
    const out: string[] = [];
    const rows: [string, string, string][] = [];
    for (const pattern of args.length ? args : ['.']) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`du: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const n = this.node(p)!;
        if (flags.has('-s') || n.type === 'FILE') {
          const t = this.totals(n);
          rows.push([fmt(t.bytes), fmt(t.consumed), p]);
        } else {
          [...n.children.values()].sort((a, b) => a.name.localeCompare(b.name)).forEach((c) => {
            const t = this.totals(c);
            rows.push([fmt(t.bytes), fmt(t.consumed), c.path]);
          });
        }
      }
    }
    const w0 = Math.max(0, ...rows.map((r) => r[0].length));
    const w1 = Math.max(0, ...rows.map((r) => r[1].length));
    rows.forEach((r) => out.push(`${r[0].padEnd(w0)}  ${r[1].padEnd(w1)}  ${r[2]}`));
    return out.join('\n');
  }

  private count(args: string[], flags: Flags, fail: (m: string) => string): string {
    const human = flags.has('-h');
    const fmt = (n: number) => (human ? humanSize(n) : String(n));
    const out: string[] = [];
    if (flags.has('-v')) out.push(`${'DIR_COUNT'.padStart(12)} ${'FILE_COUNT'.padStart(12)} ${'CONTENT_SIZE'.padStart(18)} PATHNAME`);
    for (const pattern of args) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`count: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const t = this.totals(this.node(p)!);
        out.push(`${String(t.dirs).padStart(12)} ${String(t.files).padStart(12)} ${fmt(t.bytes).padStart(18)} ${p}`);
      }
    }
    if (!args.length) return fail('-count: Not enough arguments: expected 1 but got 0');
    return out.join('\n');
  }

  private df(flags: Flags): string {
    const dns = this.ctx.nameNode.getDataNodes();
    const cap = dns.reduce((a, n) => a + n.storageCapacityBytes, 0);
    const used = dns.reduce((a, n) => a + n.storageUsedBytes, 0);
    const fmt = (n: number) => (flags.has('-h') ? humanSize(n) : String(n));
    const fs = this.ctx.conf('fs.defaultFS', 'hdfs://localhost:9000');
    const pct = cap ? Math.round((used / cap) * 100) : 0;
    const cols = [fs, fmt(cap), fmt(used), fmt(cap - used), `${pct}%`];
    const head = ['Filesystem', 'Size', 'Used', 'Available', 'Use%'];
    const w = head.map((h, i) => Math.max(h.length, cols[i].length));
    return [head.map((h, i) => (i === 0 ? h.padEnd(w[i]) : h.padStart(w[i]))).join('  '), cols.map((c, i) => (i === 0 ? c.padEnd(w[i]) : c.padStart(w[i]))).join('  ')].join('\n');
  }

  private stat(args: string[], fail: (m: string) => string): string {
    if (!args.length) return fail('-stat: Not enough arguments: expected 1 but got 0');
    let format = '%y';
    let paths = args;
    if (args.length > 1 && args[0].includes('%')) {
      format = args[0];
      paths = args.slice(1);
    }
    const out: string[] = [];
    for (const pattern of paths) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`stat: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const n = this.node(p)!;
        const d = new Date(n.modifiedAt);
        const utc = `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())} ${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}:${pad2(d.getUTCSeconds())}`;
        out.push(
          format.replace(/%([bFgnorRuyYaA])/g, (_m, c) => {
            switch (c) {
              case 'b': return String(n.type === 'FILE' ? n.sizeBytes : 0);
              case 'F': return n.type === 'FILE' ? (n.sizeBytes === 0 ? 'regular file' : 'regular file') : 'directory';
              case 'g': return n.group;
              case 'n': return n.name || '/';
              case 'o': return String(n.type === 'FILE' ? n.blockSizeBytes : 0);
              case 'r': return String(n.type === 'FILE' ? n.replicationFactor : 0);
              case 'u': return n.owner;
              case 'y': return utc;
              case 'Y': return String(n.modifiedAt);
              case 'A': return n.permissions.slice(1);
              case 'a': return String(n.permissions.slice(1).match(/.../g)!.map((t) => (t[0] !== '-' ? 4 : 0) + (t[1] !== '-' ? 2 : 0) + (t[2] !== '-' && t[2] !== 'T' ? 1 : 0)).join(''));
              default: return '';
            }
          })
        );
      }
    }
    return out.join('\n');
  }

  private test(flags: Flags, args: string[], fail: (m: string) => string): string {
    const path = args[0];
    if (!path) return fail('-test: No test flag given\nUsage: hadoop fs [generic options] -test -[defswrz] <path>');
    const n = this.node(path);
    let ok = false;
    if (flags.has('-e')) ok = !!n;
    else if (flags.has('-d')) ok = !!n && n.type === 'DIRECTORY';
    else if (flags.has('-f')) ok = !!n && n.type === 'FILE';
    else if (flags.has('-s')) ok = !!n && n.type === 'FILE' && n.sizeBytes > 0;
    else if (flags.has('-z')) ok = !!n && n.type === 'FILE' && n.sizeBytes === 0;
    else if (flags.has('-r') || flags.has('-w')) ok = !!n;
    else return fail('-test: No test flag given\nUsage: hadoop fs [generic options] -test -[defswrz] <path>');
    this.ctx.setExitCode(ok ? 0 : 1);
    return '';
  }

  private setrep(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (args.length < 2) return fail('-setrep: Not enough arguments: expected 2 but got ' + args.length + '\nUsage: hadoop fs [generic options] -setrep [-R] [-w] <rep> <path> ...');
    const rep = parseInt(args[0], 10);
    if (!(rep >= 1)) return fail(`setrep: Invalid replication factor: ${args[0]}`);
    const out: string[] = [];
    const live = this.ctx.nameNode.getDataNodes().filter((d) => d.state === 'RUNNING').length;
    for (const pattern of args.slice(1)) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`setrep: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const files = this.ctx.nameNode.listFilesUnder(p);
        for (const f of files) {
          this.ctx.nameNode.setReplication(f.path, rep);
          out.push(`Replication ${rep} set for ${f.path}`);
        }
        if (flags.has('-w')) {
          out.push(`Waiting for ${p} ...${rep > live ? ` WARNING: the waiting time may be long for DECREASING the number of replications.\nOnly ${live} DataNodes are live, so blocks stay under-replicated until more DataNodes join.` : ' done'}`);
        }
      }
    }
    this.ctx.persistMeta();
    return out.join('\n');
  }

  private chmod(args: string[], flags: Flags, fail: (m: string) => string): string {
    if (args.length < 2) return fail('-chmod: Not enough arguments: expected 2 but got ' + args.length);
    const mode = args[0];
    const out: string[] = [];
    for (const pattern of args.slice(1)) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`chmod: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const apply = (n: INode) => {
          n.permissions = applyMode(n.permissions, mode, n.type === 'DIRECTORY');
        };
        if (flags.has('-R')) this.walk(this.node(p)!, apply);
        else apply(this.node(p)!);
      }
    }
    this.ctx.persistMeta();
    return out.join('\n');
  }

  private chown(args: string[], flags: Flags, fail: (m: string) => string, groupOnly: boolean): string {
    const verb = groupOnly ? 'chgrp' : 'chown';
    if (args.length < 2) return fail(`-${verb}: Not enough arguments: expected 2 but got ${args.length}`);
    const spec = args[0];
    const out: string[] = [];
    for (const pattern of args.slice(1)) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`${verb}: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const apply = (n: INode) => {
          if (groupOnly) n.group = spec;
          else {
            const [o, g] = spec.split(':');
            if (o) n.owner = o;
            if (g) n.group = g;
          }
        };
        if (flags.has('-R')) this.walk(this.node(p)!, apply);
        else apply(this.node(p)!);
      }
    }
    this.ctx.persistMeta();
    return out.join('\n');
  }

  private find(args: string[], fail: (m: string) => string): string {
    const paths: string[] = [];
    let i = 0;
    while (i < args.length && !args[i].startsWith('-')) paths.push(args[i++]);
    let nameRe: RegExp | null = null;
    while (i < args.length) {
      const opt = args[i++];
      if ((opt === '-name' || opt === '-iname') && i < args.length) {
        const re = globToRegExp(args[i++]);
        nameRe = opt === '-iname' ? new RegExp(re.source, 'i') : re;
      }
    }
    const out: string[] = [];
    for (const pattern of paths.length ? paths : ['.']) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`find: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        this.walk(this.node(p)!, (n) => {
          if (!nameRe || nameRe.test(n.name || '/')) out.push(n.path);
        });
      }
    }
    return out.join('\n');
  }

  private checksum(args: string[], fail: (m: string) => string): string {
    const out: string[] = [];
    for (const pattern of args) {
      const matches = this.expand(pattern).filter((p) => this.node(p));
      if (!matches.length) {
        out.push(fail(`checksum: \`${pattern}': No such file or directory`));
        continue;
      }
      for (const p of matches) {
        const n = this.node(p)!;
        if (n.type === 'DIRECTORY') {
          out.push(fail(`checksum: \`${p}': Is a directory`));
          continue;
        }
        out.push(`${p}\tMD5-of-0MD5-of-512CRC32C\t${md5(this.ctx.nameNode.readFileContent(p) ?? '')}`);
      }
    }
    return out.join('\n');
  }

  private truncate(args: string[], fail: (m: string) => string): string {
    const rest = args.filter((a) => a !== '-w');
    if (rest.length < 2) return fail('-truncate: Not enough arguments: expected 2 but got ' + rest.length);
    const len = parseInt(rest[0], 10);
    const p = normalizeHdfsPath(rest[1]);
    const n = this.node(p);
    if (!n || n.type !== 'FILE') return fail(`truncate: \`${rest[1]}': No such file or directory`);
    if (!(len >= 0) || len > n.sizeBytes) return fail(`truncate: Cannot truncate to a larger file size. Current size: ${n.sizeBytes}, truncate size: ${rest[0]}.`);
    const bytes = new TextEncoder().encode(this.ctx.nameNode.readFileContent(p) ?? '').slice(0, len);
    this.writeFile(p, new TextDecoder().decode(bytes), n.replicationFactor);
    return `Truncated ${p} to length: ${len}`;
  }

  private help(cmd?: string): string {
    const lines: [string, string][] = [
      ['-appendToFile <localsrc> ... <dst>', 'Append local files to an HDFS file'],
      ['-cat [-ignoreCrc] <src> ...', 'Print file contents (globs like /out/part-* work)'],
      ['-checksum <src> ...', 'Show the checksum of a file'],
      ['-chgrp [-R] GROUP PATH...', 'Change group'],
      ['-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...', 'Change permissions (755, u+x, go-w)'],
      ['-chown [-R] [OWNER][:[GROUP]] PATH...', 'Change owner/group'],
      ['-copyFromLocal [-f] <localsrc> ... <dst>', 'Same as -put'],
      ['-copyToLocal [-f] <src> ... <localdst>', 'Same as -get'],
      ['-count [-h] [-v] <path> ...', 'Count directories, files and bytes'],
      ['-cp [-f] <src> ... <dst>', 'Copy within HDFS (directories too)'],
      ['-df [-h] [<path> ...]', 'Show free space'],
      ['-du [-s] [-h] <path> ...', 'Show space used (size, size with replicas)'],
      ['-expunge', 'Empty the trash'],
      ['-find <path> ... -name|-iname <pattern>', 'Find files'],
      ['-get [-f] <src> ... <localdst>', 'Copy HDFS -> local'],
      ['-getmerge [-nl] <src> <localdst>', 'Merge HDFS files into one local file'],
      ['-head <file>', 'Show the first 1KB of a file'],
      ['-ls [-C] [-d] [-h] [-R] [-t] [-S] [-r] [<path> ...]', 'List files'],
      ['-mkdir [-p] <path> ...', 'Create directories'],
      ['-moveFromLocal <localsrc> ... <dst>', 'Like -put, then delete the local file'],
      ['-mv <src> ... <dst>', 'Move/rename within HDFS'],
      ['-put [-f] <localsrc> ... <dst>', 'Copy local -> HDFS (a directory target keeps the file name)'],
      ['-rm [-f] [-r|-R] [-skipTrash] <src> ...', 'Delete files (-r for directories)'],
      ['-rmdir [--ignore-fail-on-non-empty] <dir> ...', 'Delete empty directories'],
      ['-setrep [-R] [-w] <rep> <path> ...', 'Change replication factor'],
      ['-stat [format] <path> ...', 'Print file stats (%b %F %g %n %o %r %u %y %Y)'],
      ['-tail [-f] <file>', 'Show the last 1KB of a file'],
      ['-test -[defsz] <path>', 'Test a path (check with: echo $?)'],
      ['-text <src> ...', 'Print a file as text'],
      ['-touchz <path> ...', 'Create an empty file'],
      ['-truncate [-w] <length> <path>', 'Truncate a file'],
      ['-getfacl / -setfacl', 'Show / set ACLs'],
      ['-allowSnapshot / -createSnapshot / -deleteSnapshot', 'Snapshots']
    ];
    const pick = cmd ? lines.filter(([u]) => u.startsWith('-' + cmd.replace(/^-/, '') + ' ') || u === '-' + cmd.replace(/^-/, '')) : lines;
    const width = Math.max(...pick.map(([u]) => u.length));
    return ['Usage: hadoop fs [generic options]', ...pick.map(([u, d]) => `\t[${u}]${' '.repeat(Math.max(1, width - u.length + 2))}${d}`)].join('\n');
  }
}

export { DIR_MARK };
export type { INodeDirectory };
