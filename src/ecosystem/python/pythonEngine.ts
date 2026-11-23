import { PYSPARK_PYTHON_SETUP } from '../spark/pysparkEngine';
import { PYSPARK_EXTENSIONS } from '../spark/pysparkExtensions';

export interface PythonExecutionResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export function transpilePythonToJS(pythonCode: string): string {
  let cleanCode = pythonCode
    .replace(/[\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]/g, ' ')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');

  if (!cleanCode.includes('\n') && cleanCode.includes('\\n')) {
    cleanCode = cleanCode.replace(/\\n/g, '\n').replace(/\\t/g, '    ');
  }

  const lines = cleanCode.split('\n');
  const jsLines: string[] = [];
  const indentStack: number[] = [0];

  for (let i = 0; i < lines.length; i++) {
    const rawLine = lines[i];
    const trimmed = rawLine.trim();

    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    let currentIndent = rawLine.search(/\S/);
    if (currentIndent < 0) currentIndent = 0;

    while (indentStack.length > 1 && currentIndent <= indentStack[indentStack.length - 1]) {
      indentStack.pop();
      if (!trimmed.startsWith('else') && !trimmed.startsWith('elif')) {
        jsLines.push('}');
      }
    }

    let line = trimmed;

    // Unescape Python boolean & keyword literals
    line = line.replace(/\bTrue\b/g, 'true');
    line = line.replace(/\bFalse\b/g, 'false');
    line = line.replace(/\bNone\b/g, 'null');
    line = line.replace(/\bis not\b/g, '!==');
    line = line.replace(/\bis\b/g, '===');
    line = line.replace(/\band\b/g, '&&');
    line = line.replace(/\bor\b/g, '||');
    line = line.replace(/\bnot\b\s+/g, '!');
    line = line.replace(/\bpass\b/g, '');

    // Handle context managers: with open(...) as f:
    if (/^with\s+open\((.*?)\)\s+as\s+([a-zA-Z_]\w*)\s*:\s*(#.*)?$/.test(line)) {
      line = line.replace(/^with\s+open\((.*?)\)\s+as\s+([a-zA-Z_]\w*)\s*:\s*(#.*)?$/, 'let $2 = __py_open__($1); {');
      indentStack.push(currentIndent);
      jsLines.push(line);
      continue;
    }

    // Handle imports: import helper
    if (/^import\s+([a-zA-Z_]\w*)\s*(#.*)?$/.test(line)) {
      line = line.replace(/^import\s+([a-zA-Z_]\w*)\s*(#.*)?$/, 'let $1 = __py_import__("$1");');
      jsLines.push(line);
      continue;
    }

    // Handle from module import func
    if (/^from\s+([a-zA-Z_]\w*)\s+import\s+(.*?)\s*(#.*)?$/.test(line)) {
      line = line.replace(/^from\s+([a-zA-Z_]\w*)\s+import\s+(.*?)\s*(#.*)?$/, 'let { $2 } = __py_import__("$1");');
      jsLines.push(line);
      continue;
    }

    const oneLinerMatch = line.match(/^for\s+([a-zA-Z_]\w*)\s+in\s+([^:]+):\s*(.+)$/);
    if (oneLinerMatch) {
      const [, varName, iterExpr, body] = oneLinerMatch;
      let jsBody = body.replace(/print\((.*?)\)/g, (_m, args) => `__py_print__(${args})`);
      jsBody = jsBody.replace(/input\((.*?)\)/g, (_m, arg) => `__py_input__(${arg || '""'})`);
      if (!jsBody.endsWith(';')) jsBody += ';';
      jsLines.push(`for (let ${varName} of ${iterExpr}) { ${jsBody} }`);
      continue;
    }

    if (/^for\s+([a-zA-Z_]\w*)\s+in\s+(.*?)\s*:\s*(#.*)?$/.test(line)) {
      line = line.replace(/^for\s+([a-zA-Z_]\w*)\s+in\s+(.*?)\s*:\s*(#.*)?$/, 'for (let $1 of $2) {');
      indentStack.push(currentIndent);
    } else if (/^while\s+(.*?)\s*:\s*(#.*)?$/.test(line)) {
      const cond = line.replace(/^while\s+(.*?)\s*:\s*(#.*)?$/, '$1');
      line = `let __py_while_count_${i}__ = 0;\nwhile (${cond}) { if (++__py_while_count_${i}__ > 10000) break;`;
      indentStack.push(currentIndent);
    } else if (/^def\s+([a-zA-Z_]\w*)\s*\((.*?)\)\s*:\s*(#.*)?$/.test(line)) {
      line = line.replace(/^def\s+([a-zA-Z_]\w*)\s*\((.*?)\)\s*:\s*(#.*)?$/, 'function $1($2) {');
      indentStack.push(currentIndent);
    } else if (/^elif\s+(.*?)\s*:\s*(#.*)?$/.test(line)) {
      line = line.replace(/^elif\s+(.*?)\s*:\s*(#.*)?$/, '} else if ($1) {');
      indentStack.push(currentIndent);
    } else if (/^if\s+(.*?)\s*:\s*(#.*)?$/.test(line)) {
      line = line.replace(/^if\s+(.*?)\s*:\s*(#.*)?$/, 'if ($1) {');
      indentStack.push(currentIndent);
    } else if (/^else\s*:\s*(#.*)?$/.test(line)) {
      line = '} else {';
      indentStack.push(currentIndent);
    } else {
      line = line.replace(/print\((.*?)\)/g, (_m, args) => `__py_print__(${args})`);
      line = line.replace(/input\((.*?)\)/g, (_m, arg) => `__py_input__(${arg || '""'})`);
      line = line.replace(/open\((.*?)\)/g, (_m, args) => `__py_open__(${args})`);

      if (!line.endsWith('{') && !line.endsWith('}') && !line.endsWith(';')) {
        line += ';';
      }
    }

    jsLines.push(line);
  }

  while (indentStack.length > 1) {
    indentStack.pop();
    jsLines.push('}');
  }

  return jsLines.join('\n');
}

export class PythonEngine {
  private pyodide: any = null;
  private pyodideInitPromise: Promise<void> | null = null;
  private isPySparkSetupDone: boolean = false;
  private isPySparkModulesLoaded: boolean = false;
  private isRunnerReady: boolean = false;
  /** Files currently mirrored into Pyodide's filesystem and the content written there. */
  private mirrored: Map<string, string> = new Map();

  /** Python helpers shared by scripts, the Python REPL and the PySpark REPL. */
  private static RUNNER = `
import sys, io, ast, builtins, importlib, traceback, os

__hadoop_real_input = builtins.input

def __hadoop_purge_user_modules():
    # A student edits word_count.py and runs again: Python must import the new code.
    for __n, __m in list(sys.modules.items()):
        __f = getattr(__m, '__file__', None) or ''
        if __f.startswith('/home/Hacker/') or __f.startswith('/tmp/'):
            sys.modules.pop(__n, None)
    importlib.invalidate_caches()

def __hadoop_format_exc(filename):
    etype, value, tb = sys.exc_info()
    te = traceback.TracebackException(etype, value, tb)
    keep = [f for f in te.stack if not f.filename.startswith('/lib/') and f.filename != '<exec>' and f.name not in ('__hadoop_run',)]
    te.stack = traceback.StackSummary.from_list(keep)
    text = ''.join(te.format())
    if not keep:
        text = text.replace('Traceback (most recent call last):\\n', '')
    return text.rstrip('\\n')

def __hadoop_run(src, filename, argv, stdin_text, repl):
    __hadoop_purge_user_modules()
    sys.argv = list(argv) if argv else ['']
    old_stdin = sys.stdin
    old_input = builtins.input
    if stdin_text is None:
        def __js_input(prompt=''):
            import js
            value = js.__pyodide_input_handler(str(prompt))
            return '' if value is None else str(value)
        builtins.input = __js_input
    else:
        sys.stdin = io.StringIO(stdin_text)
        def __stdin_input(prompt=''):
            line = sys.stdin.readline()
            if not line:
                raise EOFError('EOF when reading a line')
            return line.rstrip('\\n')
        builtins.input = __stdin_input
    try:
        if repl:
            ns = globals()
            tree = ast.parse(src, '<stdin>', 'exec')
            if tree.body and isinstance(tree.body[-1], ast.Expr):
                last = ast.Expression(tree.body.pop().value)
                if tree.body:
                    exec(compile(tree, '<stdin>', 'exec'), ns)
                value = eval(compile(last, '<stdin>', 'eval'), ns)
                if value is not None:
                    ns['_'] = value
                    print(repr(value))
            else:
                exec(compile(tree, '<stdin>', 'exec'), ns)
        else:
            ns = {'__name__': '__main__', '__file__': filename, '__builtins__': builtins}
            exec(compile(src, filename, 'exec'), ns)
        return 0
    except SystemExit as e:
        code = e.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        print(code, file=sys.stderr)
        return 1
    except BaseException:
        print(__hadoop_format_exc(filename), file=sys.stderr)
        return 1
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        sys.stdin = old_stdin
        builtins.input = old_input
`;

  private ensureRunner(): void {
    if (this.isRunnerReady || !this.pyodide) return;
    this.pyodide.runPython(PythonEngine.RUNNER);
    this.isRunnerReady = true;
  }

  /** Version string of the bundled Python, e.g. "Python 3.14.2". */
  public getPythonVersion(): string | null {
    if (!this.pyodide) return null;
    try {
      return 'Python ' + String(this.pyodide.runPython('import sys; sys.version.split()[0]'));
    } catch {
      return null;
    }
  }

  public isReady(): boolean {
    return !!this.pyodide;
  }

  /**
   * Mirror the shell's files into Pyodide. Only changed files are written, and files
   * deleted in the shell (rm) are removed from Pyodide too, so they cannot come back.
   */
  private syncLocalFiles(localFiles?: Map<string, string>): void {
    if (!localFiles || !this.pyodide?.FS) return;
    const fs = this.pyodide.FS;

    for (const path of Array.from(this.mirrored.keys())) {
      const normalized = path;
      const stillThere = localFiles.has(normalized) || localFiles.has(normalized.replace(/^\/home\/Hacker\//, ''));
      if (!stillThere) {
        try {
          fs.unlink(normalized);
        } catch {
          // Already gone.
        }
        this.mirrored.delete(path);
      }
    }

    const importDirs = new Set<string>(['/home/Hacker']);
    const pythonDirs = new Set<string>();
    localFiles.forEach((content, filepath) => {
      const normalized = filepath.startsWith('/') ? filepath : `/home/Hacker/${filepath}`;
      const parent = normalized.slice(0, normalized.lastIndexOf('/')) || '/';
      importDirs.add(parent);
      if (normalized.endsWith('.py')) pythonDirs.add(parent);
      if (this.mirrored.get(normalized) === content) return;
      try {
        fs.mkdirTree(parent);
        fs.writeFile(normalized, content);
        this.mirrored.set(normalized, content);
      } catch {
        // A duplicate or unsupported path should not stop Python execution.
      }
    });

    pythonDirs.forEach((dir) => {
      if (dir !== '/home/Hacker') {
        try {
          if (!fs.analyzePath(`${dir}/__init__.py`).exists) fs.writeFile(`${dir}/__init__.py`, '');
        } catch {
          // The marker is only needed when the directory is writable.
        }
      }
    });

    try {
      this.pyodide.runPython(`
import sys, os, importlib
__project_paths = ${JSON.stringify(Array.from(importDirs))}
os.chdir('/home/Hacker')
sys.path[:] = __project_paths + [p for p in sys.path if p not in __project_paths]
importlib.invalidate_caches()
`);
    } catch {
      // The filesystem sync still helps open(); imports can report their own errors.
    }
  }

  /** Save files that Python created or changed back into the shell (only real changes). */
  private persistLocalFiles(onSaveFile?: (path: string, content: string) => void): void {
    if (!onSaveFile || !this.pyodide?.FS) return;
    const fs = this.pyodide.FS;
    const visit = (directory: string) => {
      let entries: string[];
      try {
        entries = fs.readdir(directory).filter((name: string) => name !== '.' && name !== '..');
      } catch {
        return;
      }
      for (const name of entries) {
        const path = `${directory.replace(/\/$/, '')}/${name}`;
        try {
          const stat = fs.stat(path);
          if (fs.isDir(stat.mode)) {
            if (name !== '__pycache__') visit(path);
          } else if (fs.isFile(stat.mode) && !name.endsWith('.pyc')) {
            if (name === '__init__.py' && this.mirrored.get(path) === undefined) continue;
            const content = fs.readFile(path, { encoding: 'utf8' });
            if (this.mirrored.get(path) !== content) {
              this.mirrored.set(path, content);
              onSaveFile(path, content);
            }
          }
        } catch {
          // Ignore transient or unsupported entries and continue persisting others.
        }
      }
    };
    visit('/home/Hacker');
  }

  /** Run code through the shared runner and capture stdout/stderr. */
  private execCaptured(
    code: string,
    filename: string,
    argv: string[],
    stdinText: string | null,
    repl: boolean,
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void,
    inputs?: string[]
  ): PythonExecutionResult {
    // Capture raw bytes so output without a trailing newline (print(x, end='')) is kept exactly.
    const outDec = new TextDecoder();
    const errDec = new TextDecoder();
    let outText = '';
    let errText = '';
    const out = { push: (s: string) => { outText += s; } };
    let inputIndex = 0;
    try {
      this.pyodide.setStdout({ write: (buf: Uint8Array) => { outText += outDec.decode(buf, { stream: true }); return buf.length; } });
      this.pyodide.setStderr({ write: (buf: Uint8Array) => { errText += errDec.decode(buf, { stream: true }); return buf.length; } });
      this.syncLocalFiles(localFiles);
      this.ensureRunner();
      if (typeof window !== 'undefined') {
        (window as any).__pyodide_input_handler = (promptMsg: string = '') => {
          if (promptMsg) out.push(String(promptMsg));
          if (inputs && inputIndex < inputs.length) return inputs[inputIndex++];
          return '';
        };
      }
      const payload = JSON.stringify(JSON.stringify([code, filename, argv, stdinText, repl]));
      const exitCode = Number(this.pyodide.runPython(`__hadoop_run(*__import__('json').loads(${payload}))`)) || 0;
      this.persistLocalFiles(onSaveFile);
      outText += outDec.decode();
      errText += errDec.decode();
      return { stdout: outText.replace(/\n$/, ''), stderr: errText.replace(/\n$/, ''), exitCode };
    } catch (e: any) {
      return { stdout: outText.replace(/\n$/, ''), stderr: e?.message || String(e), exitCode: 1 };
    }
  }

  /** Run a script with text on stdin (Hadoop Streaming mapper/reducer, shell pipes). */
  public runWithStdin(code: string, stdinText: string, argv: string[], localFiles?: Map<string, string>): PythonExecutionResult | null {
    if (!this.pyodide) return null;
    this.pyodide.runPython(PYSPARK_PYTHON_SETUP);
    return this.execCaptured(code, argv[0] || '<stdin>', argv, stdinText, false, localFiles);
  }

  constructor() {
    this.initPyodide();
  }

  public resetPySparkRepl(): void {
    this.isPySparkSetupDone = false;
  }

  private ensurePySparkModules(): void {
    if (this.isPySparkModulesLoaded || !this.pyodide) return;
    this.pyodide.runPython(PYSPARK_PYTHON_SETUP);
    this.pyodide.runPython(PYSPARK_EXTENSIONS);
    this.isPySparkModulesLoaded = true;
  }

  public async runPythonReplLine(
    line: string,
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void
  ): Promise<PythonExecutionResult> {
    await this.initPyodide();
    if (!this.pyodide) {
      return { stdout: '', stderr: 'Python REPL could not start because the Pyodide runtime is unavailable.', exitCode: 1 };
    }
    this.ensurePySparkModules();
    return this.execCaptured(line, '<stdin>', [''], null, true, localFiles, onSaveFile);
  }

  public async runPySparkReplLine(
    line: string,
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void
  ): Promise<PythonExecutionResult> {
    await this.initPyodide();
    if (!this.pyodide) {
      return {
        stdout: '',
        stderr: 'PySpark REPL could not start because the Pyodide Python runtime is unavailable.',
        exitCode: 1
      };
    }
    this.ensurePySparkModules();
    if (!this.isPySparkSetupDone) {
      try {
        this.pyodide.runPython(`
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("PySparkShell").getOrCreate()
sc = spark.sparkContext
`);
        this.isPySparkSetupDone = true;
      } catch (err: any) {
        return { stdout: '', stderr: err?.message || String(err), exitCode: 1 };
      }
    }
    return this.execCaptured(line, '<stdin>', [''], null, true, localFiles, onSaveFile);
  }

  public async initPyodide(): Promise<void> {
    if (typeof window === 'undefined') return;
    if ((window as any).pyodide) {
      this.pyodide = (window as any).pyodide;
      return;
    }

    if (this.pyodideInitPromise) {
      await this.pyodideInitPromise;
      return;
    }
    this.pyodideInitPromise = (async () => {
      try {
        if (!(window as any).loadPyodide) {
          const script = document.createElement('script');
          script.src = './pyodide/pyodide.js';
          script.async = true;
          document.head.appendChild(script);
          await new Promise((resolve) => {
            script.onload = resolve;
            script.onerror = resolve;
          });
        }

        if ((window as any).loadPyodide) {
          this.pyodide = await (window as any).loadPyodide({
            indexURL: './pyodide/',
            // Keep package downloads local too (pyodide's default is a CDN).
            packageBaseUrl: './pyodide/'
          });
          (window as any).pyodide = this.pyodide;
        }
      } catch {
        this.pyodide = null;
      }
    })();
    await this.pyodideInitPromise;
    this.pyodideInitPromise = null;
  }

  public runSync(
    code: string,
    inputs: string[] = [],
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void,
    scriptName: string = '<string>',
    argv?: string[]
  ): PythonExecutionResult | null {
    if (!this.pyodide) return null;
    try {
      this.ensurePySparkModules();
    } catch (err: any) {
      return { stdout: '', stderr: err?.message || String(err), exitCode: 1 };
    }
    return this.execCaptured(code, scriptName, argv || [scriptName, ...inputs], null, false, localFiles, onSaveFile, inputs);
  }

  public fallbackExecuteSync(
    code: string,
    inputs: string[] = [],
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void
  ): PythonExecutionResult {
    let inputIdx = 0;
    const inputFn = (_promptMsg: string): string => {
      if (inputIdx < inputs.length) return inputs[inputIdx++];
      return '';
    };
    return this.fallbackExecute(code, inputFn, localFiles, onSaveFile);
  }

  public fallbackExecute(
    code: string,
    inputFn: (prompt: string) => string,
    localFiles?: Map<string, string>,
    onSaveFile?: (path: string, content: string) => void
  ): PythonExecutionResult {
    const stdout: string[] = [];
    const stderr: string[] = [];

    try {
      const jsCode = transpilePythonToJS(code);

      const pyPrint = (...args: any[]) => {
        const line = args.map((a) => (typeof a === 'object' ? JSON.stringify(a) : String(a))).join(' ');
        stdout.push(line);
      };

      const pyInput = (promptStr: string = ''): string => {
        return inputFn(promptStr);
      };

      const pyOpen = (path: string, mode: string = 'r') => {
        const resolvedPath = path.startsWith('/') ? path : `/home/Hacker/${path}`;
        const basename = path.split('/').pop() || path;

        if (mode.includes('w') || mode.includes('a')) {
          let fileData = mode.includes('a') ? (localFiles?.get(resolvedPath) || localFiles?.get(basename) || '') : '';
          return {
            write: (str: string) => {
              fileData += String(str);
              if (onSaveFile) onSaveFile(basename, fileData);
            },
            writelines: (lines: string[]) => {
              fileData += lines.join('');
              if (onSaveFile) onSaveFile(basename, fileData);
            },
            read: () => fileData,
            close: () => {},
            __enter__: function() { return this; },
            __exit__: function() {}
          };
        } else {
          const content = localFiles?.get(resolvedPath) ?? localFiles?.get(basename) ?? '';
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
        const content = localFiles?.get(`/home/Hacker/${modFile}`) || localFiles?.get(modFile);
        if (!content) {
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

      return {
        stdout: stdout.join('\n'),
        stderr: stderr.join('\n'),
        exitCode: 0
      };
    } catch (err: any) {
      return {
        stdout: stdout.join('\n'),
        stderr: `Python SyntaxError / RuntimeError: ${err.message}`,
        exitCode: 1
      };
    }
  }
}
