export interface ParsedCommand {
  raw: string;
  utility: 'hdfs' | 'hadoop' | 'yarn' | 'mapred' | 'script' | 'linux' | 'help' | 'clear' | 'comment' | 'unknown';
  subcommand?: string;
  action?: string;
  flags: Set<string>;
  positionalArgs: string[];
}

/**
 * Split a command line like a shell does: whitespace separates words, but text inside
 * single or double quotes stays one word (quotes removed), and an unquoted word starting
 * with # begins a comment.
 */
export function shellSplit(line: string): string[] {
  const out: string[] = [];
  let cur = '';
  let inWord = false;
  let quote: string | null = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (quote) {
      if (ch === quote) quote = null;
      else cur += ch;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      inWord = true;
      continue;
    }
    if (/\s/.test(ch)) {
      if (inWord) out.push(cur);
      cur = '';
      inWord = false;
      continue;
    }
    if (ch === '#' && !inWord) break;
    cur += ch;
    inWord = true;
  }
  if (inWord) out.push(cur);
  return out;
}

const HADOOP_UTILITIES = new Set(['hdfs', 'hadoop', 'yarn', 'mapred']);

export function parseHadoopCommand(input: string): ParsedCommand {
  const firstWord = input.trim().split(/\s+/)[0]?.toLowerCase() || '';
  if (HADOOP_UTILITIES.has(firstWord)) {
    return parseHadoopUtility(input);
  }
  return parseOtherCommand(input);
}

function parseHadoopUtility(input: string): ParsedCommand {
  const tokens = shellSplit(input.trim());
  if (tokens.length === 0) {
    return { raw: input, utility: 'comment', flags: new Set(), positionalArgs: [] };
  }
  const utility = tokens[0].toLowerCase() as 'hdfs' | 'hadoop' | 'yarn' | 'mapred';
  let subcommand: string | undefined;
  let action: string | undefined;
  const flags = new Set<string>();
  const positionalArgs: string[] = [];

  let idx = 1;
  // 'hdfs --daemon start namenode' / 'hadoop --config dir fs ...'
  if (idx < tokens.length && !tokens[idx].startsWith('-')) {
    subcommand = tokens[idx];
    idx++;
  } else if (idx < tokens.length && (tokens[idx] === '--daemon' || tokens[idx] === '-version' || tokens[idx] === '--version')) {
    subcommand = tokens[idx].replace(/^-+/, '');
    idx++;
  }

  // 'hadoop fs' and 'hadoop dfs' are the same shell as 'hdfs dfs'.
  if (subcommand === 'fs' || (utility === 'hadoop' && subcommand === 'dfs')) subcommand = 'dfs';

  const ACTION_SUBCOMMANDS = new Set(['namenode', 'dfs', 'dfsadmin', 'rmadmin', 'application', 'logs', 'haadmin', 'storagepolicies', 'ec', 'cacheadmin', 'balancer', 'diskbalancer', 'queue', 'crypto', 'node', 'getconf', 'job', 'container', 'applicationattempt']);

  while (idx < tokens.length) {
    const token = tokens[idx];
    if (token.startsWith('-') && token.length > 1 && !/^-\d/.test(token)) {
      if (!action && subcommand && ACTION_SUBCOMMANDS.has(subcommand)) {
        action = token;
      } else {
        flags.add(token);
      }
    } else {
      positionalArgs.push(token);
    }
    idx++;
  }

  return { raw: input, utility, subcommand, action, flags, positionalArgs };
}

function parseOtherCommand(input: string): ParsedCommand {
  let cleanInput = input;
  const hashIdx = cleanInput.indexOf('#');
  if (hashIdx !== -1) {
    cleanInput = cleanInput.substring(0, hashIdx);
  }

  const trimmed = cleanInput.trim();
  if (!trimmed || input.trim().startsWith('#')) {
    return { raw: input, utility: 'comment', flags: new Set(), positionalArgs: [] };
  }

  const tokens = trimmed.split(/\s+/).filter(Boolean);
  const utilityStr = tokens[0].toLowerCase();

  if (utilityStr === 'clear') {
    return { raw: input, utility: 'clear', flags: new Set(), positionalArgs: [] };
  }

  if (utilityStr === 'help') {
    return { raw: input, utility: 'help', flags: new Set(), positionalArgs: tokens.slice(1) };
  }

  
  if (['start-dfs.sh', 'start-dfs', 'stop-dfs.sh', 'stop-dfs', 'start-yarn.sh', 'start-yarn', 'stop-yarn.sh', 'stop-yarn', 'start-all.sh', 'start-all', 'stop-all.sh', 'stop-all'].includes(utilityStr)) {
    return { raw: input, utility: 'script', action: utilityStr, flags: new Set(), positionalArgs: tokens.slice(1) };
  }

  
  if (['ls', 'pwd', 'cd', 'mkdir', 'rmdir', 'echo', 'cat', 'rm', 'touch', 'whoami', 'grep', 'wc', 'head', 'tail', 'history', 'top', 'df', 'free', 'uname', 'ifconfig', 'ip', 'ping', 'netstat', 'ps', 'jps', 'kill', 'killall', 'sort', 'uniq', 'node', 'python', 'python3', 'cp', 'mv', 'hive', 'pig', 'beeline', 'download', 'get', 'spark', 'pyspark', 'spark-shell', 'spark-submit', 'about'].includes(utilityStr)) {
    return { raw: input, utility: 'linux', action: utilityStr, flags: new Set(), positionalArgs: tokens.slice(1) };
  }

  if (utilityStr !== 'hdfs' && utilityStr !== 'hadoop' && utilityStr !== 'yarn') {
    return { raw: input, utility: 'unknown', flags: new Set(), positionalArgs: tokens };
  }

  const utility = utilityStr as 'hdfs' | 'hadoop' | 'yarn';
  let subcommand: string | undefined;
  let action: string | undefined;
  const flags = new Set<string>();
  const positionalArgs: string[] = [];

  let idx = 1;
  if (idx < tokens.length && !tokens[idx].startsWith('-')) {
    subcommand = tokens[idx];
    idx++;
  }

  while (idx < tokens.length) {
    const token = tokens[idx];
    if (token.startsWith('-')) {
      if (!action && (subcommand === 'namenode' || subcommand === 'dfs' || subcommand === 'dfsadmin' || subcommand === 'rmadmin' || subcommand === 'application' || subcommand === 'logs' || subcommand === 'haadmin' || subcommand === 'storagepolicies' || subcommand === 'ec' || subcommand === 'cacheadmin' || subcommand === 'balancer' || subcommand === 'diskbalancer' || subcommand === 'queue' || subcommand === 'crypto' || subcommand === 'node')) {
        action = token;
      } else {
        flags.add(token);
      }
    } else {
      positionalArgs.push(token);
    }
    idx++;
  }

  return { raw: input, utility, subcommand, action, flags, positionalArgs };
}
