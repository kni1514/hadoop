/**
 * The programs inside hadoop-mapreduce-examples-3.3.6.jar, computed for real on the
 * input text (not canned output). Results match Apache Hadoop for the same input.
 */

export interface JobResult {
  /** Content of part-r-00000 (empty string for jobs without file output). */
  output: string;
  /** Extra text Hadoop prints after the job (for example the Pi estimate). */
  summary?: string;
  counters: {
    mapInputRecords: number;
    mapOutputRecords: number;
    reduceInputGroups: number;
    reduceOutputRecords: number;
    bytesRead: number;
    bytesWritten: number;
  };
}

/** Java's StringTokenizer default delimiters, used by WordCount. */
const TOKEN_SPLIT = /[ \t\n\r\f]+/;

/** Java String.compareTo order (UTF-16 code units), which Hadoop's Text keys follow for ASCII. */
const javaCompare = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

function countLines(texts: string[]): number {
  return texts.reduce((n, t) => n + (t === '' ? 0 : t.split('\n').length - (t.endsWith('\n') ? 1 : 0)), 0);
}

function bytes(texts: string[]): number {
  return texts.reduce((n, t) => n + new TextEncoder().encode(t).length, 0);
}

export function wordCount(texts: string[]): JobResult {
  const counts = new Map<string, number>();
  let emitted = 0;
  for (const text of texts) {
    for (const token of text.split(TOKEN_SPLIT)) {
      if (!token) continue;
      counts.set(token, (counts.get(token) || 0) + 1);
      emitted++;
    }
  }
  const keys = [...counts.keys()].sort(javaCompare);
  const output = keys.map((k) => `${k}\t${counts.get(k)}`).join('\n') + (keys.length ? '\n' : '');
  return {
    output,
    counters: {
      mapInputRecords: countLines(texts),
      mapOutputRecords: emitted,
      reduceInputGroups: keys.length,
      reduceOutputRecords: keys.length,
      bytesRead: bytes(texts),
      bytesWritten: new TextEncoder().encode(output).length
    }
  };
}

/** grep <in> <out> <regex> [<group>]: count matches, sorted by count (desc) then text. */
export function grep(texts: string[], regex: string, group = 0): JobResult {
  let re: RegExp;
  try {
    re = new RegExp(regex, 'g');
  } catch (err: any) {
    throw new Error(`java.util.regex.PatternSyntaxException: ${err.message}`);
  }
  const counts = new Map<string, number>();
  let emitted = 0;
  for (const text of texts) {
    for (const line of text.split('\n')) {
      re.lastIndex = 0;
      let m: RegExpExecArray | null;
      while ((m = re.exec(line)) !== null) {
        const key = m[group] ?? '';
        counts.set(key, (counts.get(key) || 0) + 1);
        emitted++;
        if (m[0] === '') re.lastIndex++;
      }
    }
  }
  const keys = [...counts.keys()].sort((a, b) => counts.get(b)! - counts.get(a)! || javaCompare(a, b));
  const output = keys.map((k) => `${counts.get(k)}\t${k}`).join('\n') + (keys.length ? '\n' : '');
  return {
    output,
    counters: {
      mapInputRecords: countLines(texts),
      mapOutputRecords: emitted,
      reduceInputGroups: keys.length,
      reduceOutputRecords: keys.length,
      bytesRead: bytes(texts),
      bytesWritten: new TextEncoder().encode(output).length
    }
  };
}

/** Halton sequence exactly as in Hadoop's QuasiMonteCarlo, so `pi 2 10` prints the same value. */
class HaltonSequence {
  private static P = [2, 3];
  private static K = [63, 40];
  private x: number[] = [];
  private q: number[][] = [];
  private d: number[][] = [];

  constructor(startIndex: number) {
    const { P, K } = HaltonSequence;
    for (let i = 0; i < K.length; i++) {
      this.q[i] = new Array(K[i]).fill(0);
      this.d[i] = new Array(K[i]).fill(0);
    }
    for (let i = 0; i < K.length; i++) {
      let k = startIndex;
      this.x[i] = 0;
      for (let j = 0; j < K[i]; j++) {
        this.q[i][j] = (j === 0 ? 1.0 : this.q[i][j - 1]) / P[i];
        this.d[i][j] = k % P[i];
        k = (k - this.d[i][j]) / P[i];
        this.x[i] += this.d[i][j] * this.q[i][j];
      }
    }
  }

  nextPoint(): number[] {
    const { P, K } = HaltonSequence;
    for (let i = 0; i < K.length; i++) {
      for (let j = 0; j < K[i]; j++) {
        this.d[i][j]++;
        this.x[i] += this.q[i][j];
        if (this.d[i][j] < P[i]) break;
        this.d[i][j] = 0;
        this.x[i] -= j === 0 ? 1.0 : this.q[i][j - 1];
      }
    }
    return this.x;
  }
}

export function piEstimate(numMaps: number, numSamples: number): JobResult {
  let inside = 0;
  let outside = 0;
  for (let m = 0; m < numMaps; m++) {
    const seq = new HaltonSequence(m * numSamples);
    for (let s = 0; s < numSamples; s++) {
      const p = seq.nextPoint();
      const x = p[0] - 0.5;
      const y = p[1] - 0.5;
      if (x * x + y * y > 0.25) outside++;
      else inside++;
    }
  }
  const total = BigInt(numMaps) * BigInt(numSamples);
  // BigDecimal(4).setScale(20) * inside / total, HALF_UP, printed with 20 decimals.
  const scale = 10n ** 20n;
  const num = 4n * BigInt(inside) * scale;
  let q = num / total;
  if ((num % total) * 2n >= total) q += 1n;
  const intPart = q / scale;
  const frac = (q % scale).toString().padStart(20, '0');
  return {
    output: '',
    summary: `Estimated value of Pi is ${intPart}.${frac}`,
    counters: {
      mapInputRecords: numMaps,
      mapOutputRecords: numMaps * 2,
      reduceInputGroups: 2,
      reduceOutputRecords: 0,
      bytesRead: 0,
      bytesWritten: 0
    }
  };
}

export function wordMean(texts: string[]): JobResult {
  let count = 0;
  let length = 0;
  for (const text of texts) {
    for (const token of text.split(TOKEN_SPLIT)) {
      if (!token) continue;
      count++;
      length += token.length;
    }
  }
  const output = `count\t${count}\nlength\t${length}\n`;
  return {
    output,
    summary: `The mean is: ${count ? length / count : 0}`,
    counters: {
      mapInputRecords: countLines(texts),
      mapOutputRecords: count * 2,
      reduceInputGroups: 2,
      reduceOutputRecords: 2,
      bytesRead: bytes(texts),
      bytesWritten: output.length
    }
  };
}

/** `sort` example on text input: lines sorted as Text keys. */
export function sortLines(texts: string[]): JobResult {
  const lines = texts.flatMap((t) => t.split('\n')).filter((l) => l !== '');
  lines.sort(javaCompare);
  const output = lines.join('\n') + (lines.length ? '\n' : '');
  return {
    output,
    counters: {
      mapInputRecords: lines.length,
      mapOutputRecords: lines.length,
      reduceInputGroups: new Set(lines).size,
      reduceOutputRecords: lines.length,
      bytesRead: bytes(texts),
      bytesWritten: output.length
    }
  };
}

/** Shuffle & sort step of Hadoop Streaming: sort mapper output lines by key (text before the first tab). */
export function streamingShuffle(mapOutput: string): string {
  const lines = mapOutput.split('\n').filter((l) => l !== '');
  const keyOf = (l: string) => {
    const i = l.indexOf('\t');
    return i === -1 ? l : l.slice(0, i);
  };
  lines.sort((a, b) => javaCompare(keyOf(a), keyOf(b)) || javaCompare(a, b));
  return lines.join('\n') + (lines.length ? '\n' : '');
}
