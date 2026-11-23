/**
 * Spark 3.5-compatible additions on top of the in-memory PySpark runtime: exact MLlib
 * estimators, Spark-style show()/printSchema(), DenseVector/SparseVector, HDFS-aware readers,
 * extra RDD operations and ml.tuning. Runs inside Pyodide; no network access.
 */
export const PYSPARK_EXTENSIONS = String.raw`
# ---------------------------------------------------------------------------
# PySpark compatibility extensions (loaded after the base in-memory runtime).
# Goal: same results and same printed output as Apache Spark 3.x for the
# APIs students use in RDD / DataFrame / MLlib labs. Pure Python, no network.
# ---------------------------------------------------------------------------
import math as _m
import random as _rnd
import re as _re
import os as _os
import sys as _sys
import types as _types
import json as _json
import pickle as _pickle
import builtins as _bi

# ----------------------------------------------------------------- utilities
def _java_double(v):
    """Format a float the way Java's Double.toString does (Spark prints doubles this way)."""
    if v != v:
        return 'NaN'
    if v == float('inf'):
        return 'Infinity'
    if v == float('-inf'):
        return '-Infinity'
    if v == 0:
        return '-0.0' if _m.copysign(1.0, v) < 0 else '0.0'
    a = abs(v)
    if 1e-3 <= a < 1e7:
        s = repr(float(v))
        if 'e' in s or 'E' in s:
            s = format(v, '.17f').rstrip('0')
        if '.' not in s:
            s += '.0'
        if s.endswith('.'):
            s += '0'
        return s
    r = repr(float(v))
    mant, _, exp = r.partition('e')
    if not exp:
        mant, exp = format(v, '.16e').split('e')
        mant = mant.rstrip('0')
    if '.' not in mant:
        mant += '.0'
    if mant.endswith('.'):
        mant += '0'
    return mant + 'E' + str(int(exp))

def _spark_str(v):
    """Cell text used by DataFrame.show()."""
    if v is None:
        return 'null'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, float):
        return _java_double(v)
    if isinstance(v, (DenseVector, SparseVector)):
        return str(v)
    if isinstance(v, dict) and not isinstance(v, Row):
        return '{' + ', '.join(_spark_str(k) + ' -> ' + _spark_str(x) for k, x in v.items()) + '}'
    if isinstance(v, Row):
        return '{' + ', '.join(_spark_str(x) for x in v.values()) + '}'
    if isinstance(v, (list, tuple)):
        return '[' + ', '.join(_spark_str(x) for x in v) + ']'
    return str(v)

# --------------------------------------------------------------- linalg
class DenseVector(list):
    """pyspark.ml.linalg.DenseVector: prints like Spark, behaves like a list."""
    def __init__(self, values=()):
        super().__init__(float(x) for x in values)
    def __repr__(self):
        return 'DenseVector([' + ', '.join(_java_double(x) for x in self) + '])'
    def __str__(self):
        return '[' + ','.join(_java_double(x) for x in self) + ']'
    @property
    def values(self):
        return list(self)
    def toArray(self):
        return _NdArray(self)
    def size_(self):
        return len(self)
    @property
    def size(self):
        return len(self)
    def dot(self, other):
        o = list(other.toArray()) if isinstance(other, SparseVector) else list(other)
        return float(sum(a * b for a, b in zip(self, o)))
    def norm(self, p):
        if p == float('inf'):
            return max(abs(x) for x in self) if self else 0.0
        return float(sum(abs(x) ** p for x in self) ** (1.0 / p))
    def squared_distance(self, other):
        o = list(other)
        return float(sum((a - b) ** 2 for a, b in zip(self, o)))
    def numNonzeros(self):
        return sum(1 for x in self if x != 0)
    def __eq__(self, other):
        try:
            return list(self) == [float(x) for x in other]
        except TypeError:
            return False
    __hash__ = None

class SparseVector(object):
    def __init__(self, size, *args):
        self.size = int(size)
        if len(args) == 1 and isinstance(args[0], dict):
            items = sorted((int(k), float(v)) for k, v in args[0].items())
        elif len(args) == 1:
            items = sorted((int(k), float(v)) for k, v in args[0])
        else:
            items = sorted(zip([int(i) for i in args[0]], [float(x) for x in args[1]]))
        self.indices = [i for i, _ in items]
        self.values = [v for _, v in items]
    def _map(self):
        return dict(zip(self.indices, self.values))
    def __repr__(self):
        return 'SparseVector(%d, {%s})' % (self.size, ', '.join('%d: %s' % (i, _java_double(v)) for i, v in zip(self.indices, self.values)))
    def __str__(self):
        return '(%d,[%s],[%s])' % (self.size, ','.join(str(i) for i in self.indices), ','.join(_java_double(v) for v in self.values))
    def __len__(self):
        return self.size
    def __getitem__(self, i):
        if i < 0:
            i += self.size
        return self._map().get(i, 0.0)
    def __iter__(self):
        m = self._map()
        return iter([m.get(i, 0.0) for i in range(self.size)])
    def toArray(self):
        return _NdArray(list(iter(self)))
    def toDense(self):
        return DenseVector(list(iter(self)))
    def dot(self, other):
        if isinstance(other, SparseVector):
            o = other._map()
            return float(sum(v * o.get(i, 0.0) for i, v in zip(self.indices, self.values)))
        o = list(other)
        return float(sum(v * o[i] for i, v in zip(self.indices, self.values)))
    def numNonzeros(self):
        return sum(1 for v in self.values if v != 0)
    def norm(self, p):
        return float(sum(abs(v) ** p for v in self.values) ** (1.0 / p))
    def __eq__(self, other):
        return isinstance(other, SparseVector) and self.size == other.size and self._map() == other._map()
    __hash__ = None

class _NdArray(list):
    """Tiny stand-in for numpy arrays returned by toArray()/clusterCenters()."""
    def __repr__(self):
        return 'array([' + ', '.join(repr(float(x)) for x in self) + '])'
    def tolist(self):
        return list(self)

class Vectors(object):
    @staticmethod
    def dense(*values):
        if len(values) == 1 and isinstance(values[0], (list, tuple, DenseVector, _NdArray)):
            values = values[0]
        return DenseVector(values)
    @staticmethod
    def sparse(size, *args):
        return SparseVector(size, *args)
    @staticmethod
    def zeros(size):
        return DenseVector([0.0] * size)
    @staticmethod
    def squared_distance(a, b):
        return float(sum((x - y) ** 2 for x, y in zip(list(a), list(b))))
    @staticmethod
    def norm(v, p):
        return v.norm(p)

def _items(v):
    """(index, value) pairs of a feature value (vector, list or number)."""
    if isinstance(v, SparseVector):
        return list(zip(v.indices, v.values))
    if isinstance(v, (int, float)):
        return [(0, float(v))]
    return [(i, float(x)) for i, x in enumerate(v)]

def _dense(v, n=None):
    if isinstance(v, SparseVector):
        return list(iter(v))
    if isinstance(v, (int, float)):
        return [float(v)]
    return [float(x) for x in v]

def _width(rows, col):
    w = 0
    for r in rows:
        v = r.get(col)
        if v is None:
            continue
        w = max(w, v.size if isinstance(v, SparseVector) else (1 if isinstance(v, (int, float)) else len(v)))
    return w

def _with(dataset, rows, new_cols):
    cols = list(dataset.columns) + [c for c in new_cols if c not in dataset.columns]
    return DataFrame(rows, cols, dataset._spark)

def _check_cols(dataset, *cols):
    missing = [c for c in cols if c and c not in dataset.columns]
    if missing:
        raise IllegalArgumentException('Field "%s" does not exist.\nAvailable fields: %s' % (missing[0], ', '.join(dataset.columns)))

class IllegalArgumentException(Exception):
    pass

class AnalysisException(Exception):
    pass

# ---------------------------------------------------- params usable in grids
class _P(object):
    """A parameter handle so ParamGridBuilder().addGrid(lr.regParam, [...]) works."""
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return 'Param(name=%r)' % self.name

class _PFloat(float):
    def __new__(cls, value, name):
        o = float.__new__(cls, value)
        o.name = name
        return o

class _PInt(int):
    def __new__(cls, value, name):
        o = int.__new__(cls, value)
        o.name = name
        return o

class _Params(object):
    _numeric = ()
    def _init_params(self, **kw):
        for k, v in kw.items():
            if k in self._numeric and isinstance(v, (int, float)) and not isinstance(v, bool):
                v = _PInt(v, k) if isinstance(v, int) else _PFloat(v, k)
            setattr(self, k, v)
    def _set(self, **kw):
        self._init_params(**kw)
        return self
    def __getattr__(self, name):
        if name.startswith('set') and len(name) > 3:
            key = name[3].lower() + name[4:]
            return lambda value: self._set(**{key: value})
        if name.startswith('get') and len(name) > 3:
            key = name[3].lower() + name[4:]
            if key in self.__dict__:
                return lambda: self.__dict__[key]
        raise AttributeError("'%s' object has no attribute '%s'" % (type(self).__name__, name))
    def copy(self, extra=None):
        import copy as _copy
        c = _copy.copy(self)
        for k, v in (extra or {}).items():
            c._init_params(**{getattr(k, 'name', k): v})
        return c
    def explainParams(self):
        return '\n'.join('%s: (current: %r)' % (k, v) for k, v in self.__dict__.items() if not k.startswith('_'))
    def extractParamMap(self):
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    def save(self, path):
        _save_obj(self, path)
    def write(self):
        return _Writer(self)
    @classmethod
    def load(cls, path):
        return _load_obj(path)
    @classmethod
    def read(cls):
        return _Reader()

def _model_path(path):
    p = str(path)
    if p.startswith('file://'):
        p = p[7:]
    return p

def _save_obj(obj, path, overwrite=False):
    p = _model_path(path)
    if _os.path.exists(p) and not overwrite:
        raise IOError('Path %s already exists. To overwrite it, please use write.overwrite().save(path) for Scala and use write().overwrite().save(path) for Java and Python.' % p)
    _os.makedirs(p, exist_ok=True)
    with open(_os.path.join(p, 'model.pkl'), 'wb') as f:
        _pickle.dump(obj, f)
    with open(_os.path.join(p, 'metadata.json'), 'w') as f:
        f.write(_json.dumps({'class': type(obj).__name__}))

def _load_obj(path):
    with open(_os.path.join(_model_path(path), 'model.pkl'), 'rb') as f:
        return _pickle.load(f)

class _Writer(object):
    def __init__(self, obj):
        self.obj = obj
        self._over = False
    def overwrite(self):
        self._over = True
        return self
    def save(self, path):
        _save_obj(self.obj, path, self._over)

class _Reader(object):
    def load(self, path):
        return _load_obj(path)

# Make existing base classes accept setters/getters/params too.
for _cls in (Transformer, Estimator):
    if not hasattr(_cls, '_numeric'):
        _cls._numeric = ()
    for _name in ('_init_params', '_set', '__getattr__', 'copy', 'explainParams', 'extractParamMap', 'save', 'write'):
        if not hasattr(_cls, _name) or _name in ('__getattr__',):
            setattr(_cls, _name, getattr(_Params, _name))
    _cls.load = classmethod(lambda cls, path: _load_obj(path))
    _cls.read = classmethod(lambda cls: _Reader())

# ------------------------------------------------------------ small linalg
def _solve(A, b):
    n = len(A)
    M = [list(map(float, A[i])) + [float(b[i])] for i in range(n)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-12:
            raise ValueError('singular')
        M[c], M[p] = M[p], M[c]
        for r in range(n):
            if r != c:
                f = M[r][c] / M[c][c]
                if f:
                    for k in range(c, n + 1):
                        M[r][k] -= f * M[c][k]
    return [M[i][n] / M[i][i] for i in range(n)]

def _inverse(A):
    n = len(A)
    M = [list(map(float, A[i])) + [1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-12:
            return None
        M[c], M[p] = M[p], M[c]
        piv = M[c][c]
        M[c] = [x / piv for x in M[c]]
        for r in range(n):
            if r != c and M[r][c]:
                f = M[r][c]
                M[r] = [a - f * b for a, b in zip(M[r], M[c])]
    return [row[n:] for row in M]

def _betainc(a, b, x):
    # Regularized incomplete beta via continued fraction (Numerical Recipes).
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lbeta = _m.lgamma(a + b) - _m.lgamma(a) - _m.lgamma(b) + a * _m.log(x) + b * _m.log(1 - x)
    def cf(a, b, x):
        qab, qap, qam = a + b, a + 1, a - 1
        c, d = 1.0, 1 - qab * x / qap
        d = 1 / d if abs(d) > 1e-300 else 1e300
        h = d
        for m in range(1, 300):
            m2 = 2 * m
            aa = m * (b - m) * x / ((qam + m2) * (a + m2))
            d = 1 + aa * d
            d = 1 / d if abs(d) > 1e-300 else 1e300
            c = 1 + aa / c if abs(c) > 1e-300 else 1e300
            h *= d * c
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
            d = 1 + aa * d
            d = 1 / d if abs(d) > 1e-300 else 1e300
            c = 1 + aa / c if abs(c) > 1e-300 else 1e300
            de = d * c
            h *= de
            if abs(de - 1) < 1e-15:
                break
        return h
    if x < (a + 1) / (a + b + 2):
        return _m.exp(lbeta) * cf(a, b, x) / a
    return 1 - _m.exp(lbeta) * cf(b, a, 1 - x) / b

def _t_pvalue(t, df):
    if df <= 0 or t != t:
        return float('nan')
    if t in (float('inf'), float('-inf')):
        return 0.0
    return _betainc(df / 2.0, 0.5, df / (df + t * t))

def _prep_xy(dataset, featuresCol, labelCol, weightCol=None):
    _check_cols(dataset, featuresCol, labelCol)
    rows = [r for r in dataset._rows if r.get(featuresCol) is not None and r.get(labelCol) is not None]
    if not rows:
        raise IllegalArgumentException('requirement failed: Dataset is empty or all rows contain nulls in %s / %s' % (featuresCol, labelCol))
    w = _width(rows, featuresCol)
    X = [_dense(r[featuresCol]) + [0.0] * (w - len(_dense(r[featuresCol]))) for r in rows]
    y = [float(r[labelCol]) for r in rows]
    wt = [float(r.get(weightCol, 1.0)) for r in rows] if weightCol else [1.0] * len(rows)
    return X, y, wt, w

# ---------------------------------------------------------- regression
class LinearRegressionSummary(object):
    def __init__(self, model, dataset, X=None):
        self._model = model
        pred = model.transform(dataset)
        self.predictions = pred
        self.predictionCol = model.predictionCol
        self.labelCol = model.labelCol
        self.featuresCol = model.featuresCol
        pairs = [(float(r[model.labelCol]), float(r[model.predictionCol])) for r in pred._rows if r.get(model.labelCol) is not None]
        n = len(pairs)
        self.numInstances = n
        ys = [a for a, _ in pairs]
        mean = sum(ys) / n if n else 0.0
        res = [a - b for a, b in pairs]
        sse = sum(e * e for e in res)
        sst = sum((a - mean) ** 2 for a in ys)
        k = len(model.coefficients)
        self.meanSquaredError = sse / n if n else 0.0
        self.rootMeanSquaredError = _m.sqrt(self.meanSquaredError)
        self.meanAbsoluteError = sum(abs(e) for e in res) / n if n else 0.0
        self.r2 = 1.0 - sse / sst if sst else (1.0 if sse == 0 else float('-inf'))
        dof = n - k - (1 if model.fitIntercept else 0)
        self.degreesOfFreedom = dof
        self.r2adj = 1.0 - (1.0 - self.r2) * (n - (1 if model.fitIntercept else 0)) / dof if dof > 0 else float('nan')
        self.explainedVariance = sum((b - mean) ** 2 for _, b in pairs) / n if n else 0.0
        self.residuals = DataFrame([{'residuals': e} for e in res], ['residuals'], dataset._spark)
        self.devianceResiduals = [min(res), max(res)] if res else []
        self.objectiveHistory = [0.0]
        self.totalIterations = 0
        # OLS standard errors (Spark: coefficients first, intercept last).
        se, tv, pv = [], [], []
        if X is not None and dof > 0 and model.regParam == 0:
            Xd = [row + ([1.0] if model.fitIntercept else []) for row in X]
            p = len(Xd[0])
            XtX = [[sum(r[i] * r[j] for r in Xd) for j in range(p)] for i in range(p)]
            inv = _inverse(XtX)
            sigma2 = sse / dof
            if inv is not None:
                coefs = list(model.coefficients) + ([model.intercept] if model.fitIntercept else [])
                for i in range(p):
                    s = _m.sqrt(max(inv[i][i] * sigma2, 0.0))
                    se.append(s)
                    t = coefs[i] / s if s else (float('inf') if coefs[i] else float('nan'))
                    tv.append(t)
                    pv.append(_t_pvalue(t, dof))
        self._se, self._tv, self._pv = se, tv, pv
    @property
    def coefficientStandardErrors(self):
        if not self._se:
            raise RuntimeError('No Std. Error of coefficients available for this LinearRegressionModel (needs solver="normal", regParam=0 and more rows than features)')
        return self._se
    @property
    def tValues(self):
        self.coefficientStandardErrors
        return self._tv
    @property
    def pValues(self):
        self.coefficientStandardErrors
        return self._pv

class LinearRegressionModel(Transformer):
    def __init__(self, coefficients, intercept, featuresCol, labelCol, predictionCol, fitIntercept, regParam):
        self.coefficients = DenseVector(coefficients)
        self.intercept = float(intercept)
        self.featuresCol = featuresCol
        self.labelCol = labelCol
        self.predictionCol = predictionCol
        self.fitIntercept = fitIntercept
        self.regParam = regParam
        self.numFeatures = len(coefficients)
        self.hasSummary = False
        self._summary = None
    @property
    def summary(self):
        if self._summary is None:
            raise RuntimeError('No training summary available for this LinearRegressionModel')
        return self._summary
    def predict(self, features):
        return self.intercept + sum(self.coefficients[i] * v for i, v in _items(features) if i < len(self.coefficients))
    def transform(self, dataset):
        _check_cols(dataset, self.featuresCol)
        rows = []
        for r in dataset._rows:
            v = r.get(self.featuresCol)
            rows.append({**r, self.predictionCol: None if v is None else float(self.predict(v))})
        return _with(dataset, rows, [self.predictionCol])
    def evaluate(self, dataset):
        return LinearRegressionSummary(self, dataset)
    def __repr__(self):
        return 'LinearRegressionModel: uid=LinearRegression_%s, numFeatures=%d' % (hex(id(self))[-12:], self.numFeatures)

class LinearRegression(Estimator):
    _numeric = ('maxIter', 'regParam', 'elasticNetParam', 'tol', 'aggregationDepth', 'epsilon')
    def __init__(self, featuresCol='features', labelCol='label', predictionCol='prediction', maxIter=100, regParam=0.0,
                 elasticNetParam=0.0, tol=1e-6, fitIntercept=True, standardization=True, solver='auto', weightCol=None,
                 aggregationDepth=2, loss='squaredError', epsilon=1.35, **kwargs):
        self._init_params(featuresCol=featuresCol, labelCol=labelCol, predictionCol=predictionCol, maxIter=maxIter,
                          regParam=regParam, elasticNetParam=elasticNetParam, tol=tol, fitIntercept=fitIntercept,
                          standardization=standardization, solver=solver, weightCol=weightCol, aggregationDepth=aggregationDepth,
                          loss=loss, epsilon=epsilon)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        X, y, wt, p = _prep_xy(dataset, est.featuresCol, est.labelCol, est.weightCol)
        n = len(X)
        W = sum(wt)
        xm = [sum(wt[i] * X[i][j] for i in range(n)) / W for j in range(p)] if est.fitIntercept else [0.0] * p
        ym = sum(wt[i] * y[i] for i in range(n)) / W if est.fitIntercept else 0.0
        Xc = [[X[i][j] - xm[j] for j in range(p)] for i in range(n)]
        yc = [y[i] - ym for i in range(n)]
        xstd = [_m.sqrt(sum(wt[i] * Xc[i][j] ** 2 for i in range(n)) / max(W - 1, 1)) for j in range(p)]
        ystd = _m.sqrt(sum(wt[i] * yc[i] ** 2 for i in range(n)) / max(W - 1, 1))
        lam = float(est.regParam)
        alpha = float(est.elasticNetParam)
        if lam > 0 and alpha > 0:
            coef = self._coordinate_descent(Xc, yc, wt, lam, alpha, xstd, ystd, est)
        else:
            A = [[sum(wt[i] * Xc[i][a] * Xc[i][b] for i in range(n)) / W for b in range(p)] for a in range(p)]
            rhs = [sum(wt[i] * Xc[i][a] * yc[i] for i in range(n)) / W for a in range(p)]
            if lam > 0:
                # L2 penalty on standardized coefficients, scaled by the label std (as WeightedLeastSquares).
                for j in range(p):
                    scale = (xstd[j] ** 2 if est.standardization else 1.0)
                    A[j][j] += lam * scale / (ystd if ystd else 1.0)
            try:
                coef = _solve(A, rhs)
            except ValueError:
                # Collinear features: fall back to a tiny ridge, like Spark's quasi-Newton fallback.
                for j in range(p):
                    A[j][j] += 1e-8
                coef = _solve(A, rhs)
        intercept = ym - sum(coef[j] * xm[j] for j in range(p)) if est.fitIntercept else 0.0
        model = LinearRegressionModel(coef, intercept, est.featuresCol, est.labelCol, est.predictionCol, est.fitIntercept, lam)
        model._summary = LinearRegressionSummary(model, dataset, X)
        model.hasSummary = True
        return model
    def _coordinate_descent(self, Xc, yc, wt, lam, alpha, xstd, ystd, est):
        n, p = len(Xc), len(Xc[0])
        W = sum(wt)
        coef = [0.0] * p
        z = [sum(wt[i] * Xc[i][j] ** 2 for i in range(n)) / W for j in range(p)]
        for _ in range(max(1, int(est.maxIter)) * 10):
            maxd = 0.0
            for j in range(p):
                r = sum(wt[i] * Xc[i][j] * (yc[i] - sum(coef[k] * Xc[i][k] for k in range(p) if k != j)) for i in range(n)) / W
                l1 = lam * alpha * (xstd[j] if est.standardization else 1.0)
                l2 = lam * (1 - alpha) * (xstd[j] ** 2 if est.standardization else 1.0)
                new = (_m.copysign(max(abs(r) - l1, 0.0), r)) / (z[j] + l2) if (z[j] + l2) else 0.0
                maxd = max(maxd, abs(new - coef[j]))
                coef[j] = new
            if maxd < float(est.tol) * 1e-3:
                break
        return coef

# ------------------------------------------------------- classification
def _sigmoid(z):
    if z >= 0:
        return 1.0 / (1.0 + _m.exp(-z))
    e = _m.exp(z)
    return e / (1.0 + e)

def _auc(scores_labels):
    pos = sum(1 for _, l in scores_labels if l == 1.0)
    neg = len(scores_labels) - pos
    if pos == 0 or neg == 0:
        return 0.0
    ordered = sorted(scores_labels, key=lambda t: t[0])
    ranks = {}
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        avg = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    rank_sum = sum(ranks[k] for k, (_, l) in enumerate(ordered) if l == 1.0)
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)

def _class_metrics(pairs):
    labels = sorted(set([l for l, _ in pairs] + [p for _, p in pairs]))
    n = len(pairs)
    out = {}
    out['accuracy'] = sum(1 for l, p in pairs if l == p) / n if n else 0.0
    wp = wr = wf = wfpr = 0.0
    per = {}
    for c in labels:
        tp = sum(1 for l, p in pairs if l == c and p == c)
        fp = sum(1 for l, p in pairs if l != c and p == c)
        fn = sum(1 for l, p in pairs if l == c and p != c)
        tn = n - tp - fp - fn
        support = tp + fn
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / support if support else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        fpr = fp / (fp + tn) if fp + tn else 0.0
        per[c] = (prec, rec, f1, fpr)
        weight = support / n if n else 0.0
        wp += weight * prec
        wr += weight * rec
        wf += weight * f1
        wfpr += weight * fpr
    out['weightedPrecision'] = wp
    out['weightedRecall'] = wr
    out['f1'] = wf
    out['weightedFMeasure'] = wf
    out['weightedTruePositiveRate'] = wr
    out['weightedFalsePositiveRate'] = wfpr
    out['hammingLoss'] = 1.0 - out['accuracy']
    out['_per'] = per
    out['_labels'] = labels
    return out

class _ClassificationSummary(object):
    def __init__(self, model, dataset):
        pred = model.transform(dataset)
        self.predictions = pred
        self.labelCol = model.labelCol
        self.predictionCol = model.predictionCol
        self.probabilityCol = getattr(model, 'probabilityCol', 'probability')
        pairs = [(float(r[model.labelCol]), float(r[model.predictionCol])) for r in pred._rows if r.get(model.labelCol) is not None]
        m = _class_metrics(pairs)
        self._m = m
        self.accuracy = m['accuracy']
        self.weightedPrecision = m['weightedPrecision']
        self.weightedRecall = m['weightedRecall']
        self.weightedFMeasure = lambda beta=1.0: m['weightedFMeasure']
        self.weightedTruePositiveRate = m['weightedTruePositiveRate']
        self.weightedFalsePositiveRate = m['weightedFalsePositiveRate']
        self.labels = m['_labels']
        self.precisionByLabel = [m['_per'][c][0] for c in m['_labels']]
        self.recallByLabel = [m['_per'][c][1] for c in m['_labels']]
        self.fMeasureByLabel = lambda beta=1.0: [m['_per'][c][2] for c in m['_labels']]
        self.truePositiveRateByLabel = self.recallByLabel
        self.falsePositiveRateByLabel = [m['_per'][c][3] for c in m['_labels']]
        self.objectiveHistory = getattr(model, '_history', [])
        self.totalIterations = len(self.objectiveHistory)
        if getattr(model, 'numClasses', 2) == 2:
            sl = [(float(_dense(r['probability'])[1]), float(r[model.labelCol])) for r in pred._rows if r.get('probability') is not None and r.get(model.labelCol) is not None]
            self.areaUnderROC = _auc(sl)
            pts = sorted(set(s for s, _ in sl), reverse=True)
            P = sum(1 for _, l in sl if l == 1.0) or 1
            N = sum(1 for _, l in sl if l != 1.0) or 1
            roc = [{'FPR': 0.0, 'TPR': 0.0}]
            for t in pts:
                roc.append({'FPR': sum(1 for s, l in sl if s >= t and l != 1.0) / N, 'TPR': sum(1 for s, l in sl if s >= t and l == 1.0) / P})
            roc.append({'FPR': 1.0, 'TPR': 1.0})
            self.roc = DataFrame(roc, ['FPR', 'TPR'], dataset._spark)

class LogisticRegressionModel(Transformer):
    def __init__(self, W, b, numClasses, featuresCol, labelCol, predictionCol, probabilityCol, rawPredictionCol, threshold, history):
        self.numClasses = numClasses
        self.numFeatures = len(W[0]) if W else 0
        self.featuresCol, self.labelCol, self.predictionCol = featuresCol, labelCol, predictionCol
        self.probabilityCol, self.rawPredictionCol = probabilityCol, rawPredictionCol
        self.threshold = threshold
        self._history = history
        self._W, self._b = W, b
        if numClasses == 2:
            self.coefficients = DenseVector(W[0])
            self.intercept = float(b[0])
        self.coefficientMatrix = [DenseVector(w) for w in W]
        self.interceptVector = DenseVector(b)
        self._summary = None
        self.hasSummary = False
    @property
    def summary(self):
        if self._summary is None:
            raise RuntimeError('No training summary available for this LogisticRegressionModel')
        return self._summary
    def _raw(self, v):
        it = _items(v)
        return [self._b[k] + sum(self._W[k][i] * x for i, x in it if i < len(self._W[k])) for k in range(len(self._W))]
    def predictRaw(self, v):
        m = self._raw(v)
        return DenseVector([-m[0], m[0]]) if self.numClasses == 2 else DenseVector(m)
    def predictProbability(self, v):
        m = self._raw(v)
        if self.numClasses == 2:
            p = _sigmoid(m[0])
            return DenseVector([1 - p, p])
        mx = max(m)
        e = [_m.exp(x - mx) for x in m]
        s = sum(e)
        return DenseVector([x / s for x in e])
    def predict(self, v):
        prob = self.predictProbability(v)
        if self.numClasses == 2:
            return 1.0 if prob[1] > float(self.threshold) else 0.0
        return float(max(range(len(prob)), key=lambda k: prob[k]))
    def transform(self, dataset):
        _check_cols(dataset, self.featuresCol)
        rows = []
        for r in dataset._rows:
            v = r.get(self.featuresCol)
            if v is None:
                rows.append({**r, self.rawPredictionCol: None, self.probabilityCol: None, self.predictionCol: None})
                continue
            rows.append({**r, self.rawPredictionCol: self.predictRaw(v), self.probabilityCol: self.predictProbability(v), self.predictionCol: self.predict(v)})
        return _with(dataset, rows, [self.rawPredictionCol, self.probabilityCol, self.predictionCol])
    def evaluate(self, dataset):
        return _ClassificationSummary(self, dataset)
    def __repr__(self):
        return 'LogisticRegressionModel: uid=LogisticRegression_%s, numClasses=%d, numFeatures=%d' % (hex(id(self))[-12:], self.numClasses, self.numFeatures)

class LogisticRegression(Estimator):
    _numeric = ('maxIter', 'regParam', 'elasticNetParam', 'tol', 'threshold')
    def __init__(self, featuresCol='features', labelCol='label', predictionCol='prediction', maxIter=100, regParam=0.0,
                 elasticNetParam=0.0, tol=1e-6, fitIntercept=True, threshold=0.5, probabilityCol='probability',
                 rawPredictionCol='rawPrediction', standardization=True, weightCol=None, family='auto', **kwargs):
        self._init_params(featuresCol=featuresCol, labelCol=labelCol, predictionCol=predictionCol, maxIter=maxIter,
                          regParam=regParam, elasticNetParam=elasticNetParam, tol=tol, fitIntercept=fitIntercept,
                          threshold=threshold, probabilityCol=probabilityCol, rawPredictionCol=rawPredictionCol,
                          standardization=standardization, weightCol=weightCol, family=family)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        _check_cols(dataset, est.featuresCol, est.labelCol)
        rows = [r for r in dataset._rows if r.get(est.featuresCol) is not None and r.get(est.labelCol) is not None]
        if not rows:
            raise IllegalArgumentException('requirement failed: Dataset is empty')
        y = [float(r[est.labelCol]) for r in rows]
        for v in y:
            if v < 0 or v != int(v):
                raise IllegalArgumentException('Classification labels should be in [0 to %d]. Found invalid label: %s.' % (int(max(y)), v))
        K = int(max(y)) + 1
        multinomial = est.family == 'multinomial' or (est.family == 'auto' and K > 2)
        p = _width(rows, est.featuresCol)
        feats = [_items(r[est.featuresCol]) for r in rows]
        n = len(rows)
        lam = float(est.regParam)
        history = []
        if not multinomial:
            K = max(K, 2)
            if p <= 60:
                # Newton / IRLS: exact maximum-likelihood (plus L2), converges like Spark's L-BFGS.
                X = [[1.0] + [0.0] * p for _ in range(n)]
                for i, it in enumerate(feats):
                    for j, v in it:
                        X[i][j + 1] = v
                if not est.fitIntercept:
                    X = [row[1:] for row in X]
                d = len(X[0])
                w = [0.0] * d
                for _ in range(max(1, int(est.maxIter))):
                    mu = [_sigmoid(sum(w[j] * X[i][j] for j in range(d))) for i in range(n)]
                    loss = -sum(_m.log(max(mu[i], 1e-300)) if y[i] == 1 else _m.log(max(1 - mu[i], 1e-300)) for i in range(n)) / n
                    history.append(loss)
                    g = [sum((mu[i] - y[i]) * X[i][j] for i in range(n)) / n for j in range(d)]
                    H = [[sum(mu[i] * (1 - mu[i]) * X[i][a] * X[i][b] for i in range(n)) / n for b in range(d)] for a in range(d)]
                    for j in range(d):
                        if not (est.fitIntercept and j == 0):
                            g[j] += lam * w[j]
                            H[j][j] += lam
                        H[j][j] += 1e-9
                    try:
                        step = _solve(H, g)
                    except ValueError:
                        break
                    w = [w[j] - step[j] for j in range(d)]
                    if max(abs(s) for s in step) < float(est.tol):
                        break
                    if max(abs(x) for x in w) > 1e6:
                        break
                b = w[0] if est.fitIntercept else 0.0
                coef = w[1:] if est.fitIntercept else w
                W, B = [coef], [b]
            else:
                W, B, history = self._sgd(feats, y, p, 1, lam, est)
        else:
            W, B, history = self._sgd(feats, y, p, K, lam, est)
        model = LogisticRegressionModel(W, B, K, est.featuresCol, est.labelCol, est.predictionCol, est.probabilityCol,
                                        est.rawPredictionCol, est.threshold, history)
        model._summary = _ClassificationSummary(model, dataset)
        model.hasSummary = True
        return model
    def _sgd(self, feats, y, p, K, lam, est):
        # Full-batch gradient descent with Adam on (possibly sparse) features; K==1 means binary.
        n = len(feats)
        classes = K if K > 1 else 1
        W = [[0.0] * p for _ in range(classes)]
        B = [0.0] * classes
        mW = [dict() for _ in range(classes)]
        vW = [dict() for _ in range(classes)]
        mB = [0.0] * classes
        vB = [0.0] * classes
        lr, b1, b2, eps = 0.1, 0.9, 0.999, 1e-8
        history = []
        for t in range(1, max(1, int(est.maxIter)) * 3 + 1):
            gW = [dict() for _ in range(classes)]
            gB = [0.0] * classes
            loss = 0.0
            for i in range(n):
                raw = [B[k] + sum(W[k][j] * v for j, v in feats[i]) for k in range(classes)]
                if classes == 1:
                    pr = [_sigmoid(raw[0])]
                    err = [pr[0] - y[i]]
                    loss -= _m.log(max(pr[0] if y[i] == 1 else 1 - pr[0], 1e-300))
                else:
                    mx = max(raw)
                    e = [_m.exp(r - mx) for r in raw]
                    s = sum(e)
                    pr = [x / s for x in e]
                    err = [pr[k] - (1.0 if int(y[i]) == k else 0.0) for k in range(classes)]
                    loss -= _m.log(max(pr[int(y[i])], 1e-300))
                for k in range(classes):
                    gB[k] += err[k] / n
                    for j, v in feats[i]:
                        gW[k][j] = gW[k].get(j, 0.0) + err[k] * v / n
            history.append(loss / n)
            for k in range(classes):
                for j in list(gW[k].keys()):
                    g = gW[k][j] + lam * W[k][j]
                    mW[k][j] = b1 * mW[k].get(j, 0.0) + (1 - b1) * g
                    vW[k][j] = b2 * vW[k].get(j, 0.0) + (1 - b2) * g * g
                    W[k][j] -= lr * (mW[k][j] / (1 - b1 ** t)) / (_m.sqrt(vW[k][j] / (1 - b2 ** t)) + eps)
                if est.fitIntercept:
                    mB[k] = b1 * mB[k] + (1 - b1) * gB[k]
                    vB[k] = b2 * vB[k] + (1 - b2) * gB[k] ** 2
                    B[k] -= lr * (mB[k] / (1 - b1 ** t)) / (_m.sqrt(vB[k] / (1 - b2 ** t)) + eps)
            if len(history) > 2 and abs(history[-2] - history[-1]) < float(est.tol) * 1e-2:
                break
        return W, B, history

# --------------------------------------------------------- decision trees
def _gini(counts):
    t = sum(counts.values())
    return 1.0 - sum((c / t) ** 2 for c in counts.values()) if t else 0.0

def _entropy(counts):
    t = sum(counts.values())
    return -sum((c / t) * _m.log(c / t, 2) for c in counts.values() if c) if t else 0.0

def _variance(ys):
    n = len(ys)
    if not n:
        return 0.0
    mu = sum(ys) / n
    return sum((v - mu) ** 2 for v in ys) / n

class _Node(object):
    __slots__ = ('feature', 'threshold', 'left', 'right', 'prediction', 'stats', 'impurity', 'gain', 'n')
    def __init__(self):
        self.feature = None
        self.threshold = None
        self.left = self.right = None
        self.prediction = 0.0
        self.stats = None
        self.impurity = 0.0
        self.gain = 0.0
        self.n = 0

def _build_tree(X, y, idx, depth, maxDepth, minInst, classification, impurity, numClasses, feature_pool, rng, importances, minInfoGain):
    node = _Node()
    node.n = len(idx)
    ys = [y[i] for i in idx]
    if classification:
        counts = {}
        for v in ys:
            counts[v] = counts.get(v, 0) + 1
        node.stats = [float(counts.get(float(c), 0)) for c in range(numClasses)]
        node.prediction = float(max(range(numClasses), key=lambda c: (node.stats[c], -c)))
        imp = (_entropy if impurity == 'entropy' else _gini)(counts)
    else:
        node.prediction = sum(ys) / len(ys) if ys else 0.0
        imp = _variance(ys)
    node.impurity = imp
    if depth >= maxDepth or len(idx) < 2 * minInst or imp == 0.0:
        return node
    feats = feature_pool(len(X[0]), rng)
    best = None
    for f in feats:
        vals = sorted(set(X[i][f] for i in idx))
        if len(vals) < 2:
            continue
        cands = [(vals[k] + vals[k + 1]) / 2.0 for k in range(len(vals) - 1)]
        if len(cands) > 32:
            step = len(cands) / 32.0
            cands = [cands[int(k * step)] for k in range(32)]
        for t in cands:
            L = [i for i in idx if X[i][f] <= t]
            R = [i for i in idx if X[i][f] > t]
            if len(L) < minInst or len(R) < minInst:
                continue
            if classification:
                cl, cr = {}, {}
                for i in L:
                    cl[y[i]] = cl.get(y[i], 0) + 1
                for i in R:
                    cr[y[i]] = cr.get(y[i], 0) + 1
                fn = _entropy if impurity == 'entropy' else _gini
                child = (len(L) * fn(cl) + len(R) * fn(cr)) / len(idx)
            else:
                child = (len(L) * _variance([y[i] for i in L]) + len(R) * _variance([y[i] for i in R])) / len(idx)
            gain = imp - child
            if gain > minInfoGain and (best is None or gain > best[0] + 1e-12):
                best = (gain, f, t, L, R)
    if best is None:
        return node
    gain, f, t, L, R = best
    node.feature, node.threshold, node.gain = f, t, gain
    importances[f] = importances.get(f, 0.0) + gain * len(idx)
    node.left = _build_tree(X, y, L, depth + 1, maxDepth, minInst, classification, impurity, numClasses, feature_pool, rng, importances, minInfoGain)
    node.right = _build_tree(X, y, R, depth + 1, maxDepth, minInst, classification, impurity, numClasses, feature_pool, rng, importances, minInfoGain)
    return node

def _tree_leaf(node, x):
    while node.feature is not None:
        node = node.left if (x[node.feature] if node.feature < len(x) else 0.0) <= node.threshold else node.right
    return node

def _tree_depth(node):
    return 0 if node.feature is None else 1 + max(_tree_depth(node.left), _tree_depth(node.right))

def _tree_count(node):
    return 1 if node.feature is None else 1 + _tree_count(node.left) + _tree_count(node.right)

def _tree_debug(node, indent=' '):
    if node.feature is None:
        return indent + 'Predict: ' + _java_double(node.prediction) + '\n'
    return (indent + 'If (feature %d <= %s)\n' % (node.feature, _java_double(node.threshold)) + _tree_debug(node.left, indent + ' ') +
            indent + 'Else (feature %d > %s)\n' % (node.feature, _java_double(node.threshold)) + _tree_debug(node.right, indent + ' '))

def _normalize_importances(imp, p):
    total = sum(imp.values())
    return SparseVector(p, {k: v / total for k, v in sorted(imp.items()) if v}) if total else SparseVector(p, {})

class _TreeModelBase(Transformer):
    def __init__(self, trees, classification, numClasses, numFeatures, est, importances, weights=None):
        self._trees = trees
        self._classification = classification
        self.numClasses = numClasses
        self.numFeatures = numFeatures
        self.featuresCol, self.labelCol, self.predictionCol = est.featuresCol, est.labelCol, est.predictionCol
        self.probabilityCol = getattr(est, 'probabilityCol', 'probability')
        self.rawPredictionCol = getattr(est, 'rawPredictionCol', 'rawPrediction')
        self.featureImportances = _normalize_importances(importances, numFeatures)
        self._summary = None
    @property
    def depth(self):
        return _tree_depth(self._trees[0])
    @property
    def numNodes(self):
        return _tree_count(self._trees[0])
    @property
    def trees(self):
        return self._trees
    @property
    def getNumTrees(self):
        return len(self._trees)
    @property
    def treeWeights(self):
        return [1.0] * len(self._trees)
    @property
    def toDebugString(self):
        if len(self._trees) == 1:
            uid = type(self).__name__.replace('ClassificationModel', 'Classifier').replace('RegressionModel', 'Regressor') + '_' + hex(id(self))[2:14]
            return '%s: uid=%s, depth=%d, numNodes=%d, numClasses=%d, numFeatures=%d\n' % (type(self).__name__, uid, self.depth, self.numNodes, self.numClasses, self.numFeatures) + _tree_debug(self._trees[0], '  ') + _tree_debug(self._trees[0], '  ')
        out = '%s (uid=%s) with %d trees\n' % (type(self).__name__, hex(id(self))[-12:], len(self._trees))
        for k, t in enumerate(self._trees):
            out += '  Tree %d (weight 1.0):\n' % k + _tree_debug(t, '    ')
        return out
    def predict(self, v):
        x = _dense(v)
        if self._classification:
            prob = self._prob(x)
            return float(max(range(len(prob)), key=lambda k: (prob[k], -k)))
        return sum(_tree_leaf(t, x).prediction for t in self._trees) / len(self._trees)
    def _raw(self, x):
        raw = [0.0] * self.numClasses
        for t in self._trees:
            leaf = _tree_leaf(t, x)
            s = sum(leaf.stats)
            for k in range(self.numClasses):
                raw[k] += (leaf.stats[k] if len(self._trees) == 1 else (leaf.stats[k] / s if s else 0.0))
        return raw
    def _prob(self, x):
        raw = self._raw(x)
        s = sum(raw)
        return [r / s for r in raw] if s else [1.0 / self.numClasses] * self.numClasses
    def transform(self, dataset):
        _check_cols(dataset, self.featuresCol)
        rows = []
        for r in dataset._rows:
            v = r.get(self.featuresCol)
            if v is None:
                rows.append(dict(r))
                continue
            x = _dense(v)
            if self._classification:
                raw = self._raw(x)
                prob = self._prob(x)
                rows.append({**r, self.rawPredictionCol: DenseVector(raw), self.probabilityCol: DenseVector(prob), self.predictionCol: self.predict(x)})
            else:
                rows.append({**r, self.predictionCol: self.predict(x)})
        new = [self.rawPredictionCol, self.probabilityCol, self.predictionCol] if self._classification else [self.predictionCol]
        return _with(dataset, rows, new)

class DecisionTreeClassificationModel(_TreeModelBase):
    pass

class DecisionTreeRegressionModel(_TreeModelBase):
    pass

class RandomForestClassificationModel(_TreeModelBase):
    pass

class RandomForestRegressionModel(_TreeModelBase):
    pass

def _all_features(p, rng):
    return list(range(p))

class _TreeEstimator(Estimator):
    _numeric = ('maxDepth', 'minInstancesPerNode', 'maxBins', 'numTrees', 'subsamplingRate', 'minInfoGain', 'seed')
    _classification = True
    _forest = False
    def __init__(self, featuresCol='features', labelCol='label', predictionCol='prediction', maxDepth=5, maxBins=32,
                 minInstancesPerNode=1, minInfoGain=0.0, impurity=None, seed=None, numTrees=20,
                 featureSubsetStrategy='auto', subsamplingRate=1.0, bootstrap=True, probabilityCol='probability',
                 rawPredictionCol='rawPrediction', **kwargs):
        self._init_params(featuresCol=featuresCol, labelCol=labelCol, predictionCol=predictionCol, maxDepth=maxDepth,
                          maxBins=maxBins, minInstancesPerNode=minInstancesPerNode, minInfoGain=minInfoGain,
                          impurity=impurity or ('gini' if self._classification else 'variance'), seed=seed,
                          numTrees=numTrees, featureSubsetStrategy=featureSubsetStrategy, subsamplingRate=subsamplingRate,
                          bootstrap=bootstrap, probabilityCol=probabilityCol, rawPredictionCol=rawPredictionCol)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        X, y, _, p = _prep_xy(dataset, est.featuresCol, est.labelCol)
        numClasses = 0
        if self._classification:
            for v in y:
                if v < 0 or v != int(v):
                    raise IllegalArgumentException('Classifier was given dataset with invalid label %s. Labels must be integers in range [0, 1, ..., %d)' % (v, int(max(y)) + 1))
            numClasses = max(2, int(max(y)) + 1)
        rng = _rnd.Random(est.seed if est.seed is not None else 42)
        importances = {}
        trees = []
        if self._forest:
            strategy = est.featureSubsetStrategy
            if strategy == 'auto':
                strategy = 'sqrt' if self._classification else 'onethird'
            k = {'all': p, 'sqrt': max(1, int(_m.ceil(_m.sqrt(p)))), 'log2': max(1, int(_m.ceil(_m.log(p, 2)))) if p > 1 else 1,
                 'onethird': max(1, int(_m.ceil(p / 3.0)))}.get(strategy, p)
            if isinstance(strategy, str) and strategy.replace('.', '', 1).isdigit():
                k = max(1, int(float(strategy) * p)) if '.' in strategy else int(strategy)
            pool = (lambda pp, r: sorted(r.sample(range(pp), min(k, pp))))
            for _ in range(int(est.numTrees)):
                n = len(X)
                m = max(1, int(round(n * float(est.subsamplingRate))))
                idx = [rng.randrange(n) for _ in range(m)] if est.bootstrap else rng.sample(range(n), m)
                trees.append(_build_tree(X, y, idx, 0, int(est.maxDepth), int(est.minInstancesPerNode), self._classification,
                                         est.impurity, numClasses, pool, rng, importances, float(est.minInfoGain)))
        else:
            trees.append(_build_tree(X, y, list(range(len(X))), 0, int(est.maxDepth), int(est.minInstancesPerNode),
                                     self._classification, est.impurity, numClasses, _all_features, rng, importances, float(est.minInfoGain)))
        model = self._model_class(trees, self._classification, numClasses, p, est, importances)
        if self._classification:
            model._summary = None
        return model

class DecisionTreeClassifier(_TreeEstimator):
    _model_class = DecisionTreeClassificationModel

class DecisionTreeRegressor(_TreeEstimator):
    _classification = False
    _model_class = DecisionTreeRegressionModel

class RandomForestClassifier(_TreeEstimator):
    _forest = True
    _model_class = RandomForestClassificationModel

class RandomForestRegressor(_TreeEstimator):
    _classification = False
    _forest = True
    _model_class = RandomForestRegressionModel

# ------------------------------------------------------------ naive bayes
class NaiveBayesModel(Transformer):
    def __init__(self, pi, theta, labels, est, modelType):
        self.pi = DenseVector(pi)
        self.theta = [DenseVector(t) for t in theta]
        self._labels = labels
        self.numClasses = len(labels)
        self.featuresCol, self.labelCol, self.predictionCol = est.featuresCol, est.labelCol, est.predictionCol
        self.probabilityCol, self.rawPredictionCol = est.probabilityCol, est.rawPredictionCol
        self.modelType = modelType
    def _raw(self, v):
        it = _items(v)
        if self.modelType == 'bernoulli':
            out = []
            for k in range(len(self.pi)):
                s = self.pi[k]
                vals = dict(it)
                for j, t in enumerate(self.theta[k]):
                    s += t if vals.get(j, 0.0) > 0 else _m.log(max(1 - _m.exp(t), 1e-300))
                out.append(s)
            return out
        return [self.pi[k] + sum(self.theta[k][j] * x for j, x in it if j < len(self.theta[k])) for k in range(len(self.pi))]
    def transform(self, dataset):
        rows = []
        for r in dataset._rows:
            raw = self._raw(r[self.featuresCol])
            mx = max(raw)
            e = [_m.exp(x - mx) for x in raw]
            s = sum(e)
            prob = [x / s for x in e]
            k = max(range(len(prob)), key=lambda i: (prob[i], -i))
            rows.append({**r, self.rawPredictionCol: DenseVector(raw), self.probabilityCol: DenseVector(prob), self.predictionCol: float(self._labels[k])})
        return _with(dataset, rows, [self.rawPredictionCol, self.probabilityCol, self.predictionCol])

class NaiveBayes(Estimator):
    _numeric = ('smoothing',)
    def __init__(self, featuresCol='features', labelCol='label', predictionCol='prediction', smoothing=1.0,
                 modelType='multinomial', probabilityCol='probability', rawPredictionCol='rawPrediction', **kwargs):
        self._init_params(featuresCol=featuresCol, labelCol=labelCol, predictionCol=predictionCol, smoothing=smoothing,
                          modelType=modelType, probabilityCol=probabilityCol, rawPredictionCol=rawPredictionCol)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        _check_cols(dataset, est.featuresCol, est.labelCol)
        rows = [r for r in dataset._rows if r.get(est.featuresCol) is not None]
        p = _width(rows, est.featuresCol)
        labels = sorted(set(float(r[est.labelCol]) for r in rows))
        n = len(rows)
        lam = float(est.smoothing)
        pi, theta = [], []
        for c in labels:
            sub = [r for r in rows if float(r[est.labelCol]) == c]
            pi.append(_m.log((len(sub) + lam) / (n + lam * len(labels))))
            sums = [0.0] * p
            for r in sub:
                for j, x in _items(r[est.featuresCol]):
                    if x < 0:
                        raise IllegalArgumentException('requirement failed: Naive Bayes requires nonnegative feature values but found %s.' % _dense(r[est.featuresCol]))
                    sums[j] += (1.0 if (est.modelType == 'bernoulli' and x > 0) else x)
            if est.modelType == 'bernoulli':
                theta.append([_m.log((s + lam) / (len(sub) + 2 * lam)) for s in sums])
            else:
                tot = sum(sums)
                theta.append([_m.log((s + lam) / (tot + lam * p)) for s in sums])
        return NaiveBayesModel(pi, theta, labels, est, est.modelType)

# -------------------------------------------------------------- clustering
class KMeansSummary(object):
    def __init__(self, model, dataset, cost, iters):
        self.predictions = model.transform(dataset)
        self.trainingCost = cost
        self.k = model.k
        self.numIter = iters
        sizes = [0] * model.k
        for r in self.predictions._rows:
            sizes[int(r[model.predictionCol])] += 1
        self.clusterSizes = sizes
        self.cluster = self.predictions.select(model.predictionCol)
        self.featuresCol = model.featuresCol
        self.predictionCol = model.predictionCol

class KMeansModel(Transformer):
    def __init__(self, centers, est):
        self._centers = centers
        self.k = len(centers)
        self.featuresCol, self.predictionCol = est.featuresCol, est.predictionCol
        self._summary = None
        self.hasSummary = False
    def clusterCenters(self):
        return [_NdArray(c) for c in self._centers]
    @property
    def summary(self):
        return self._summary
    def predict(self, v):
        x = _dense(v)
        return min(range(self.k), key=lambda c: (sum((a - b) ** 2 for a, b in zip(x, self._centers[c])), c))
    def transform(self, dataset):
        _check_cols(dataset, self.featuresCol)
        rows = [{**r, self.predictionCol: self.predict(r[self.featuresCol])} for r in dataset._rows]
        return _with(dataset, rows, [self.predictionCol])

class KMeans(Estimator):
    _numeric = ('k', 'maxIter', 'tol', 'seed', 'initSteps')
    def __init__(self, featuresCol='features', predictionCol='prediction', k=2, maxIter=20, seed=None, tol=1e-4,
                 initMode='k-means||', initSteps=2, distanceMeasure='euclidean', **kwargs):
        self._init_params(featuresCol=featuresCol, predictionCol=predictionCol, k=k, maxIter=maxIter, seed=seed, tol=tol,
                          initMode=initMode, initSteps=initSteps, distanceMeasure=distanceMeasure)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        _check_cols(dataset, est.featuresCol)
        pts = [_dense(r[est.featuresCol]) for r in dataset._rows if r.get(est.featuresCol) is not None]
        k = int(est.k)
        if k < 2:
            raise IllegalArgumentException('requirement failed: k must be > 1, got %d' % k)
        rng = _rnd.Random(est.seed if est.seed is not None else 42)
        uniq = []
        for p_ in pts:
            if p_ not in uniq:
                uniq.append(p_)
        d2 = lambda a, b: sum((x - y) ** 2 for x, y in zip(a, b))
        centers = [list(rng.choice(uniq))] if uniq else []
        while len(centers) < min(k, len(uniq)):
            w = [min(d2(p_, c) for c in centers) for p_ in uniq]
            tot = sum(w)
            if tot == 0:
                break
            r = rng.random() * tot
            acc = 0.0
            for p_, wi in zip(uniq, w):
                acc += wi
                if acc >= r:
                    centers.append(list(p_))
                    break
        iters = 0
        for iters in range(1, int(est.maxIter) + 1):
            groups = [[] for _ in centers]
            for p_ in pts:
                groups[min(range(len(centers)), key=lambda c: (d2(p_, centers[c]), c))].append(p_)
            new = [[sum(col) / len(g) for col in zip(*g)] if g else centers[i] for i, g in enumerate(groups)]
            moved = max(_m.sqrt(d2(a, b)) for a, b in zip(new, centers)) if centers else 0
            centers = new
            if moved <= float(est.tol):
                break
        model = KMeansModel(centers, est)
        cost = sum(min(d2(p_, c) for c in centers) for p_ in pts)
        model._summary = KMeansSummary(model, dataset, cost, iters)
        model.hasSummary = True
        return model

class ClusteringEvaluator(_Params):
    def __init__(self, predictionCol='prediction', featuresCol='features', metricName='silhouette', distanceMeasure='squaredEuclidean', **kwargs):
        self._init_params(predictionCol=predictionCol, featuresCol=featuresCol, metricName=metricName, distanceMeasure=distanceMeasure)
    def isLargerBetter(self):
        return True
    def evaluate(self, dataset, params=None):
        pts = [(_dense(r[self.featuresCol]), r[self.predictionCol]) for r in dataset._rows]
        clusters = sorted(set(c for _, c in pts))
        if len(clusters) < 2:
            raise AssertionError('Number of clusters must be greater than one.')
        if self.distanceMeasure == 'cosine':
            def dist(a, b):
                na = _m.sqrt(sum(x * x for x in a)) or 1.0
                nb = _m.sqrt(sum(x * x for x in b)) or 1.0
                return 1 - sum(x * y for x, y in zip(a, b)) / (na * nb)
        else:
            def dist(a, b):
                return sum((x - y) ** 2 for x, y in zip(a, b))
        total = 0.0
        for i, (p_, c) in enumerate(pts):
            same = [dist(p_, q) for j, (q, cc) in enumerate(pts) if cc == c and j != i]
            if not same:
                continue
            a = sum(same) / len(same)
            b = min(sum(dist(p_, q) for q, cc in pts if cc == o) / sum(1 for _, cc in pts if cc == o) for o in clusters if o != c)
            total += (b - a) / max(a, b) if max(a, b) else 0.0
        return total / len(pts)

# ------------------------------------------------------------ evaluators
class RegressionEvaluator(_Params):
    def __init__(self, predictionCol='prediction', labelCol='label', metricName='rmse', weightCol=None, throughOrigin=False, **kwargs):
        self._init_params(predictionCol=predictionCol, labelCol=labelCol, metricName=metricName, weightCol=weightCol, throughOrigin=throughOrigin)
    def isLargerBetter(self):
        return self.metricName in ('r2', 'var')
    def evaluate(self, dataset, params=None):
        ev = self.copy(params) if params else self
        _check_cols(dataset, ev.labelCol, ev.predictionCol)
        pairs = [(float(r[ev.labelCol]), float(r[ev.predictionCol])) for r in dataset._rows if r.get(ev.labelCol) is not None and r.get(ev.predictionCol) is not None]
        n = len(pairs)
        if not n:
            return float('nan')
        err = [p - y for y, p in pairs]
        mse = sum(e * e for e in err) / n
        if ev.metricName == 'mse':
            return mse
        if ev.metricName == 'mae':
            return sum(abs(e) for e in err) / n
        if ev.metricName == 'r2':
            mean = sum(y for y, _ in pairs) / n
            sst = sum((y - mean) ** 2 for y, _ in pairs)
            return 1.0 - (sum(e * e for e in err)) / sst if sst else float('nan')
        if ev.metricName == 'var':
            mean = sum(y for y, _ in pairs) / n
            return sum((p - mean) ** 2 for _, p in pairs) / n
        return _m.sqrt(mse)

class MulticlassClassificationEvaluator(_Params):
    def __init__(self, predictionCol='prediction', labelCol='label', metricName='f1', probabilityCol='probability', metricLabel=0.0, beta=1.0, **kwargs):
        self._init_params(predictionCol=predictionCol, labelCol=labelCol, metricName=metricName, probabilityCol=probabilityCol, metricLabel=metricLabel, beta=beta)
    def isLargerBetter(self):
        return self.metricName not in ('weightedFalsePositiveRate', 'falsePositiveRateByLabel', 'hammingLoss', 'logLoss')
    def evaluate(self, dataset, params=None):
        ev = self.copy(params) if params else self
        _check_cols(dataset, ev.labelCol, ev.predictionCol)
        pairs = [(float(r[ev.labelCol]), float(r[ev.predictionCol])) for r in dataset._rows if r.get(ev.labelCol) is not None]
        if ev.metricName == 'logLoss':
            eps = 1e-15
            vals = [-_m.log(min(max(_dense(r[ev.probabilityCol])[int(float(r[ev.labelCol]))], eps), 1 - eps)) for r in dataset._rows]
            return sum(vals) / len(vals)
        m = _class_metrics(pairs)
        lbl = float(ev.metricLabel)
        per = m['_per'].get(lbl, (0.0, 0.0, 0.0, 0.0))
        byLabel = {'precisionByLabel': per[0], 'recallByLabel': per[1], 'truePositiveRateByLabel': per[1], 'fMeasureByLabel': per[2], 'falsePositiveRateByLabel': per[3]}
        if ev.metricName in byLabel:
            return byLabel[ev.metricName]
        return m.get(ev.metricName, m['f1'])

class BinaryClassificationEvaluator(_Params):
    def __init__(self, rawPredictionCol='rawPrediction', labelCol='label', metricName='areaUnderROC', numBins=1000, **kwargs):
        self._init_params(rawPredictionCol=rawPredictionCol, labelCol=labelCol, metricName=metricName, numBins=numBins)
    def isLargerBetter(self):
        return True
    def evaluate(self, dataset, params=None):
        ev = self.copy(params) if params else self
        _check_cols(dataset, ev.rawPredictionCol, ev.labelCol)
        sl = []
        for r in dataset._rows:
            v = r.get(ev.rawPredictionCol)
            if v is None or r.get(ev.labelCol) is None:
                continue
            s = float(v) if isinstance(v, (int, float)) else float(_dense(v)[1] if len(_dense(v)) > 1 else _dense(v)[0])
            sl.append((s, float(r[ev.labelCol])))
        if ev.metricName == 'areaUnderPR':
            pts = sorted(sl, key=lambda t: -t[0])
            P = sum(1 for _, l in sl if l == 1.0)
            if not P:
                return 0.0
            area, tp, fp, prev_r, prev_p = 0.0, 0, 0, 0.0, 1.0
            i = 0
            while i < len(pts):
                j = i
                while j < len(pts) and pts[j][0] == pts[i][0]:
                    if pts[j][1] == 1.0:
                        tp += 1
                    else:
                        fp += 1
                    j += 1
                rec = tp / P
                prec = tp / (tp + fp)
                area += (rec - prev_r) * (prec + prev_p) / 2.0
                prev_r, prev_p = rec, prec
                i = j
            return area
        return _auc(sl)

# ------------------------------------------------------- text features
_ENGLISH_STOP = set("""i me my myself we our ours ourselves you your yours yourself yourselves he him his himself she her hers herself it its itself they them their theirs themselves what which who whom this that these those am is are was were be been being have has had having do does did doing a an the and but if or because as until while of at by for with about against between into through during before after above below to from up down in out on off over under again further then once here there when where why how all any both each few more most other some such no nor not only own same so than too very s t can will just don should now d ll m o re ve y ain aren couldn didn doesn hadn hasn haven isn ma mightn mustn needn shan shouldn wasn weren won wouldn""".split())

def _murmur3_32(data, seed=42):
    # MurmurHash3_x86_32 as used by Spark's HashingTF (UTF-8 bytes, seed 42).
    c1, c2 = 0xcc9e2d51, 0x1b873593
    h = seed & 0xffffffff
    n = len(data)
    nblocks = n // 4
    for i in range(nblocks):
        k = data[4 * i] | (data[4 * i + 1] << 8) | (data[4 * i + 2] << 16) | (data[4 * i + 3] << 24)
        k = (k * c1) & 0xffffffff
        k = ((k << 15) | (k >> 17)) & 0xffffffff
        k = (k * c2) & 0xffffffff
        h ^= k
        h = ((h << 13) | (h >> 19)) & 0xffffffff
        h = (h * 5 + 0xe6546b64) & 0xffffffff
    # Spark 3+ (hashUnsafeBytes2): standard MurmurHash3 tail handling.
    k = 0
    shift = 0
    for i in range(nblocks * 4, n):
        k ^= (data[i] & 0xff) << shift
        shift += 8
    if n % 4:
        k = (k * c1) & 0xffffffff
        k = ((k << 15) | (k >> 17)) & 0xffffffff
        k = (k * c2) & 0xffffffff
        h ^= k
    h ^= n
    h ^= h >> 16
    h = (h * 0x85ebca6b) & 0xffffffff
    h ^= h >> 13
    h = (h * 0xc2b2ae35) & 0xffffffff
    h ^= h >> 16
    return h - 0x100000000 if h >= 0x80000000 else h

class Tokenizer(Transformer):
    def __init__(self, inputCol=None, outputCol=None, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol)
    def transform(self, dataset):
        _check_cols(dataset, self.inputCol)
        rows = [{**r, self.outputCol: None if r.get(self.inputCol) is None else str(r[self.inputCol]).lower().split()} for r in dataset._rows]
        return _with(dataset, rows, [self.outputCol])

class RegexTokenizer(Transformer):
    _numeric = ('minTokenLength',)
    def __init__(self, inputCol=None, outputCol=None, pattern='\\s+', gaps=True, toLowercase=True, minTokenLength=1, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, pattern=pattern, gaps=gaps, toLowercase=toLowercase, minTokenLength=minTokenLength)
    def transform(self, dataset):
        rows = []
        for r in dataset._rows:
            s = r.get(self.inputCol)
            if s is None:
                rows.append({**r, self.outputCol: None})
                continue
            s = str(s).lower() if self.toLowercase else str(s)
            toks = _re.split(self.pattern, s) if self.gaps else _re.findall(self.pattern, s)
            rows.append({**r, self.outputCol: [t for t in toks if len(t) >= int(self.minTokenLength)]})
        return _with(dataset, rows, [self.outputCol])

class StopWordsRemover(Transformer):
    def __init__(self, inputCol=None, outputCol=None, stopWords=None, caseSensitive=False, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, stopWords=list(stopWords) if stopWords else sorted(_ENGLISH_STOP), caseSensitive=caseSensitive)
    @staticmethod
    def loadDefaultStopWords(language='english'):
        return sorted(_ENGLISH_STOP)
    def transform(self, dataset):
        sw = set(self.stopWords) if self.caseSensitive else set(w.lower() for w in self.stopWords)
        rows = []
        for r in dataset._rows:
            toks = r.get(self.inputCol)
            rows.append({**r, self.outputCol: None if toks is None else [t for t in toks if (t if self.caseSensitive else str(t).lower()) not in sw]})
        return _with(dataset, rows, [self.outputCol])

class HashingTF(Transformer):
    _numeric = ('numFeatures',)
    def __init__(self, inputCol=None, outputCol=None, numFeatures=262144, binary=False, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, numFeatures=numFeatures, binary=binary)
    def indexOf(self, term):
        h = _murmur3_32(str(term).encode('utf-8'), 42)
        n = int(self.numFeatures)
        return ((h % n) + n) % n
    def transform(self, dataset):
        rows = []
        for r in dataset._rows:
            toks = r.get(self.inputCol) or []
            counts = {}
            for t in toks:
                i = self.indexOf(t)
                counts[i] = 1.0 if self.binary else counts.get(i, 0.0) + 1.0
            rows.append({**r, self.outputCol: SparseVector(int(self.numFeatures), counts)})
        return _with(dataset, rows, [self.outputCol])

class CountVectorizerModel(Transformer):
    def __init__(self, vocabulary, est):
        self.vocabulary = vocabulary
        self.inputCol, self.outputCol, self.binary, self.minTF = est.inputCol, est.outputCol, est.binary, est.minTF
    def transform(self, dataset):
        index = {w: i for i, w in enumerate(self.vocabulary)}
        rows = []
        for r in dataset._rows:
            counts = {}
            for t in r.get(self.inputCol) or []:
                if t in index:
                    counts[index[t]] = counts.get(index[t], 0.0) + 1.0
            total = sum(counts.values())
            thr = self.minTF if self.minTF >= 1 else self.minTF * total
            counts = {i: (1.0 if self.binary else c) for i, c in counts.items() if c >= thr}
            rows.append({**r, self.outputCol: SparseVector(len(self.vocabulary), counts)})
        return _with(dataset, rows, [self.outputCol])

class CountVectorizer(Estimator):
    _numeric = ('vocabSize', 'minDF', 'minTF', 'maxDF')
    def __init__(self, inputCol=None, outputCol=None, vocabSize=262144, minDF=1.0, minTF=1.0, maxDF=2 ** 63 - 1, binary=False, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, vocabSize=vocabSize, minDF=minDF, minTF=minTF, maxDF=maxDF, binary=binary)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        docs = [r.get(est.inputCol) or [] for r in dataset._rows]
        n = len(docs)
        df, tf = {}, {}
        for d in docs:
            for t in set(d):
                df[t] = df.get(t, 0) + 1
            for t in d:
                tf[t] = tf.get(t, 0) + 1
        mindf = est.minDF if est.minDF >= 1 else est.minDF * n
        maxdf = est.maxDF if est.maxDF >= 1 else est.maxDF * n
        words = [w for w in tf if mindf <= df[w] <= maxdf]
        words.sort(key=lambda w: (-tf[w], w))
        return CountVectorizerModel(words[:int(est.vocabSize)], est)

class IDFModel(Transformer):
    def __init__(self, idf, est, docFreq, numDocs):
        self.idf = DenseVector(idf)
        self.docFreq = docFreq
        self.numDocs = numDocs
        self.inputCol, self.outputCol = est.inputCol, est.outputCol
    def transform(self, dataset):
        rows = []
        for r in dataset._rows:
            v = r.get(self.inputCol)
            if isinstance(v, SparseVector):
                out = SparseVector(v.size, {i: x * self.idf[i] for i, x in zip(v.indices, v.values)})
            else:
                out = DenseVector([x * self.idf[i] for i, x in enumerate(_dense(v))])
            rows.append({**r, self.outputCol: out})
        return _with(dataset, rows, [self.outputCol])

class IDF(Estimator):
    _numeric = ('minDocFreq',)
    def __init__(self, inputCol=None, outputCol=None, minDocFreq=0, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, minDocFreq=minDocFreq)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        vecs = [r.get(est.inputCol) for r in dataset._rows if r.get(est.inputCol) is not None]
        m = len(vecs)
        size = _width(dataset._rows, est.inputCol)
        df = [0] * size
        for v in vecs:
            for i, x in _items(v):
                if x > 0:
                    df[i] += 1
        idf = [(_m.log((m + 1.0) / (d + 1.0)) if d >= int(est.minDocFreq) else 0.0) for d in df]
        return IDFModel(idf, est, df, m)

class _VecScalerModel(Transformer):
    def __init__(self, fn, est, **attrs):
        self._fn = fn
        self.inputCol, self.outputCol = est.inputCol, est.outputCol
        for k, v in attrs.items():
            setattr(self, k, v)
    def transform(self, dataset):
        rows = [{**r, self.outputCol: DenseVector(self._fn(_dense(r[self.inputCol])))} for r in dataset._rows]
        return _with(dataset, rows, [self.outputCol])

class MinMaxScaler(Estimator):
    _numeric = ('min', 'max')
    def __init__(self, inputCol=None, outputCol=None, min=0.0, max=1.0, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, min=min, max=max)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        vs = [_dense(r[est.inputCol]) for r in dataset._rows]
        lo = [_bi.min(c) for c in zip(*vs)]
        hi = [_bi.max(c) for c in zip(*vs)]
        a, b = float(est.min), float(est.max)
        fn = lambda x: [((v - l) / (h - l) * (b - a) + a) if h != l else (a + b) / 2.0 for v, l, h in zip(x, lo, hi)]
        return _VecScalerModel(fn, est, originalMin=DenseVector(lo), originalMax=DenseVector(hi))

class MaxAbsScaler(Estimator):
    def __init__(self, inputCol=None, outputCol=None, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol)
    def fit(self, dataset, params=None):
        vs = [_dense(r[self.inputCol]) for r in dataset._rows]
        mx = [_bi.max(abs(v) for v in c) for c in zip(*vs)]
        return _VecScalerModel(lambda x: [v / m if m else 0.0 for v, m in zip(x, mx)], self, maxAbs=DenseVector(mx))

class StandardScaler(Estimator):
    def __init__(self, inputCol=None, outputCol=None, withMean=False, withStd=True, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, withMean=withMean, withStd=withStd)
    def fit(self, dataset, params=None):
        est = self.copy(params) if params else self
        vs = [_dense(r[est.inputCol]) for r in dataset._rows]
        n = len(vs)
        mean = [sum(c) / n for c in zip(*vs)]
        std = [_m.sqrt(sum((v - mu) ** 2 for v in c) / (n - 1)) if n > 1 else 0.0 for c, mu in zip(zip(*vs), mean)]
        def fn(x):
            out = []
            for v, mu, sd in zip(x, mean, std):
                v = v - mu if est.withMean else v
                out.append(v / sd if est.withStd and sd else (0.0 if est.withStd else v))
            return out
        return _VecScalerModel(fn, est, mean=DenseVector(mean), std=DenseVector(std))

class Bucketizer(Transformer):
    def __init__(self, splits=None, inputCol=None, outputCol=None, handleInvalid='error', **kwargs):
        self._init_params(splits=list(splits or []), inputCol=inputCol, outputCol=outputCol, handleInvalid=handleInvalid)
    def transform(self, dataset):
        rows = []
        for r in dataset._rows:
            v = r.get(self.inputCol)
            b = None
            if v is not None:
                v = float(v)
                for i in range(len(self.splits) - 1):
                    if self.splits[i] <= v < self.splits[i + 1] or (i == len(self.splits) - 2 and v == self.splits[-1]):
                        b = float(i)
                        break
                if b is None and self.handleInvalid == 'error':
                    raise Exception('Feature value %s out of Bucketizer bounds [%s, %s].' % (v, self.splits[0], self.splits[-1]))
                if b is None and self.handleInvalid == 'keep':
                    b = float(len(self.splits) - 1)
            rows.append({**r, self.outputCol: b})
        return _with(dataset, rows, [self.outputCol])

class Binarizer(Transformer):
    _numeric = ('threshold',)
    def __init__(self, threshold=0.0, inputCol=None, outputCol=None, **kwargs):
        self._init_params(threshold=threshold, inputCol=inputCol, outputCol=outputCol)
    def transform(self, dataset):
        t = float(self.threshold)
        rows = []
        for r in dataset._rows:
            v = r.get(self.inputCol)
            out = (1.0 if float(v) > t else 0.0) if isinstance(v, (int, float)) else DenseVector([1.0 if x > t else 0.0 for x in _dense(v)])
            rows.append({**r, self.outputCol: out})
        return _with(dataset, rows, [self.outputCol])

class IndexToString(Transformer):
    def __init__(self, inputCol=None, outputCol=None, labels=None, **kwargs):
        self._init_params(inputCol=inputCol, outputCol=outputCol, labels=labels)
    def transform(self, dataset):
        rows = [{**r, self.outputCol: self.labels[int(r[self.inputCol])] if r.get(self.inputCol) is not None else None} for r in dataset._rows]
        return _with(dataset, rows, [self.outputCol])

class PolynomialExpansion(Transformer):
    _numeric = ('degree',)
    def __init__(self, degree=2, inputCol=None, outputCol=None, **kwargs):
        self._init_params(degree=degree, inputCol=inputCol, outputCol=outputCol)
    def transform(self, dataset):
        import itertools as _it
        rows = []
        for r in dataset._rows:
            x = _dense(r[self.inputCol])
            out = []
            for d in range(1, int(self.degree) + 1):
                for combo in _it.combinations_with_replacement(range(len(x)), d):
                    v = 1.0
                    for i in combo:
                        v *= x[i]
                    out.append(v)
            rows.append({**r, self.outputCol: DenseVector(out)})
        return _with(dataset, rows, [self.outputCol])

# VectorAssembler: produce DenseVector (prints [1.0] in show(), DenseVector([1.0]) in the REPL).
_base_va_transform = VectorAssembler.transform
def _va_transform(self, dataset):
    _check_cols(dataset, *self.inputCols)
    rows = []
    for r in dataset._rows:
        vals = []
        skip = False
        for name in self.inputCols:
            v = r.get(name)
            if v is None:
                if self.handleInvalid == 'skip':
                    skip = True
                    break
                if self.handleInvalid == 'keep':
                    vals.append(float('nan'))
                    continue
                raise Exception('Encountered null while assembling a row with handleInvalid = "error". Consider removing nulls from dataset or using handleInvalid = "keep" or "skip".')
            if isinstance(v, bool):
                vals.append(1.0 if v else 0.0)
            elif isinstance(v, (int, float)):
                vals.append(float(v))
            elif isinstance(v, (DenseVector, SparseVector, list, tuple)):
                vals.extend(_dense(v))
            else:
                raise IllegalArgumentException('Data type string of column %s is not supported.' % name)
        if not skip:
            rows.append({**r, self.outputCol: DenseVector(vals)})
    return _with(dataset, rows, [self.outputCol])
VectorAssembler.transform = _va_transform

# StringIndexer: keep base logic, add handleInvalid and labels attribute.
_base_si_fit = StringIndexer.fit
def _si_fit(self, dataset):
    _check_cols(dataset, self.inputCol)
    model = _base_si_fit(self, dataset)
    model.labelsArray = [list(model.labels)]
    return model
StringIndexer.fit = _si_fit

# ------------------------------------------------------------------ tuning
class ParamGridBuilder(object):
    def __init__(self):
        self._grid = []
    def addGrid(self, param, values):
        name = getattr(param, 'name', None)
        if name is None:
            raise TypeError('addGrid expects a parameter like lr.regParam')
        self._grid.append((name, list(values)))
        return self
    def baseOn(self, *args):
        return self
    def build(self):
        maps = [{}]
        for name, values in self._grid:
            maps = [dict(m, **{name: v}) for m in maps for v in values]
        return maps

def _split_folds(rows, k, seed):
    rng = _rnd.Random(seed if seed is not None else 42)
    idx = list(range(len(rows)))
    rng.shuffle(idx)
    return [[rows[i] for i in idx[f::k]] for f in range(k)]

class CrossValidatorModel(Transformer):
    def __init__(self, bestModel, avgMetrics, stdMetrics, maps):
        self.bestModel = bestModel
        self.avgMetrics = avgMetrics
        self.stdMetrics = stdMetrics
        self._maps = maps
    def getEstimatorParamMaps(self):
        return self._maps
    def transform(self, dataset):
        return self.bestModel.transform(dataset)

class CrossValidator(Estimator):
    _numeric = ('numFolds', 'seed', 'parallelism')
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3, seed=None, parallelism=1, collectSubModels=False, **kwargs):
        self._init_params(estimator=estimator, estimatorParamMaps=estimatorParamMaps or [{}], evaluator=evaluator, numFolds=numFolds, seed=seed)
    def _fit_with(self, maps, train):
        est = self.estimator.copy(maps)
        return est.fit(train)
    def fit(self, dataset, params=None):
        k = int(self.numFolds)
        if k < 2:
            raise IllegalArgumentException('numFolds must be >= 2')
        folds = _split_folds(dataset._rows, k, self.seed)
        avg, std = [], []
        for pm in self.estimatorParamMaps:
            scores = []
            for f in range(k):
                test = DataFrame(folds[f], dataset.columns, dataset._spark)
                train = DataFrame([r for g in range(k) if g != f for r in folds[g]], dataset.columns, dataset._spark)
                scores.append(self.evaluator.evaluate(self._fit_with(pm, train).transform(test)))
            mean = sum(scores) / len(scores)
            avg.append(mean)
            std.append(_m.sqrt(sum((s - mean) ** 2 for s in scores) / len(scores)))
        pick = max if self.evaluator.isLargerBetter() else min
        best = avg.index(pick(avg))
        return CrossValidatorModel(self._fit_with(self.estimatorParamMaps[best], dataset), avg, std, self.estimatorParamMaps)

class TrainValidationSplitModel(Transformer):
    def __init__(self, bestModel, validationMetrics):
        self.bestModel = bestModel
        self.validationMetrics = validationMetrics
    def transform(self, dataset):
        return self.bestModel.transform(dataset)

class TrainValidationSplit(Estimator):
    _numeric = ('trainRatio', 'seed')
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, trainRatio=0.75, seed=None, parallelism=1, **kwargs):
        self._init_params(estimator=estimator, estimatorParamMaps=estimatorParamMaps or [{}], evaluator=evaluator, trainRatio=trainRatio, seed=seed)
    def fit(self, dataset, params=None):
        train, val = dataset.randomSplit([float(self.trainRatio), 1 - float(self.trainRatio)], self.seed)
        metrics = [self.evaluator.evaluate(self.estimator.copy(pm).fit(train).transform(val)) for pm in self.estimatorParamMaps]
        pick = max if self.evaluator.isLargerBetter() else min
        best = metrics.index(pick(metrics))
        return TrainValidationSplitModel(self.estimator.copy(self.estimatorParamMaps[best]).fit(dataset), metrics)

# Pipeline.fit passes stage results through; make sure our Estimators are recognised.
def _pipeline_fit(self, dataset, params=None):
    result = dataset
    fitted = []
    for stage in self.stages:
        model = stage.fit(result) if isinstance(stage, Estimator) else stage
        fitted.append(model)
        result = model.transform(result)
    return PipelineModel(fitted)
Pipeline.fit = _pipeline_fit
PipelineModel.save = _Params.save
PipelineModel.write = _Params.write
Pipeline.save = _Params.save
Pipeline.write = _Params.write

# ------------------------------------------------- DataFrame: show / schema
def _infer_type(values):
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            return 'boolean'
        if isinstance(v, int):
            return 'long'
        if isinstance(v, float):
            return 'double'
        if isinstance(v, (DenseVector, SparseVector)):
            return 'vector'
        if isinstance(v, Row):
            return 'struct'
        if isinstance(v, dict):
            return 'map<string,%s>' % _infer_type(list(v.values()))
        if isinstance(v, (list, tuple)):
            inner = _infer_type(list(v))
            return 'array<%s>' % inner
        import datetime as _dt
        if isinstance(v, _dt.datetime):
            return 'timestamp'
        if isinstance(v, _dt.date):
            return 'date'
        return 'string'
    return 'string'

def _col_type(df, c):
    t = (getattr(df, '_types', None) or {}).get(c)
    return t or _infer_type([r.get(c) for r in df._rows])

def _df_show(self, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool):
        width = 20 if truncate else 0
    else:
        width = int(truncate)
    rows = self._rows[:n]
    cols = list(self.columns)
    def cell(v):
        s = _spark_str(v)
        if width > 0 and len(s) > width:
            s = s[:width - 3] + '...' if width >= 4 else s[:width]
        return s
    data = [[cell(r.get(c)) for c in cols] for r in rows]
    if vertical:
        if not data:
            print('(0 rows)')
            return
        kw = max(len(c) for c in cols) if cols else 0
        for i, row in enumerate(data):
            head = '-RECORD %d' % i
            print(head + '-' * max(0, kw + 20 - len(head)))
            for c, v in zip(cols, row):
                print(' %s | %s ' % (c.ljust(kw), v))
        if len(self._rows) > n:
            print('only showing top %d row%s' % (n, '' if n == 1 else 's'))
        return
    widths = [max(3, len(c), *(len(row[i]) for row in data)) if data else max(3, len(c)) for i, c in enumerate(cols)]
    sep = '+' + '+'.join('-' * w for w in widths) + '+'
    def fmt(vals):
        return '|' + '|'.join((v.rjust(w) if width > 0 else v.ljust(w)) for v, w in zip(vals, widths)) + '|'
    print(sep)
    print(fmt(cols))
    print(sep)
    for row in data:
        print(fmt(row))
    print(sep)
    if len(self._rows) > n:
        print('only showing top %d row%s' % (n, '' if n == 1 else 's'))
    print()

def _df_print_schema(self, level=None):
    print('root')
    for c in self.columns:
        print(' |-- %s: %s (nullable = true)' % (c, _col_type(self, c)))
    print()

def _df_dtypes(self):
    return [(c, {'long': 'bigint', 'integer': 'int'}.get(_col_type(self, c), _col_type(self, c))) for c in self.columns]

class _Field(object):
    def __init__(self, name, t):
        self.name = name
        self.dataType = t
        self.nullable = True
    def __repr__(self):
        return "StructField('%s', %s, True)" % (self.name, self.dataType)

class _TypeName(object):
    _map = {'long': 'LongType()', 'integer': 'IntegerType()', 'double': 'DoubleType()', 'string': 'StringType()',
            'boolean': 'BooleanType()', 'vector': 'VectorUDT()', 'timestamp': 'TimestampType()', 'date': 'DateType()'}
    def __init__(self, t):
        self.t = t
    def simpleString(self):
        return self.t
    def typeName(self):
        return self.t
    def __repr__(self):
        return self._map.get(self.t, self.t)

class _Schema(list):
    def __repr__(self):
        return 'StructType([' + ', '.join(repr(f) for f in self) + '])'
    @property
    def fields(self):
        return list(self)
    @property
    def names(self):
        return [f.name for f in self]
    def fieldNames(self):
        return self.names
    def simpleString(self):
        return 'struct<' + ','.join('%s:%s' % (f.name, f.dataType.t) for f in self) + '>'

def _df_schema(self):
    return _Schema(_Field(c, _TypeName(_col_type(self, c))) for c in self.columns)

DataFrame.show = _df_show
DataFrame.printSchema = _df_print_schema
DataFrame.dtypes = property(_df_dtypes)
DataFrame.schema = property(_df_schema)

def _df_repr(self):
    return 'DataFrame[' + ', '.join('%s: %s' % (c, {'long': 'bigint', 'integer': 'int'}.get(_col_type(self, c), _col_type(self, c))) for c in self.columns) + ']'
DataFrame.__repr__ = _df_repr

def _df_to_pandas(self):
    raise ImportError("toPandas() needs pandas, which is not bundled in this offline simulator. Use df.show() or df.collect() instead.")
DataFrame.toPandas = _df_to_pandas

def _df_summary(self, *stats):
    return self.describe()
if not hasattr(DataFrame, 'summary'):
    DataFrame.summary = _df_summary

# Row repr like Spark: Row(name='Alice', age=1)
def _row_repr(self):
    return 'Row(' + ', '.join('%s=%r' % (k, v) for k, v in self.items()) + ')'
try:
    Row.__repr__ = _row_repr
except Exception:
    pass

# ------------------------------------------------ path resolution & HDFS
def _js():
    try:
        import js
        return js
    except Exception:
        return None

def _hdfs_read(path):
    j = _js()
    if j is None or not hasattr(j, '__hadoop_hdfs_read'):
        return None
    v = j.__hadoop_hdfs_read(path)
    return None if v is None else str(v)

def _resolve_read(path):
    """Return (content, description) or raise like Spark when the path is missing."""
    p = str(path)
    if p.startswith('file://'):
        lp = p[7:]
        if _os.path.exists(lp):
            return _read_local(lp)
        raise AnalysisException('[PATH_NOT_FOUND] Path does not exist: %s.' % p)
    if p.startswith('hdfs://'):
        c = _hdfs_read(p)
        if c is None:
            raise AnalysisException('[PATH_NOT_FOUND] Path does not exist: %s.' % p)
        return c
    local = p if p.startswith('/') else _os.path.join('/home/Hacker', p)
    if _os.path.exists(local):
        return _read_local(local)
    c = _hdfs_read(p if p.startswith('/') else '/' + p)
    if c is not None:
        return c
    raise AnalysisException('[PATH_NOT_FOUND] Path does not exist: file:%s. (checked the local folder and HDFS)' % local)

def _read_local(p):
    if _os.path.isdir(p):
        parts = []
        for name in sorted(_os.listdir(p)):
            if name.startswith('_') or name.startswith('.'):
                continue
            fp = _os.path.join(p, name)
            if _os.path.isfile(fp):
                with open(fp, 'r') as f:
                    parts.append(f.read())
        return ''.join(parts)
    with open(p, 'r') as f:
        return f.read()

def _write_target(path):
    """'hdfs' or 'local' for an output path."""
    p = str(path)
    if p.startswith('hdfs://'):
        return 'hdfs', p
    if p.startswith('file://'):
        return 'local', p[7:]
    return 'local', (p if p.startswith('/') else _os.path.join('/home/Hacker', p))

def _write_output(path, files):
    kind, target = _write_target(path)
    j = _js()
    if kind == 'hdfs':
        if j.__hadoop_hdfs_exists(target):
            raise Exception('org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory %s already exists' % path)
        j.__hadoop_hdfs_mkdir(target)
        for name, content in files:
            j.__hadoop_hdfs_write(target.rstrip('/') + '/' + name, content)
        j.__hadoop_hdfs_write(target.rstrip('/') + '/_SUCCESS', '')
        return
    if _os.path.exists(target):
        raise Exception('org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:%s already exists' % target)
    _os.makedirs(target, exist_ok=True)
    for name, content in files:
        with open(_os.path.join(target, name), 'w') as f:
            f.write(content)
    with open(_os.path.join(target, '_SUCCESS'), 'w') as f:
        f.write('')

# DataFrameReader: real Spark defaults (header=False, inferSchema=False) and HDFS paths.
_TRUE = ('true', '1', 'yes')
def _opt(reader, kwargs, key, default):
    v = kwargs.get(key, reader._options.get(key.lower(), default))
    if isinstance(v, str):
        return v.lower() in _TRUE if v.lower() in ('true', 'false', '1', '0', 'yes', 'no') else v
    return v

def _parse_csv_line(line, sep, quote='"'):
    out, cur, inq, i = [], '', False, 0
    while i < len(line):
        ch = line[i]
        if inq:
            if ch == quote and i + 1 < len(line) and line[i + 1] == quote:
                cur += quote
                i += 1
            elif ch == quote:
                inq = False
            else:
                cur += ch
        elif ch == quote:
            inq = True
        elif line.startswith(sep, i):
            out.append(cur)
            cur = ''
            i += len(sep) - 1
        else:
            cur += ch
        i += 1
    out.append(cur)
    return out

def _cast_csv(v):
    if v is None or v == '':
        return None, None
    s = v.strip()
    if _re.fullmatch(r'[+-]?\d+', s):
        n = int(s)
        return n, ('integer' if -2 ** 31 <= n < 2 ** 31 else 'long')
    if _re.fullmatch(r'[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?', s):
        return float(s), 'double'
    if s.lower() in ('true', 'false'):
        return s.lower() == 'true', 'boolean'
    return v, 'string'

def _reader_csv(self, path, schema=None, sep=None, header=None, inferSchema=None, **kwargs):
    if header is not None:
        kwargs['header'] = header
    if inferSchema is not None:
        kwargs['inferSchema'] = inferSchema
    if sep is not None:
        kwargs['sep'] = sep
    paths = path if isinstance(path, (list, tuple)) else [path]
    content = ''.join(_resolve_read(p) for p in paths)
    hdr = _opt(self, kwargs, 'header', False)
    infer = _opt(self, kwargs, 'inferSchema', False)
    delim = kwargs.get('sep', kwargs.get('delimiter', self._options.get('sep', self._options.get('delimiter', ','))))
    lines = [l for l in content.splitlines() if l.strip() != '']
    if not lines:
        return DataFrame([], [], self.spark)
    first = _parse_csv_line(lines[0], delim)
    if hdr:
        cols = [c.strip() for c in first]
        body = lines[1:]
    else:
        cols = ['_c%d' % i for i in range(len(first))]
        body = lines
    raw = [_parse_csv_line(l, delim) for l in body]
    types = {}
    if schema is not None:
        names, stypes = _schema_names_types(schema)
        cols = names or cols
        types = dict(zip(cols, stypes))
    elif infer:
        for i, c in enumerate(cols):
            ts = set()
            for r in raw:
                _, t = _cast_csv(r[i] if i < len(r) else None)
                if t:
                    ts.add(t)
            if not ts:
                types[c] = 'string'
            elif ts <= {'integer'}:
                types[c] = 'integer'
            elif ts <= {'integer', 'long'}:
                types[c] = 'long'
            elif ts <= {'integer', 'long', 'double'}:
                types[c] = 'double'
            elif ts == {'boolean'}:
                types[c] = 'boolean'
            else:
                types[c] = 'string'
    else:
        types = {c: 'string' for c in cols}
    def conv(v, t):
        if v is None or v == '':
            return None
        try:
            if t in ('integer', 'long', 'int', 'bigint'):
                return int(v.strip())
            if t in ('double', 'float'):
                return float(v)
            if t == 'boolean':
                return v.strip().lower() == 'true'
        except ValueError:
            return None
        return v
    rows = []
    for r in raw:
        rows.append({c: conv(r[i] if i < len(r) else None, types.get(c, 'string')) for i, c in enumerate(cols)})
    df = DataFrame(rows, cols, self.spark)
    df._types = types
    return df

def _schema_names_types(schema):
    if isinstance(schema, str):
        names, types = [], []
        for part in schema.split(','):
            bits = part.strip().split()
            if len(bits) >= 2:
                names.append(bits[0])
                types.append(bits[1].lower().replace('int', 'integer') if bits[1].lower() == 'int' else bits[1].lower())
        return names, types
    names, types = [], []
    for f in getattr(schema, 'fields', []) or []:
        names.append(f.name)
        t = type(f.dataType).__name__.replace('Type', '').lower()
        types.append({'integer': 'integer', 'long': 'long', 'double': 'double', 'float': 'double', 'string': 'string', 'boolean': 'boolean'}.get(t, 'string'))
    return names, types

def _reader_text(self, path, wholetext=False, **kwargs):
    paths = path if isinstance(path, (list, tuple)) else [path]
    content = ''.join(_resolve_read(p) for p in paths)
    if wholetext:
        df = DataFrame([{'value': content}], ['value'], self.spark)
    else:
        df = DataFrame([{'value': l} for l in content.splitlines()], ['value'], self.spark)
    df._types = {'value': 'string'}
    return df

def _reader_json(self, path, **kwargs):
    content = _resolve_read(path)
    content = content.strip()
    try:
        data = _json.loads(content)
        if isinstance(data, dict):
            data = [data]
    except ValueError:
        data = [_json.loads(l) for l in content.splitlines() if l.strip()]
    cols = []
    for d in data:
        for k in d.keys():
            if k not in cols:
                cols.append(k)
    cols.sort()
    return DataFrame([{c: d.get(c) for c in cols} for d in data], cols, self.spark)

def _reader_load(self, path=None, format=None, schema=None, **kwargs):
    source = (format or self._format or 'parquet').lower()
    self._options.update({k.lower(): v for k, v in kwargs.items()})
    if source in ('csv', 'com.databricks.spark.csv'):
        return _reader_csv(self, path, schema=schema)
    if source == 'json':
        return _reader_json(self, path)
    if source in ('text', 'txt'):
        return _reader_text(self, path)
    if source == 'libsvm':
        return _reader_libsvm(self, path)
    raise AnalysisException('%s files are not supported by this in-browser Spark. Use csv, json, text or libsvm.' % source)

def _reader_libsvm(self, path, numFeatures=None, **kwargs):
    content = _resolve_read(path)
    rows = []
    maxi = 0
    parsed = []
    for line in content.splitlines():
        line = line.split('#')[0].strip()
        if not line:
            continue
        bits = line.split()
        lab = float(bits[0])
        feats = {}
        for b in bits[1:]:
            i, v = b.split(':')
            feats[int(i) - 1] = float(v)
            maxi = max(maxi, int(i))
        parsed.append((lab, feats))
    size = int(numFeatures or maxi)
    for lab, feats in parsed:
        rows.append({'label': lab, 'features': SparseVector(size, feats)})
    df = DataFrame(rows, ['label', 'features'], self.spark)
    df._types = {'label': 'double', 'features': 'vector'}
    return df

def _reader_schema(self, schema):
    self._schema = schema
    return self

DataFrameReader.csv = lambda self, path, schema=None, sep=None, header=None, inferSchema=None, **kw: _reader_csv(self, path, schema if schema is not None else getattr(self, '_schema', None), sep, header, inferSchema, **kw)
DataFrameReader.text = _reader_text
DataFrameReader.json = _reader_json
DataFrameReader.load = _reader_load
DataFrameReader.schema = _reader_schema

# DataFrameWriter: write part files to local folder or HDFS like Spark.
def _writer_save(self, path=None, format=None, mode=None, **options):
    if mode:
        self._mode = mode.lower()
    self._options.update({k.lower(): v for k, v in options.items()})
    source = (format or self._format or 'parquet').lower()
    df = self.df
    if source == 'json':
        data = '\n'.join(_json.dumps({c: r.get(c) for c in df.columns}, default=str) for r in df._rows) + '\n'
        ext = 'json'
    elif source == 'csv':
        sep = self._options.get('sep', self._options.get('delimiter', ','))
        hdr = str(self._options.get('header', 'false')).lower() in _TRUE
        lines = [sep.join(df.columns)] if hdr else []
        for r in df._rows:
            vals = []
            for c in df.columns:
                v = r.get(c)
                s = '' if v is None else _spark_str(v)
                if sep in s or '"' in s:
                    s = '"' + s.replace('"', '\\"') + '"'
                vals.append(s)
            lines.append(sep.join(vals))
        data = '\n'.join(lines) + ('\n' if lines else '')
        ext = 'csv'
    elif source in ('text', 'txt'):
        if len(df.columns) != 1:
            raise AnalysisException('Text data source supports only a single column, and you have %d columns.' % len(df.columns))
        data = '\n'.join('' if r.get(df.columns[0]) is None else str(r.get(df.columns[0])) for r in df._rows) + '\n'
        ext = 'txt'
    else:
        raise AnalysisException('%s output is not supported by this in-browser Spark. Use csv, json or text.' % source)
    kind, target = _write_target(path)
    j = _js()
    exists = j.__hadoop_hdfs_exists(target) if kind == 'hdfs' else _os.path.exists(target)
    if exists:
        if self._mode in ('error', 'errorifexists', 'default'):
            raise AnalysisException('[PATH_ALREADY_EXISTS] Path %s already exists. Set mode as "overwrite" to overwrite the existing path.' % path)
        if self._mode == 'ignore':
            return
        if kind == 'local' and self._mode == 'overwrite':
            import shutil as _sh
            _sh.rmtree(target, ignore_errors=True)
        if kind == 'hdfs' and self._mode == 'overwrite':
            raise AnalysisException('Overwriting an HDFS directory from Spark is not supported here; remove it first with hdfs dfs -rm -r %s' % target)
    name = 'part-00000-%s-c000.%s' % (uuid_module.uuid4(), ext)
    if kind == 'local' and exists and self._mode == 'append':
        _os.makedirs(target, exist_ok=True)
        with open(_os.path.join(target, name), 'w') as f:
            f.write(data)
        return
    _write_output(path, [(name, data)])

DataFrameWriter.save = _writer_save
DataFrameWriter.csv = lambda self, path, mode=None, header=None, sep=None, **kw: (self._options.update({k: v for k, v in (('header', header), ('sep', sep)) if v is not None}), _writer_save(self, path, 'csv', mode, **kw))[1]
DataFrameWriter.json = lambda self, path, mode=None, **kw: _writer_save(self, path, 'json', mode, **kw)
DataFrameWriter.text = lambda self, path, mode=None, **kw: _writer_save(self, path, 'text', mode, **kw)

# ------------------------------------------------------------------- RDD
def _sc_default_parallelism(self):
    j = _js()
    try:
        return max(1, int(j.navigator.hardwareConcurrency)) if j is not None else 4
    except Exception:
        return 4
SparkContext.defaultParallelism = property(_sc_default_parallelism)
SparkContext.defaultMinPartitions = property(lambda self: min(self.defaultParallelism, 2))

_base_parallelize = SparkContext.parallelize
def _parallelize(self, c, numSlices=None):
    return RDD(list(c), self, numSlices or self.defaultParallelism)
SparkContext.parallelize = _parallelize

def _text_file(self, name, minPartitions=None, use_unicode=True):
    content = _resolve_read(name)
    lines = content.split('\n')
    if lines and lines[-1] == '':
        lines.pop()
    return RDD(lines, self, minPartitions or self.defaultMinPartitions)
SparkContext.textFile = _text_file

def _whole_text_files(self, path, minPartitions=None, use_unicode=True):
    p = str(path)
    out = []
    local = p if p.startswith('/') else _os.path.join('/home/Hacker', p)
    if p.startswith('file://'):
        local = p[7:]
    if not p.startswith('hdfs://') and _os.path.isdir(local):
        for nm in sorted(_os.listdir(local)):
            fp = _os.path.join(local, nm)
            if _os.path.isfile(fp):
                with open(fp) as f:
                    out.append(('file:' + fp, f.read()))
    else:
        out.append((p if p.startswith('hdfs://') else 'hdfs://localhost:9000' + (p if p.startswith('/') else '/' + p), _resolve_read(p)))
    return RDD(out, self, minPartitions or self.defaultMinPartitions)
SparkContext.wholeTextFiles = _whole_text_files

def _sc_repr(self):
    return '<SparkContext master=%s appName=%s>' % (self.master, self.appName)
SparkContext.__repr__ = _sc_repr
SparkContext.version = property(lambda self: '3.5.1')
SparkContext.uiWebUrl = property(lambda self: 'http://localhost:4040')
SparkContext.setLogLevel = lambda self, level: None
SparkContext.union = lambda self, rdds: RDD([x for r in rdds for x in r._data], self)
SparkContext.accumulator = lambda self, value, accum_param=None: _Accumulator(value)

class _Accumulator(object):
    def __init__(self, v):
        self.value = v
    def add(self, x):
        self.value += x
    def __iadd__(self, x):
        self.value += x
        return self
    def __repr__(self):
        return str(self.value)

def _partitions(self):
    n = max(1, self._num_partitions)
    L = len(self._data)
    return [self._data[(i * L) // n:((i + 1) * L) // n] for i in range(n)]

def _rdd(data, like, parts=None):
    return RDD(data, like.sc, parts or like._num_partitions)

class _StatCounter(object):
    def __init__(self, vals):
        vals = [float(v) for v in vals]
        self.n = len(vals)
        self.mu = sum(vals) / self.n if self.n else float('nan')
        self.m2 = sum((v - self.mu) ** 2 for v in vals)
        self.maxValue = max(vals) if vals else float('-inf')
        self.minValue = min(vals) if vals else float('inf')
        self.total = sum(vals)
    def count(self):
        return self.n
    def mean(self):
        return self.mu
    def sum(self):
        return self.total
    def max(self):
        return self.maxValue
    def min(self):
        return self.minValue
    def variance(self):
        return self.m2 / self.n if self.n else float('nan')
    def sampleVariance(self):
        return self.m2 / (self.n - 1) if self.n > 1 else float('nan')
    def stdev(self):
        return _m.sqrt(self.variance())
    def sampleStdev(self):
        return _m.sqrt(self.sampleVariance())
    def __repr__(self):
        return '(count: %d, mean: %s, stdev: %s, max: %s, min: %s)' % (self.n, self.mu, self.stdev(), self.maxValue, self.minValue)

def _join(self, other, how):
    right = {}
    for k, v in other._data:
        right.setdefault(k, []).append(v)
    left_keys = set()
    out = []
    for k, v in self._data:
        left_keys.add(k)
        if k in right:
            for w in right[k]:
                out.append((k, (v, w)))
        elif how in ('left', 'full'):
            out.append((k, (v, None)))
    if how in ('right', 'full'):
        for k, vs in right.items():
            if k not in left_keys:
                for w in vs:
                    out.append((k, (None, w)))
    return _rdd(out, self)

_rdd_methods = {
    'glom': lambda self: _rdd([list(p) for p in _partitions(self)], self),
    'mapPartitions': lambda self, f, preservesPartitioning=False: _rdd([x for p in _partitions(self) for x in (f(iter(p)) or [])], self),
    'mapPartitionsWithIndex': lambda self, f, preservesPartitioning=False: _rdd([x for i, p in enumerate(_partitions(self)) for x in (f(i, iter(p)) or [])], self),
    'foreachPartition': lambda self, f: [f(iter(p)) for p in _partitions(self)] and None,
    'coalesce': lambda self, numPartitions, shuffle=False: _rdd(self._data, self, min(numPartitions, self._num_partitions) if not shuffle else numPartitions),
    'repartition': lambda self, numPartitions: _rdd(self._data, self, numPartitions),
    'join': lambda self, other, numPartitions=None: _join(self, other, 'inner'),
    'leftOuterJoin': lambda self, other, numPartitions=None: _join(self, other, 'left'),
    'rightOuterJoin': lambda self, other, numPartitions=None: _join(self, other, 'right'),
    'fullOuterJoin': lambda self, other, numPartitions=None: _join(self, other, 'full'),
    'cogroup': lambda self, other, numPartitions=None: _rdd([(k, (_ResultIterable([v for kk, v in self._data if kk == k]), _ResultIterable([v for kk, v in other._data if kk == k]))) for k in list(dict.fromkeys([k for k, _ in self._data] + [k for k, _ in other._data]))], self),
    'subtract': lambda self, other, numPartitions=None: _rdd([x for x in self._data if x not in set(other._data)], self),
    'subtractByKey': lambda self, other, numPartitions=None: _rdd([(k, v) for k, v in self._data if k not in set(k2 for k2, _ in other._data)], self),
    'intersection': lambda self, other: _rdd(list(dict.fromkeys([x for x in self._data if x in set(other._data)])), self),
    'countByKey': lambda self: _count_by(k for k, _ in self._data),
    'countByValue': lambda self: _count_by(self._data),
    'collectAsMap': lambda self: dict(self._data),
    'lookup': lambda self, key: [v for k, v in self._data if k == key],
    'top': lambda self, num, key=None: sorted(self._data, key=key, reverse=True)[:num],
    'takeOrdered': lambda self, num, key=None: sorted(self._data, key=key)[:num],
    'stats': lambda self: _StatCounter(self._data),
    'variance': lambda self: _StatCounter(self._data).variance(),
    'stdev': lambda self: _StatCounter(self._data).stdev(),
    'sampleVariance': lambda self: _StatCounter(self._data).sampleVariance(),
    'sampleStdev': lambda self: _StatCounter(self._data).sampleStdev(),
    'zipWithIndex': lambda self: _rdd([(x, i) for i, x in enumerate(self._data)], self),
    'zipWithUniqueId': lambda self: _rdd([(x, i) for i, x in enumerate(self._data)], self),
    'keyBy': lambda self, f: _rdd([(f(x), x) for x in self._data], self),
    'groupBy': lambda self, f, numPartitions=None: _rdd(list(_group(self._data, f).items()), self),
    'groupByKey': lambda self, numPartitions=None: _rdd([(k, _ResultIterable(v)) for k, v in _group_kv(self._data).items()], self),
    'aggregateByKey': lambda self, zeroValue, seqFunc, combFunc, numPartitions=None: _rdd([(k, _fold(vs, zeroValue, seqFunc)) for k, vs in _group_kv(self._data).items()], self),
    'foldByKey': lambda self, zeroValue, func, numPartitions=None: _rdd([(k, _fold(vs, zeroValue, func)) for k, vs in _group_kv(self._data).items()], self),
    'combineByKey': lambda self, createCombiner, mergeValue, mergeCombiners, numPartitions=None: _rdd([(k, _combine(vs, createCombiner, mergeValue)) for k, vs in _group_kv(self._data).items()], self),
    'reduceByKey': lambda self, func, numPartitions=None: _rdd([(k, _reduce(vs, func)) for k, vs in _group_kv(self._data).items()], self),
    'sortByKey': lambda self, ascending=True, numPartitions=None, keyfunc=lambda x: x: _rdd(sorted(self._data, key=lambda kv: keyfunc(kv[0]), reverse=not ascending), self),
    'sortBy': lambda self, keyfunc, ascending=True, numPartitions=None: _rdd(sorted(self._data, key=keyfunc, reverse=not ascending), self),
    'distinct': lambda self, numPartitions=None: _rdd(list(dict.fromkeys(self._data)), self),
    'max': lambda self, key=None: max(self._data, key=key) if key else max(self._data),
    'min': lambda self, key=None: min(self._data, key=key) if key else min(self._data),
    'histogram': lambda self, buckets: _histogram(self._data, buckets),
    'saveAsTextFile': lambda self, path, compressionCodecClass=None: _write_output(path, [('part-%05d' % i, ''.join(str(x) + '\n' for x in p)) for i, p in enumerate(_partitions(self))]),
    'toDF': lambda self, schema=None, sampleRatio=None: (self.sc._session if getattr(self.sc, '_session', None) else SparkSession.builder.getOrCreate()).createDataFrame(self._data, schema),
    'name': lambda self: getattr(self, '_name', None),
    'setName': lambda self, n: (setattr(self, '_name', n), self)[1],
    'id': lambda self: id(self) % 100000,
    'toDebugString': lambda self: ('(%d) PythonRDD[%d] at RDD at PythonRDD.scala:53 []\n |  ParallelCollectionRDD[%d] at readRDDFromFile at PythonRDD.scala:289 []' % (self._num_partitions, id(self) % 100, id(self) % 100 - 1)).encode(),
    'getStorageLevel': lambda self: 'StorageLevel(False, False, False, False, 1)',
    'checkpoint': lambda self: None,
    'isCheckpointed': lambda self: False,
    'context': property(lambda self: self.sc),
    'first': lambda self: _first(self),
    'reduce': lambda self, f: _reduce_nonempty(self._data, f),
    'aggregate': lambda self, zeroValue, seqOp, combOp: _aggregate(self, zeroValue, seqOp, combOp),
    'fold': lambda self, zeroValue, op: _aggregate(self, zeroValue, op, op),
    'collect': lambda self: list(self._data),
    'count': lambda self: len(self._data),
    'sum': lambda self: sum(self._data),
    'mean': lambda self: _StatCounter(self._data).mean(),
}
for _k, _v in _rdd_methods.items():
    setattr(RDD, _k, _v)

def _rdd_repr(self):
    return 'PythonRDD[%d] at RDD at PythonRDD.scala:53' % (id(self) % 1000) if getattr(self, '_name', None) is None else '%s PythonRDD[%d] at RDD at PythonRDD.scala:53' % (self._name, id(self) % 1000)
RDD.__repr__ = _rdd_repr

class _ResultIterable(list):
    def __repr__(self):
        return '<pyspark.resultiterable.ResultIterable object at 0x%x>' % (id(self) & 0xffffffffffff)
    @property
    def data(self):
        return list(self)
    @property
    def maxindex(self):
        return len(self)

def _count_by(it):
    import collections as _c
    d = _c.defaultdict(int)
    for x in it:
        d[x] += 1
    return d

def _group(data, f):
    d = {}
    for x in data:
        d.setdefault(f(x), _ResultIterable()).append(x)
    return d

def _group_kv(data):
    d = {}
    for k, v in data:
        d.setdefault(k, []).append(v)
    return d

def _fold(vs, zero, f):
    import copy as _copy
    acc = _copy.deepcopy(zero)
    for v in vs:
        acc = f(acc, v)
    return acc

def _combine(vs, create, merge):
    acc = create(vs[0])
    for v in vs[1:]:
        acc = merge(acc, v)
    return acc

def _reduce(vs, f):
    acc = vs[0]
    for v in vs[1:]:
        acc = f(acc, v)
    return acc

def _reduce_nonempty(data, f):
    if not data:
        raise ValueError('Can not reduce() empty RDD')
    return _reduce(data, f)

def _first(self):
    if not self._data:
        raise ValueError('RDD is empty')
    return self._data[0]

def _aggregate(self, zero, seqOp, combOp):
    import copy as _copy
    result = _copy.deepcopy(zero)
    for p in _partitions(self):
        acc = _copy.deepcopy(zero)
        for x in p:
            acc = seqOp(acc, x)
        result = combOp(result, acc)
    return result

def _histogram(data, buckets):
    vals = [float(v) for v in data]
    if isinstance(buckets, int):
        lo, hi = min(vals), max(vals)
        step = (hi - lo) / buckets if buckets else 0
        edges = [lo + i * step for i in range(buckets)] + [hi]
    else:
        edges = [float(b) for b in buckets]
    counts = [0] * (len(edges) - 1)
    for v in vals:
        for i in range(len(edges) - 1):
            if edges[i] <= v < edges[i + 1] or (i == len(edges) - 2 and v == edges[-1]):
                counts[i] += 1
                break
    return ([e if not float(e).is_integer() or not isinstance(buckets, list) else e for e in edges], counts)

# --------------------------------------------- module registration
def _module(name, **attrs):
    mod = _sys.modules.get(name) or _types.ModuleType(name)
    mod.__path__ = getattr(mod, '__path__', [])
    for k, v in attrs.items():
        setattr(mod, k, v)
    _sys.modules[name] = mod
    parent, _, child = name.rpartition('.')
    if parent and parent in _sys.modules:
        setattr(_sys.modules[parent], child, mod)
    return mod

_module('pyspark.ml.linalg', Vectors=Vectors, DenseVector=DenseVector, SparseVector=SparseVector, VectorUDT=object, Vector=DenseVector, Matrices=object)
_module('pyspark.mllib')
_module('pyspark.mllib.linalg', Vectors=Vectors, DenseVector=DenseVector, SparseVector=SparseVector)
_module('pyspark.ml.feature', VectorAssembler=VectorAssembler, StringIndexer=StringIndexer, OneHotEncoder=OneHotEncoder,
        Imputer=Imputer, StandardScaler=StandardScaler, Normalizer=Normalizer, MinMaxScaler=MinMaxScaler, MaxAbsScaler=MaxAbsScaler,
        Tokenizer=Tokenizer, RegexTokenizer=RegexTokenizer, StopWordsRemover=StopWordsRemover, HashingTF=HashingTF, IDF=IDF,
        CountVectorizer=CountVectorizer, Bucketizer=Bucketizer, Binarizer=Binarizer, IndexToString=IndexToString,
        PolynomialExpansion=PolynomialExpansion, IDFModel=IDFModel, CountVectorizerModel=CountVectorizerModel)
_module('pyspark.ml.regression', LinearRegression=LinearRegression, LinearRegressionModel=LinearRegressionModel,
        DecisionTreeRegressor=DecisionTreeRegressor, DecisionTreeRegressionModel=DecisionTreeRegressionModel,
        RandomForestRegressor=RandomForestRegressor, RandomForestRegressionModel=RandomForestRegressionModel)
_module('pyspark.ml.classification', LogisticRegression=LogisticRegression, LogisticRegressionModel=LogisticRegressionModel,
        DecisionTreeClassifier=DecisionTreeClassifier, DecisionTreeClassificationModel=DecisionTreeClassificationModel,
        RandomForestClassifier=RandomForestClassifier, RandomForestClassificationModel=RandomForestClassificationModel,
        NaiveBayes=NaiveBayes, NaiveBayesModel=NaiveBayesModel)
_module('pyspark.ml.clustering', KMeans=KMeans, KMeansModel=KMeansModel)
_module('pyspark.ml.evaluation', RegressionEvaluator=RegressionEvaluator, MulticlassClassificationEvaluator=MulticlassClassificationEvaluator,
        BinaryClassificationEvaluator=BinaryClassificationEvaluator, ClusteringEvaluator=ClusteringEvaluator)
_module('pyspark.ml.tuning', ParamGridBuilder=ParamGridBuilder, CrossValidator=CrossValidator, CrossValidatorModel=CrossValidatorModel,
        TrainValidationSplit=TrainValidationSplit, TrainValidationSplitModel=TrainValidationSplitModel)
_module('pyspark.ml', Pipeline=Pipeline, PipelineModel=PipelineModel, Transformer=Transformer, Estimator=Estimator)
_module('pyspark.ml.pipeline', Pipeline=Pipeline, PipelineModel=PipelineModel)
_module('pyspark.sql.utils', AnalysisException=AnalysisException, IllegalArgumentException=IllegalArgumentException)
_module('pyspark.errors', AnalysisException=AnalysisException, IllegalArgumentException=IllegalArgumentException, PySparkException=Exception)
_module('pyspark.resultiterable', ResultIterable=_ResultIterable)
_sys.modules['pyspark'].__version__ = '3.5.1'
_sys.modules['pyspark'].SparkContext = SparkContext
_sys.modules['pyspark'].RDD = RDD
_sys.modules['pyspark'].StorageLevel = type('StorageLevel', (), {'MEMORY_ONLY': 'MEMORY_ONLY', 'MEMORY_AND_DISK': 'MEMORY_AND_DISK', 'DISK_ONLY': 'DISK_ONLY'})
`;
