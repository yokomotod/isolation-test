import "./App.css";
import specs from "../../test/specs.json";
import { useState } from "react";

const POSTGRES = "postgres";
const MYSQL = "mysql";
const SQLSERVER = "sqlserver";
const ORACLE = "oracle";
const DB2 = "db2";
// const SQLITE = "sqlite";
const databases = [
  POSTGRES,
  MYSQL,
  SQLSERVER,
  ORACLE,
  DB2,
  // SQLITE,
] as const;
type Database =
  | typeof POSTGRES
  | typeof MYSQL
  | typeof SQLSERVER
  | typeof ORACLE
  | typeof DB2;

const NO_TRANSACTION = "NO TRANSACTION";
const READ_UNCOMMITTED = "READ UNCOMMITTED";
const READ_COMMITTED = "READ COMMITTED";
const READ_COMMITTED_SNAPSHOT = "READ COMMITTED SNAPSHOT";
const READ_STABILITY = "READ STABILITY";
const REPEATABLE_READ = "REPEATABLE READ";
const SNAPSHOT = "SNAPSHOT";
const SERIALIZABLE = "SERIALIZABLE";

const levels = [
  NO_TRANSACTION,
  READ_UNCOMMITTED,
  READ_COMMITTED,
  READ_COMMITTED_SNAPSHOT,
  READ_STABILITY,
  REPEATABLE_READ,
  SNAPSHOT,
  SERIALIZABLE,
] as const;
type Level = (typeof levels)[number];

const levelInt: Record<string, number> = {
  [NO_TRANSACTION]: 0,
  [READ_UNCOMMITTED]: 1,
  [READ_COMMITTED]: 2,
  [READ_COMMITTED_SNAPSHOT]: 3,
  [READ_STABILITY]: 4,
  [REPEATABLE_READ]: 5,
  [SNAPSHOT]: 6,
  [SERIALIZABLE]: 7,
};

const dbLevels = {
  [POSTGRES]: {
    [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: `${READ_UNCOMMITTED}/(${READ_COMMITTED} alias)`,
    [READ_COMMITTED]: READ_COMMITTED,
    [REPEATABLE_READ]: REPEATABLE_READ,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [SQLSERVER]: {
    [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: READ_UNCOMMITTED,
    [READ_COMMITTED]: READ_COMMITTED,
    [READ_COMMITTED_SNAPSHOT]: "REPEATABLE_READ (SNAPSHOT)",
    [REPEATABLE_READ]: REPEATABLE_READ,
    [SNAPSHOT]: SNAPSHOT,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [ORACLE]: {
    [NO_TRANSACTION]: "no transaction",
    [READ_COMMITTED]: READ_COMMITTED,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [DB2]: {
    [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: `UR(Uncommitted read)/${READ_UNCOMMITTED}/DIRTY READ`,
    [READ_COMMITTED]: `CS/CURSOR STABILITY/${READ_COMMITTED}`,
    [READ_STABILITY]: `RS(Read stability)`,
    [REPEATABLE_READ]: `RR/${REPEATABLE_READ}`,
    [SERIALIZABLE]: `${SERIALIZABLE}/(RR alias)`,
  },
  "*": {
    [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: READ_UNCOMMITTED,
    [READ_COMMITTED]: READ_COMMITTED,
    [REPEATABLE_READ]: REPEATABLE_READ,
    [SERIALIZABLE]: SERIALIZABLE,
  },
};

const defaultLevel = {
  [POSTGRES]: READ_COMMITTED,
  [MYSQL]: REPEATABLE_READ,
  [SQLSERVER]: READ_COMMITTED,
  [ORACLE]: READ_COMMITTED,
  [DB2]: READ_COMMITTED,
};

const dbNames: Record<string, string> = {
  [POSTGRES]: "PostgreSQL",
  [MYSQL]: "MySQL/InnoDB",
  [SQLSERVER]: "MS SQL Server",
  [ORACLE]: "Oracle Database",
  [DB2]: "IBM Db2",
};

// TODO: "Read Committed" ほしい
// TODO: "Cursor Stability" ほしい
const models: Record<string, Record<string, string>> = {
  [POSTGRES]: {
    [READ_UNCOMMITTED]: "Monotonic Atomic View",
    [READ_COMMITTED]: "Monotonic Atomic View",
    [REPEATABLE_READ]: "Snapshot Isolation",
    [SERIALIZABLE]: "Serializable Snapshot Isolation",
  },
  [MYSQL]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Monotonic Atomic View",
    [REPEATABLE_READ]: "Monotonic Atomic View",
    [SERIALIZABLE]: "Serializable",
  },
  [SQLSERVER]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Monotonic Atomic View",
    [READ_COMMITTED_SNAPSHOT]: "Monotonic Atomic View",
    [REPEATABLE_READ]: "Repeatable Read",
    [SNAPSHOT]: "Snapshot Isolation",
    [SERIALIZABLE]: "Serializable",
  },
  [ORACLE]: {
    [READ_COMMITTED]: "Monotonic Atomic View",
    [SERIALIZABLE]: "Snapshot Isolation",
  },
  [DB2]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Monotonic Atomic View",
    [REPEATABLE_READ]: "Monotonic Atomic View",
    [SERIALIZABLE]: "Serializable",
  },
};

const orderedSpecs: typeof specs = [];
orderedSpecs.push(specs.find(({ name }) => name === "dirty write")!);
orderedSpecs.push(specs.find(({ name }) => name === "dirty read")!);
orderedSpecs.push(specs.find(({ name }) => name === "fuzzy read")!);
orderedSpecs.push(specs.find(({ name }) => name === "phantom read")!);
orderedSpecs.push(specs.find(({ name }) => name === "lost update")!);
orderedSpecs.push(specs.find(({ name }) => name === "write skew")!);
orderedSpecs.push(
  specs.find(({ name }) => name === "fuzzy read with locking read")!
);
orderedSpecs.push(
  specs.find(({ name }) => name === "phantom read with locking read")!
);

type Tx = {
  query: string;
  want: { Int64: number }[] | null;
  wantOk: { Int64: number }[] | null;
  wantNg: { Int64: number }[] | null;
  wantErr: Record<string, string | undefined> | null;
};
type Spec = {
  name: string;
  txs: Tx[][];
  threshold: Record<string, string | undefined> & { "*": string };
  wantStarts: Record<string, string[] | undefined> & { "*": string[] };
  wantEnds: Record<string, string[] | undefined> & { "*": string[] };
  skip: Record<string, boolean | undefined> | null;
};

function convertTxs(
  txs: Tx[][],
  wantStarts: string[],
  wantEnds: string[]
): (Partial<Tx & { rowspan: number }> | null)[][] {
  // const m = spec.txs.length;
  // const n = Math.max(...txs.map((queries) => queries.length));
  // const rows = [];
  // for (let i = 0; i < n; i++) {
  //   for (let j = 0; j < m; j++) {
  //     if (!txs[j][i]?.query) {
  //       continue;
  //     }

  //     const cols = [];
  //     cols.push(...new Array(j));
  //     cols.push(txs[j][i]);
  //     cols.push(...new Array(m - 1 - j));
  //     rows.push(cols);
  //   }
  // }

  // return rows;

  const txIndex: Record<string, number> = { a: 0, b: 1 };
  // const nTx = Object.keys(txIndex).length;

  const n = Math.max(wantStarts.length, wantEnds.length);
  const rows = [];
  const isWaiting: Record<string, boolean> = { a: false, b: false };
  for (let i = 0; i < n; i++) {
    console.log("------------");
    console.log(`end: ${wantEnds[i]}, ${isWaiting}`);
    const [tx, startIdxStr] = wantStarts[i].split(":");
    const [endTx] = wantEnds[i].split(":");

    const txId = txIndex[tx];
    const startIndex = Number(startIdxStr);
    // const endIndex = Number(endIdxStr);

    // if (!txs[startTxId][startIndex]?.query) {
    //   continue;
    // }

    const endPosition = wantEnds.indexOf(wantStarts[i]);
    if (endPosition > i) {
      isWaiting[tx] = true;
    }

    console.log(isWaiting);
    const cols = [];
    if (tx === "b") {
      if (!isWaiting["a"]) {
        console.log(`padding a`);
        cols.push({});
      } else {
        cols.push(null);
      }
    }
    console.log(
      `${wantStarts[i]} ${txs[txId][startIndex].query} rowspan=${
        endPosition - i + 1
      }`
    );
    cols.push({
      ...txs[txId][startIndex],
      rowspan: isWaiting[tx] ? endPosition - i + 2 : 1,
    });
    if (tx === "a") {
      if (!isWaiting["b"]) {
        console.log(`padding b`);
        cols.push({});
      } else {
        cols.push(null);
      }
    }

    rows.push(cols);

    if (isWaiting[endTx]) {
      console.log("WAIT END: " + wantEnds[i]);
      rows.push([{}]);

      isWaiting[endTx] = false;
    }
  }

  return rows;
}

function findValue<T>(
  map: Record<string, T | undefined>,
  k1: string,
  k2?: string
): T | undefined {
  return map[`${k1}:${k2}`] || map[k1] || (k2 && map[k2]) || undefined;
}

function getValue<T>(
  map: Record<string, T | undefined> & { "*": T },
  k1: string,
  k2?: string
): T {
  return findValue(map, k1, k2) || map["*"];
}

function isLocked(wantEnds: string[]) {
  return !wantEnds
    .map((v) => {
      const [tx, idx] = v.split(":");

      return [tx, Number(idx)] as const;
    })
    .every(([tx, idx], i, arr) => {
      if (i === 0) {
        return true;
      }

      const [prevTx, prevIdx] = arr[i - 1];

      if (tx === "a") {
        return (
          (prevTx === "b" && prevIdx === idx - 1) ||
          (prevTx === "a" && prevIdx === idx - 1)
        );
      } else {
        return (
          (prevTx === "a" && prevIdx === idx) ||
          (prevTx === "b" && prevIdx === idx - 1)
        );
      }
    });
}

function App() {
  const [selected, select] = useState<{
    database: string;
    level: string;
    spec: Spec;
  } | null>(null);

  return (
    <div className="App">
      <h1>zakodb</h1>
      <table border={1}>
        <thead>
          <tr>
            <th>DBMS</th>
            <th>Level</th>
            <th>Model</th>
            {orderedSpecs.map((spec) => (
              <th key={spec.name}>{spec.name}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {databases.map((database) =>
            Array.from(Object.entries(getValue(dbLevels, database))).map(
              ([level, levelName]) => {
                const isDefault = defaultLevel[database] == level;

                return (
                  <tr key={`${database}-${level}`}>
                    <td>{dbNames[database]}</td>
                    <td>
                      {levelName
                        .split("/")
                        .reduce<React.ReactNode[]>(
                          (acc, x) =>
                            acc.length > 0 ? [...acc, <br />, x] : [x],
                          []
                        )}
                      {isDefault && "★"}
                    </td>
                    <td>{models[database][level]}</td>
                    {orderedSpecs.map((spec) => {
                      const skip =
                        !!spec.skip && findValue(spec.skip, database, level);
                      const ok =
                        levelInt[level] >=
                        levelInt[getValue(spec.threshold, database)];
                      const err = spec.txs.some((queries) =>
                        queries.some(
                          (q) =>
                            q.wantErr && `${database}:${level}` in q.wantErr
                        )
                      );
                      const locked = isLocked(
                        getValue(spec.wantEnds, database, level)
                      );

                      return (
                        <td
                          key={spec.name}
                          style={{
                            backgroundColor: skip
                              ? "lightgray"
                              : ok
                              ? err
                                ? "yellow"
                                : locked
                                ? "green"
                                : "lightgreen"
                              : "red",
                          }}
                        >
                          {skip ? (
                            "N/A"
                          ) : (
                            <a
                              onClick={() => select({ database, level, spec })}
                            >
                              {ok
                                ? err
                                  ? "ERROR"
                                  : locked
                                  ? "LOCK"
                                  : "OK"
                                : "NG"}
                            </a>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              }
            )
          )}
        </tbody>
      </table>
      {selected && <Anomaly database={selected.database} level={selected.level} {...selected.spec} />}
    </div>
  );
}

const Anomaly: React.FC<{ database: string; level: string } & Spec> = ({
  database,
  level,
  name,
  txs,
  threshold,
  wantStarts,
  wantEnds,
}) => {
  const ok = levelInt[level] >= levelInt[getValue(threshold, database)];

  const rows = convertTxs(
    txs,
    getValue(wantStarts, database, level),
    getValue(wantEnds, database, level)
  );

  return (
    <div>
      <h2>{dbNames[database]}</h2>
      <div key={`${name}-${level}`}>
        <h3>{level}</h3>
        <div key={database} id={`${name}-${database}-${level}`}>
          <h4>
            {name}: {ok ? "OK" : "NG"}
          </h4>
          <div>{JSON.stringify(threshold)}</div>
          <div>{JSON.stringify(wantStarts)}</div>
          <div>{getValue(wantStarts, database, level).join(", ")}</div>
          <div>{getValue(wantEnds, database, level).join(", ")}</div>
          <table border={1}>
            <tbody>
              {rows.map((steps, i) => (
                <Row
                  key={i}
                  database={database}
                  level={level}
                  ok={ok}
                  steps={steps}
                />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

const Row: React.FC<{
  key: number;
  database: string;
  level: string;
  ok: boolean;
  steps: (Partial<
    Tx & {
      rowspan: number;
    }
  > | null)[];
}> = ({ key, database, level, ok, steps }) => (
  <tr key={key}>
    {steps.map((step, j) => {
      if (!step) {
        return null;
      }

      const wantErr = step.wantErr?.[`${database}:${level}`];

      return (
        <>
          <td rowSpan={step.rowspan}>{step.query || "-"}</td>
          <td rowSpan={step.rowspan} style={{ minWidth: 24 }}>
            {wantErr ? (
              <div>
                <span>{wantErr}</span>
              </div>
            ) : step.wantOk && step.wantNg ? (
              <div>
                <span
                  style={
                    ok
                      ? { color: "green" }
                      : {
                          textDecorationLine: "line-through",
                        }
                  }
                >
                  {step.wantOk.map((want) => want.Int64)}
                </span>
                <span
                  style={
                    ok
                      ? {
                          textDecorationLine: "line-through",
                        }
                      : { color: "red" }
                  }
                >
                  {step.wantNg.map((want) => want.Int64)}
                </span>
              </div>
            ) : step.want ? (
              <span>{step.want.map((want) => want.Int64)}</span>
            ) : null}
          </td>
        </>
      );
    })}
  </tr>
);

export default App;
