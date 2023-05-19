import "./App.css";
import specs from "../../test/specs.json";
import { useState } from "react";
import { useEffect } from "react";
import { ReactComponent as GitHubMark } from "./github-mark.svg";

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
const CURSOR_STABILITY = "CURSOR STABILITY";
const READ_STABILITY = "RS";
const REPEATABLE_READ = "REPEATABLE READ";
// const REPEATABLE_READ_LOCK = "REPEATABLE READ LOCK";
const SNAPSHOT = "SNAPSHOT";
const SERIALIZABLE = "SERIALIZABLE";

const levels = [
  NO_TRANSACTION,
  READ_UNCOMMITTED,
  READ_COMMITTED,
  READ_COMMITTED_SNAPSHOT,
  CURSOR_STABILITY,
  READ_STABILITY,
  REPEATABLE_READ,
  // REPEATABLE_READ_LOCK,
  SNAPSHOT,
  SERIALIZABLE,
] as const;
type Level = (typeof levels)[number];

const levelInt: Record<string, number> = {
  [NO_TRANSACTION]: 0,
  [READ_UNCOMMITTED]: 1,
  [READ_COMMITTED]: 2,
  [READ_COMMITTED_SNAPSHOT]: 3,
  [CURSOR_STABILITY]: 4,
  [READ_STABILITY]: 5,
  [REPEATABLE_READ]: 6,
  // [REPEATABLE_READ_LOCK]: 6,
  [SNAPSHOT]: 6,
  [SERIALIZABLE]: 7,
};

const dbLevels = {
  [POSTGRES]: {
    // [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: [READ_UNCOMMITTED, <br />, `(${READ_COMMITTED} alias)`],
    [READ_COMMITTED]: READ_COMMITTED,
    [REPEATABLE_READ]: REPEATABLE_READ,
    // [REPEATABLE_READ_LOCK]: `${REPEATABLE_READ} (LOCK)`,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [SQLSERVER]: {
    // [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: READ_UNCOMMITTED,
    [READ_COMMITTED]: READ_COMMITTED,
    [READ_COMMITTED_SNAPSHOT]: `${READ_COMMITTED} (SNAPSHOT)`,
    [REPEATABLE_READ]: REPEATABLE_READ,
    [SNAPSHOT]: SNAPSHOT,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [ORACLE]: {
    // [NO_TRANSACTION]: "no transaction",
    [READ_COMMITTED]: READ_COMMITTED,
    [SERIALIZABLE]: SERIALIZABLE,
  },
  [DB2]: {
    // [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: [
      "UR(Uncommitted read)",
      <br />,
      READ_UNCOMMITTED,
      <br />,
      "DIRTY READ",
    ],
    [READ_COMMITTED]: [
      "CS",
      <br />,
      "CURSOR STABILITY",
      <br />,
      READ_COMMITTED,
    ],
    [CURSOR_STABILITY]: CURSOR_STABILITY,
    [READ_STABILITY]: `RS(Read stability)`,
    [REPEATABLE_READ]: ["RR", <br />, REPEATABLE_READ],
    [SERIALIZABLE]: `${SERIALIZABLE}/(RR alias)`,
  },
  "*": {
    // [NO_TRANSACTION]: "no transaction",
    [READ_UNCOMMITTED]: READ_UNCOMMITTED,
    [READ_COMMITTED]: READ_COMMITTED,
    [REPEATABLE_READ]: REPEATABLE_READ,
    // [REPEATABLE_READ_LOCK]: `${REPEATABLE_READ} (LOCK)`,
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
const models: Record<string, Record<string, React.ReactNode>> = {
  // [POSTGRES]: {
  //   [READ_UNCOMMITTED]: ["Monotonic Atomic View?", <br />, "Monotonic View?"],
  //   [READ_COMMITTED]: ["Monotonic Atomic View?", <br />, "Monotonic View?"],
  //   [REPEATABLE_READ]: ["Snapshot Isolation"],
  //   [SERIALIZABLE]: ["Serializable Snapshot Isolation"],
  // },
  // [MYSQL]: {
  //   [READ_UNCOMMITTED]: ["Read Uncommitted"],
  //   [READ_COMMITTED]: ["Monotonic Atomic View?", <br />, "Monotonic View?"],
  //   [REPEATABLE_READ]: ["Monotonic Atomic View?", <br />, "Monotonic View?"],
  //   [SERIALIZABLE]: ["Serializable"],
  // },
  // [SQLSERVER]: {
  //   [READ_UNCOMMITTED]: ["Read Uncommitted"],
  //   [READ_COMMITTED]: ["Monotonic Atomic View?", <br />, "Monotonic View?"],
  //   [READ_COMMITTED_SNAPSHOT]: [
  //     "Monotonic Atomic View?",
  //     <br />,
  //     "Monotonic View?",
  //   ],
  //   [REPEATABLE_READ]: ["Repeatable Read"],
  //   [SNAPSHOT]: ["Snapshot Isolation"],
  //   [SERIALIZABLE]: ["Serializable"],
  // },
  // [ORACLE]: {
  //   [READ_COMMITTED]: [
  //     "Monotonic Atomic View?",
  //     <br />,
  //     "Monotonic View?",
  //     <br />,
  //     "Monotonic Snapshot Reads?",
  //   ],
  //   [SERIALIZABLE]: ["Snapshot Isolation"],
  // },
  // [DB2]: {
  //   [READ_UNCOMMITTED]: ["Read Uncommitted?"],
  //   [READ_COMMITTED]: ["Read Committed?", <br />, "Cursor Stability?"],
  //   [READ_STABILITY]: ["Repeatable Read?"],
  //   [REPEATABLE_READ]: ["Serializable?"],
  //   [SERIALIZABLE]: ["Serializable?"],
  // },
  [POSTGRES]: {
    [READ_UNCOMMITTED]: "Read Committed (MVCC)",
    [READ_COMMITTED]: "Read Committed (MVCC)",
    [REPEATABLE_READ]: "Snapshot Isolation",
    // [REPEATABLE_READ_LOCK]: "Snapshot Isolation",
    [SERIALIZABLE]: "Serializable (MVCC)",
  },
  [MYSQL]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Read Committed (MVCC)",
    [REPEATABLE_READ]: "Snapshot Isolation",
    // [REPEATABLE_READ_LOCK]: "Read Committed (Gap Locking)",
    [SERIALIZABLE]: "Serializable (Locking)",
  },
  [SQLSERVER]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Read Committed (Locking)",
    [READ_COMMITTED_SNAPSHOT]: "Read Committed (MVCC)",
    [REPEATABLE_READ]: "Repeatable Read",
    [SNAPSHOT]: "Snapshot Isolation",
    [SERIALIZABLE]: "Serializable (Locking)",
  },
  [ORACLE]: {
    [READ_COMMITTED]: "Read Committed (MVCC)",
    [SERIALIZABLE]: "Snapshot Isolation",
  },
  [DB2]: {
    [READ_UNCOMMITTED]: "Read Uncommitted",
    [READ_COMMITTED]: "Read Committed (MVCC)",
    [CURSOR_STABILITY]: "Read Committed (Locking)",
    [READ_STABILITY]: "Repeatable Read",
    [REPEATABLE_READ]: "Serializable (Locking)",
    [SERIALIZABLE]: "Serializable (Locking)",
  },
};

const orderedSpecs: typeof specs = [];
// orderedSpecs.push(specs.find(({ name }) => name === "dirty write")!);
orderedSpecs.push(specs.find(({ name }) => name === "dirty read")!);
orderedSpecs.push(specs.find(({ name }) => name === "fuzzy read")!);
orderedSpecs.push(specs.find(({ name }) => name === "phantom read")!);
// orderedSpecs.push(specs.find(({ name }) => name === "phantom delete")!);
orderedSpecs.push(specs.find(({ name }) => name === "lost update")!);
orderedSpecs.push(specs.find(({ name }) => name === "write skew")!);
// orderedSpecs.push(
//   specs.find(({ name }) => name === "fuzzy read with locking read")!
// );
// orderedSpecs.push(
//   specs.find(({ name }) => name === "phantom read with locking read")!
// );
// orderedSpecs.push(
//   specs.find(({ name }) => name === "lost update with locking read")!
// );
// orderedSpecs.push(specs.find(({ name }) => name === "G-cursor")!);
// if (orderedSpecs.length !== specs.length) {
//   throw new Error(
//     `orderedSpecs.length !== specs.length: ${orderedSpecs.length}, ${specs.length}`
//   );
// }

const rows: { database: Database; level: string }[] = [];
for (const database of databases) {
  for (const level of Object.keys(getValue(dbLevels, database))) {
    rows.push({ database, level });
  }
}
const orderedRows: { database: Database; level: string }[] = [];
orderedRows.push(
  ...rows.filter(({ database, level }) => !models[database][level])!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) => models[database][level] === "Read Uncommitted"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) =>
      models[database][level] === "Read Committed (Locking)"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) => models[database][level] === "Read Committed (MVCC)"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) => models[database][level] === "Repeatable Read"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) =>
      models[database][level] === "Read Committed (Gap Locking)"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) => models[database][level] === "Snapshot Isolation"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) =>
      models[database][level] === "Serializable (Locking)"
  )!
);
orderedRows.push(
  ...rows.filter(
    ({ database, level }) => models[database][level] === "Serializable (MVCC)"
  )!
);
if (orderedRows.length !== rows.length) {
  throw new Error(
    `orderedRows.length !== rows.length: ${orderedRows.length}, ${rows.length}`
  );
}
// orderedRows.splice(0, orderedRows.length, ...rows);

type Tx = {
  query: string;
  want: Record<string, { Int64: number }[] | null | undefined> | null;
  wantOk: Record<string, { Int64: number }[] | null | undefined> | null;
  wantNg: Record<string, { Int64: number }[] | null | undefined> | null;
  wantErr: Record<string, string | undefined> | null;
};
type Spec = {
  name: string;
  txs: Tx[][];
  threshold: Record<string, string | undefined> & { "*": string };
  additionalOk: Record<string, string[] | undefined> | null;
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
    // console.log("------------");
    // console.log(`end: ${wantEnds[i]}, ${isWaiting}`);
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

    // console.log(isWaiting);
    const cols = [];
    if (tx === "b") {
      if (!isWaiting["a"]) {
        // console.log(`padding a`);
        cols.push({});
      } else {
        cols.push(null);
      }
    }
    // console.log(
    //   `${wantStarts[i]} ${txs[txId][startIndex].query} rowspan=${
    //     endPosition - i + 1
    //   }`
    // );
    cols.push({
      ...txs[txId][startIndex],
      rowspan: isWaiting[tx] ? endPosition - i + 2 : 1,
    });
    if (tx === "a") {
      if (!isWaiting["b"]) {
        // console.log(`padding b`);
        cols.push({});
      } else {
        cols.push(null);
      }
    }

    rows.push(cols);

    if (isWaiting[endTx]) {
      // console.log("WAIT END: " + wantEnds[i]);
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
  return (
    map[`${k1}:${k2}`] || map[k1] || (k2 && map[k2]) || map["*"] || undefined
  );
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

  const [levelChecks, setLevelChecks] = useState<readonly boolean[]>(
    Array(levels.length).fill(false)
  );

  const [shouldFilter, setShouldFilter] = useState(false);
  const [checks, setChecks] = useState(new Set<string>());

  let currentRow: { database: string; level: string } | undefined;
  let prevRow: { database: string; level: string } | undefined;

  return (
    <div className="antialiased text-slate-700 xdark:text-slate-400 bg-white xdark:bg-slate-900">
      <div className="sticky top-0 bg-white border-b border-slate-900/10">
        <div className="max-w-8xl mx-auto px-8 py-4 flex justify-between">
          <h1 className="text-3xl font-extrabold text-slate-800">
            Isolation Test
          </h1>
          <div className="flex items-center">
            <a href="https://github.com/yokomotod/zakodb">
              <GitHubMark
                width={32}
                height={32}
                viewBox="0 0 98 96"
                fill="currentColor"
              />
            </a>
          </div>
        </div>
      </div>
      {/* <CheckBoxes items={levels} onChange={setLevelChecks} /> */}
      <div className="max-w-8xl mx-auto px-8 py-4">
        <div className="p-2 flex space-x-2">
          <button
            className="px-2 py-1 font-semibold text-sm text-slate-700 rounded-md ring-1 disabled:opacity-75"
            disabled={shouldFilter || checks.size === 0}
            onClick={() => setShouldFilter(true)}
          >
            絞り込み
          </button>
          <button
            className="px-2 py-1 font-semibold text-sm text-slate-700 rounded-md ring-1 disabled:opacity-75"
            disabled={!shouldFilter}
            onClick={() => setShouldFilter(false)}
          >
            解除
          </button>
        </div>
        {/* `height: 1px` to make <td> children `height: 100%` works */}
        <table border={1} className="border border-slate-400 w-full h-[1px]">
          <thead>
            <tr>
              <th className="border border-slate-300 p-4" />
              <th className="border border-slate-300 p-4">データベース</th>
              <th className="border border-slate-300 p-4">
                設定値 (★:デフォルト)
              </th>
              <th className="border border-slate-300 p-4">モデル</th>
              {orderedSpecs.map((spec) => (
                <th className="border border-slate-300 p-4" key={spec.name}>
                  {spec.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {orderedRows.map(({ database, level }) => {
              if (
                levelChecks.includes(true) &&
                !levelChecks[(levels as readonly string[]).indexOf(level)]
              ) {
                return null;
              }

              prevRow = currentRow;
              currentRow = { database, level };

              const levelName = (
                getValue(dbLevels, database) as Record<string, string>
              )[level];
              const isDefault = defaultLevel[database] == level;

              const key = `${database}-${level}`;
              if (shouldFilter && !checks.has(key)) {
                return null;
              }

              return (
                <tr
                  key={key}
                  // className={
                  //   prevRow &&
                  //   models[prevRow.database][prevRow.level] !=
                  //     models[database][level]
                  //     ? "border-top-2"
                  //     : undefined
                  // }
                >
                  <td className="border border-slate-300 p-4">
                    <input
                      type="checkbox"
                      onChange={(e) => {
                        const newChecks = new Set(checks);
                        if (e.currentTarget.checked) {
                          newChecks.add(key);
                        } else {
                          newChecks.delete(key);
                        }
                        setChecks(newChecks);
                      }}
                    />
                  </td>
                  <td className="border border-slate-300 p-4">
                    {dbNames[database]}
                  </td>
                  <td className="border border-slate-300 p-4">
                    {levelName}
                    {isDefault && "★"}
                  </td>
                  <td className="border border-slate-300 p-4">
                    {models[database][level]}
                  </td>
                  {orderedSpecs.map((spec) => {
                    const skip =
                      !!spec.skip && findValue(spec.skip, database, level);
                    let ok =
                      levelInt[level] >=
                      levelInt[getValue(spec.threshold, database)];

                    if (
                      (spec.additionalOk as Record<string, string[]> | null)?.[
                        database
                      ]?.includes(level)
                    ) {
                      ok = true;
                    }
                    const aborted = spec.txs.some((queries) =>
                      queries.some(
                        (q) => q.wantErr && `${database}:${level}` in q.wantErr
                      )
                    );
                    const deadLocked = spec.txs.some((queries) =>
                      queries.some(
                        (q) =>
                          q.wantErr &&
                          (q.wantErr as Record<string, string> | null)?.[
                            `${database}:${level}`
                          ]
                            ?.toLowerCase()
                            ?.includes("deadlock")
                      )
                    );
                    const locked = isLocked(
                      getValue(spec.wantEnds, database, level)
                    );

                    return (
                      <td
                        className="border border-slate-300 hover:border-4"
                        key={spec.name}
                        // style={{
                        //   backgroundColor: skip
                        //     ? "lightgray"
                        //     : ok
                        //     ? deadLocked
                        //       ? "purple"
                        //       : aborted
                        //       ? "yellow"
                        //       : locked
                        //       ? "teal"
                        //       : "lime"
                        //     : "red",
                        // }}
                      >
                        {skip ? (
                          "n/a"
                        ) : (
                          <a
                            className="w-full h-full flex items-center justify-center text-center"
                            href={`#${database}-${level}-${spec.name}`}
                            onClick={() => select({ database, level, spec })}
                          >
                            {ok
                              ? deadLocked
                                ? ["◯", <br />, "(deadlock)"]
                                : aborted
                                ? ["◯", <br />, "(aborted)"]
                                : locked
                                ? ["◯", <br />, "(locked)"]
                                : "◯"
                              : "×"}
                          </a>
                        )}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
        {selected && (
          <Anomaly
            database={selected.database}
            level={selected.level}
            {...selected.spec}
          />
        )}
      </div>
    </div>
  );
}

const CheckBoxes: React.FC<{
  items: readonly string[];
  labels?: Record<string, string>;
  onChange: (checks: readonly boolean[]) => void;
}> = ({ items, labels, onChange }) => {
  const [checks, setChecks] = useState(
    Array<boolean>(items.length).fill(false)
  );

  useEffect(() => {
    onChange(checks);
  }, [checks]);

  return (
    <div>
      {items.map((item, i) => (
        <label>
          <input
            type="checkbox"
            onChange={() =>
              setChecks((prev) => {
                prev[i] = !prev[i];
                return [...prev];
              })
            }
          />
          {labels ? labels[item] : item}
        </label>
      ))}
    </div>
  );
};

const Anomaly: React.FC<{ database: string; level: string } & Spec> = ({
  database,
  level,
  name,
  txs,
  threshold,
  additionalOk,
  wantStarts,
  wantEnds,
}) => {
  let ok = levelInt[level] >= levelInt[getValue(threshold, database)];
  if (
    (additionalOk as Record<string, string[]> | null)?.[database]?.includes(
      level
    )
  ) {
    ok = true;
  }

  const rows = convertTxs(
    txs,
    getValue(wantStarts, database, level),
    getValue(wantEnds, database, level)
  );

  return (
    <div className="py-4">
      <h2 className="py-4 text-xl font-bold text-slate-800">
        {dbNames[database]} - {level} - {name}: {ok ? "OK" : "NG"}
      </h2>
      <div id={`${database}-${level}-${name}`}>
        {/* <div>threshold: {JSON.stringify(threshold)}</div>
          <div>wantStarts: {JSON.stringify(wantStarts)}</div>
          <div>wantEnds: {JSON.stringify(wantEnds)}</div>
          <div>
            wantStarts: {getValue(wantStarts, database, level).join(", ")}
          </div>
          <div>wantEnds: {getValue(wantEnds, database, level).join(", ")}</div> */}
        <table className="w-full border border-slate-400">
          <thead>
            <tr>
              <th className="border border-slate-300 p-1" rowSpan={2}>
                時刻
              </th>
              {txs.map((_, i) => (
                <>
                  <th className="border border-slate-300 p-1" colSpan={2}>
                    トランザクション{i + 1}
                  </th>
                </>
              ))}
            </tr>
            <tr>
              {txs.map((_, i) => (
                <>
                  <th className="border border-slate-300 p-1">クエリ</th>
                  <th className="border border-slate-300 p-1">結果</th>
                </>
              ))}
            </tr>
          </thead>
          <tbody className="border-top-2">
            {rows.map((steps, time) => (
              <Row
                time={time}
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
  );
};

const Row: React.FC<{
  time: number;
  database: string;
  level: string;
  ok: boolean;
  steps: (Partial<
    Tx & {
      rowspan: number;
    }
  > | null)[];
}> = ({ time, database, level, ok, steps }) => (
  <tr key={time}>
    <td className="border border-slate-300 p-1 text-center">T{time + 1}</td>
    {steps.map((step, j) => {
      if (!step) {
        return null;
      }

      const want = step.want && findValue(step.want, database, level);
      const wantOk =
        want != undefined
          ? undefined
          : step.wantOk && findValue(step.wantOk, database, level);
      const wantNg =
        want != undefined
          ? undefined
          : step.wantNg && findValue(step.wantNg, database, level);
      const wantErr = step.wantErr && findValue(step.wantErr, database, level);

      return (
        <>
          <td
            className="border border-slate-300 p-1 align-top"
            rowSpan={step.rowspan}
          >
            {step.query}
          </td>
          <td
            className="border border-slate-300 p-1 align-bottom"
            rowSpan={step.rowspan}
            style={{ minWidth: 24 }}
          >
            {wantErr ? (
              <div>
                <span>
                  {wantErr.toLowerCase().includes("deadlock")
                    ? "Deadlock Error"
                    : "Abort"}
                </span>
              </div>
            ) : wantOk && wantNg ? (
              <div>
                <span
                  style={
                    ok
                      ? // ? { color: "green" }
                        undefined
                      : {
                          textDecorationLine: "line-through",
                        }
                  }
                >
                  ○: {wantOk.map((want) => want.Int64).join(", ")}
                </span>
                <br />
                <span
                  style={
                    ok
                      ? {
                          textDecorationLine: "line-through",
                        }
                      : // : { color: "red" }
                        undefined
                  }
                >
                  ×: {wantNg.map((want) => want.Int64).join(", ")}
                </span>
              </div>
            ) : want ? (
              <span>{want.map((want) => want.Int64).join(", ")}</span>
            ) : null}
          </td>
        </>
      );
    })}
  </tr>
);

export default App;
