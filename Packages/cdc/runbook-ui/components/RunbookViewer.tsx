"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import { INTRO, RUNBOOK, VARIABLES, type Step } from "@/data/runbook";
import {
  anchorOf,
  commandText,
  defaultValues,
  numbers,
  parseNote,
  render,
  toMarkdown,
  type Values,
} from "@/data/render";

const STORE = "cdc-runbook-v2";

const RISK_LABEL: Record<Step["risk"], string> = {
  "read-only": "Read-only",
  change: "Changes the database",
  downtime: "Needs downtime",
};

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="btn"
      onClick={async () => {
        try {
          await navigator.clipboard.writeText(text);
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        } catch {
          window.prompt("Copy the command:", text);
        }
      }}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

/** Text with [[step-id]] references turned into links. */
function Note({ text }: { text: string }) {
  return (
    <>
      {parseNote(text).map((p, i) =>
        "text" in p ? (
          <Fragment key={i}>{p.text}</Fragment>
        ) : (
          <a key={i} href={`#${anchorOf(p.ref)}`}>
            {p.label}
          </a>
        ),
      )}
    </>
  );
}

export default function RunbookViewer() {
  const [values, setValues] = useState<Values>(defaultValues);
  const [done, setDone] = useState<Record<string, boolean>>({});
  const [query, setQuery] = useState("");
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(STORE) ?? "{}");
      if (saved.values) setValues((v) => ({ ...v, ...saved.values }));
      if (saved.done) setDone(saved.done);
    } catch {}
    setLoaded(true);
  }, []);

  useEffect(() => {
    if (!loaded) return;
    try {
      // Passwords are never persisted.
      const kept = Object.fromEntries(
        VARIABLES.filter((v) => !v.secret).map((v) => [v.key, values[v.key]]),
      );
      localStorage.setItem(STORE, JSON.stringify({ values: kept, done }));
    } catch {}
  }, [values, done, loaded]);

  const q = query.trim().toLowerCase();
  const sections = useMemo(
    () =>
      RUNBOOK.map((s) => ({
        ...s,
        steps: s.steps.filter(
          (st) =>
            !q ||
            [st.title, st.summary, st.script ?? "", ...st.commands.map((c) => c.code), ...(st.notes ?? [])]
              .join(" ")
              .toLowerCase()
              .includes(q),
        ),
      })).filter((s) => s.steps.length),
    [q],
  );

  const total = RUNBOOK.reduce((n, s) => n + s.steps.length, 0);
  const finished = Object.values(done).filter(Boolean).length;

  const download = () => {
    const blob = new Blob([toMarkdown(values)], { type: "text/markdown" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "runbook.md";
    a.click();
    URL.revokeObjectURL(a.href);
  };

  return (
    <div className="layout">
      <aside className="side">
        <h1>CDC Runbook</h1>
        <p className="muted">Oracle database setup</p>
        <nav>
          {RUNBOOK.map((s) => (
            <a key={s.id} href={`#s${numbers[s.id]}`}>
              {numbers[s.id]}. {s.title}
            </a>
          ))}
        </nav>
        <p className="muted small">
          {finished} of {total} steps ticked
        </p>
      </aside>

      <main className="main">
        <section className="panel intro" aria-label="About this runbook">
          <h2>{INTRO.title}</h2>
          <p className="muted">For: {INTRO.audience}</p>
          <p>{INTRO.summary}</p>
          <h3>Before you start</h3>
          <ul className="notes">
            {INTRO.before.map((b) => (
              <li key={b}>{b}</li>
            ))}
          </ul>
          <h3>Scripts</h3>
          <table className="scripts">
            <thead>
              <tr>
                <th>Script (dba_scripts/)</th>
                <th>What it does</th>
                <th>Runbook</th>
              </tr>
            </thead>
            <tbody>
              {INTRO.scripts.map((s) => (
                <tr key={s.file}>
                  <td>
                    <code>{s.file}</code>
                  </td>
                  <td>{s.does}</td>
                  <td>{s.covers}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel" aria-label="Variables">
          <div className="panel-head">
            <h2>Your environment</h2>
            <span className="muted small">
              Commands update as you type. Nothing is sent anywhere, and passwords are not saved.
            </span>
          </div>
          <div className="vars">
            {VARIABLES.map((v) => (
              <label key={v.key}>
                <span>{v.label}</span>
                <input
                  type={v.secret ? "password" : "text"}
                  value={values[v.key]}
                  autoComplete="off"
                  spellCheck={false}
                  onChange={(e) => setValues({ ...values, [v.key]: e.target.value })}
                />
                {v.help && <em>{v.help}</em>}
              </label>
            ))}
          </div>
          <div className="row">
            <input
              className="search"
              type="search"
              placeholder="Search steps and commands"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <button className="btn" onClick={() => setValues(defaultValues())}>
              Reset values
            </button>
            <button className="btn" onClick={download}>
              Download runbook.md
            </button>
          </div>
        </section>

        {sections.length === 0 && <p className="muted">No steps match "{query}".</p>}

        {sections.map((s) => (
          <section key={s.id} id={`s${numbers[s.id]}`} className="sec">
            <h2>
              {numbers[s.id]}. {s.title}
            </h2>
            <p className="muted">{s.intro}</p>
            {s.steps.map((st) => (
              <article
                key={st.id}
                id={anchorOf(st.id)}
                className={`step ${done[st.id] ? "is-done" : ""}`}
              >
                <header>
                  <label className="check">
                    <input
                      type="checkbox"
                      checked={!!done[st.id]}
                      onChange={(e) => setDone({ ...done, [st.id]: e.target.checked })}
                    />
                    <h3>
                      {numbers[st.id]} {st.title}
                    </h3>
                  </label>
                  <span className={`tag ${st.risk}`}>{RISK_LABEL[st.risk]}</span>
                </header>
                {(st.when || st.script) && (
                  <p className="meta">
                    {st.when && (
                      <span>
                        <strong>When:</strong> <Note text={st.when} />
                      </span>
                    )}
                    {st.script && (
                      <span>
                        <strong>Script:</strong> <code>{st.script}</code>
                      </span>
                    )}
                  </p>
                )}
                <p>{st.summary}</p>
                {st.commands.map((c, i) => {
                  const text = commandText(c, values);
                  return (
                    <div className="cmd" key={i}>
                      <div className="cmd-head">
                        <span>
                          {c.lang === "sql" ? "SQL" : "Shell"} · {render(c.where, values)}
                        </span>
                        <CopyButton text={text} />
                      </div>
                      <pre>
                        <code>{text}</code>
                      </pre>
                    </div>
                  );
                })}
                {st.expect && (
                  <p className="expect">
                    <strong>Expected:</strong> <Note text={st.expect} />
                  </p>
                )}
                {st.notes && (
                  <ul className="notes">
                    {st.notes.map((n) => (
                      <li key={n}>
                        <Note text={n} />
                      </li>
                    ))}
                  </ul>
                )}
              </article>
            ))}
          </section>
        ))}
      </main>
    </div>
  );
}
