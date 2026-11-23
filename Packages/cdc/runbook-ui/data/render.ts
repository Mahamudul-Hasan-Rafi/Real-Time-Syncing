import { INTRO, RUNBOOK, VARIABLES, type Command } from "./runbook.ts";

export type Values = Record<string, string>;

export const defaultValues = (): Values =>
  Object.fromEntries(VARIABLES.map((v) => [v.key, v.value]));

/** Fill {{KEY}} placeholders. Unknown keys are left as they are. */
export function render(code: string, values: Values): string {
  // The scripts' SQL*Plus DEFINEs are compared with the dictionary, which stores names in upper case
  const all: Values = { ...values, CDC_USER_UC: (values.CDC_USER ?? "").toUpperCase(), PDB_NAME_UC: (values.PDB_NAME ?? "").toUpperCase() };
  return code.replace(/\{\{(\w+)\}\}/g, (m, key: string) => all[key] ?? m);
}

export const commandText = (c: Command, values: Values) => render(c.code, values);

/** "1", "2.1": numbers are derived from the order in RUNBOOK, so reordering never leaves a stale number. */
export const numbers: Record<string, string> = {};
RUNBOOK.forEach((s, si) => {
  numbers[s.id] = String(si + 1);
  s.steps.forEach((st, i) => {
    numbers[st.id] = `${si + 1}.${i + 1}`;
  });
});

const titles: Record<string, string> = {};
for (const s of RUNBOOK) for (const st of s.steps) titles[st.id] = st.title;

/** Splits a note into text and [[step-id]] references. */
export type Part = { text: string } | { ref: string; label: string };
export function parseNote(note: string): Part[] {
  const parts: Part[] = [];
  let last = 0;
  for (const m of note.matchAll(/\[\[([\w-]+)\]\]/g)) {
    if (m.index! > last) parts.push({ text: note.slice(last, m.index) });
    const id = m[1];
    if (!numbers[id]) throw new Error(`runbook: unknown step reference [[${id}]]`);
    parts.push({ ref: id, label: `step ${numbers[id]}` });
    last = m.index! + m[0].length;
  }
  if (last < note.length) parts.push({ text: note.slice(last) });
  return parts;
}

const mdNote = (note: string) =>
  parseNote(note)
    .map((p) => ("text" in p ? p.text : `[${p.label}](#s${numbers[p.ref].replace(".", "-")})`))
    .join("");

export const anchorOf = (id: string) => `s${numbers[id].replace(".", "-")}`;

/** The whole runbook as one Markdown document: this is what is shared with the DBA. */
export function toMarkdown(values: Values, generatedNote?: string): string {
  const out: string[] = [`# ${INTRO.title}`, ""];
  if (generatedNote) out.push(`<!-- ${generatedNote} -->`, "");
  out.push(`**For:** ${INTRO.audience}`, "", INTRO.summary, "", "## Before you start", "");
  for (const b of INTRO.before) out.push(`- ${b}`);
  out.push("", "## Scripts", "", "| Script (dba_scripts/) | What it does | Runbook |", "|---|---|---|");
  for (const s of INTRO.scripts) out.push(`| \`${s.file}\` | ${s.does} | ${s.covers} |`);
  out.push("", "## Contents", "");
  for (const s of RUNBOOK) {
    out.push(`${numbers[s.id]}. [${s.title}](#s${numbers[s.id]})`);
    for (const st of s.steps) out.push(`    - ${numbers[st.id]} [${st.title}](#${anchorOf(st.id)})`);
  }
  out.push("");

  for (const s of RUNBOOK) {
    out.push(`<a id="s${numbers[s.id]}"></a>`, `## ${numbers[s.id]}. ${s.title}`, "", s.intro, "");
    for (const st of s.steps) {
      out.push(`<a id="${anchorOf(st.id)}"></a>`, `### ${numbers[st.id]} ${st.title}`, "");
      const tags = [`**${st.risk}**`];
      if (st.when) tags.push(`**when:** ${mdNote(render(st.when, values))}`);
      if (st.script) tags.push(`**script:** \`${st.script}\``);
      out.push(tags.join(" · "), "", render(st.summary, values), "");
      for (const c of st.commands) {
        out.push(`Run in: ${c.where}`, "", "```" + c.lang, commandText(c, values), "```", "");
      }
      if (st.expect) out.push(`**Expected:** ${mdNote(render(st.expect, values))}`, "");
      for (const n of st.notes ?? []) out.push(`- ${mdNote(render(n, values))}`);
      if (st.notes?.length) out.push("");
      out.push("[Back to contents](#contents)", "");
    }
  }
  return out.join("\n").replace(/\n{3,}/g, "\n\n");
}
