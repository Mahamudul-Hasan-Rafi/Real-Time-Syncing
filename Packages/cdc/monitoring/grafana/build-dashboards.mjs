// Generates the Grafana dashboards in ./dashboards. Run: node build-dashboards.mjs
// Edit the panels here, regenerate, and commit both this file and the JSON. Grafana reloads the JSON every 30s.
//
// Dashboard hierarchy (overview first, then one drill-down per component, as Strimzi / Confluent dashboards do):
//   kafka-command-center  Kafka Stack Command Center  the whole stack on one screen: resources, pipeline flow,
//                                                      connector and task walls, auto-recovery, lag, latency, alerts
//   cdc-connectors        Debezium Connectors          connector/task state over time, streaming, snapshot, errors
//   kafka-cluster         Kafka Cluster                KRaft and partition health, throughput, latency, saturation
//   kafka-topics          Topics & Consumer Groups     topic inventory, consumer lag and catch-up time
//
// Metric sources (see ../prometheus.yml): kafka-exporter (job "kafka"), broker JMX ("kafka-jmx"), Connect/Debezium
// JMX ("connect-jmx"), cAdvisor ("containers"), node-exporter ("node"), connect-guardian ("connect-guardian").
// The JMX rules are in ../../dockerfiles/{kafka,connect}/jmx-exporter.yml.
import { mkdirSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const out = join(dirname(fileURLToPath(import.meta.url)), "dashboards");
mkdirSync(out, { recursive: true });
// Every dashboard is generated here: remove the old files so Grafana also drops dashboards that no longer exist
for (const f of readdirSync(out)) if (f.endsWith(".json")) unlinkSync(join(out, f));

const DS = { type: "prometheus", uid: "${ds}" };

// ---- palette -------------------------------------------------------------------------------------------------------
const COL = { green: "#73BF69", blue: "#5794F2", orange: "#FF9830", red: "#F2495C", purple: "#B877D9", cyan: "#8AB8FF", teal: "#5AD7C9", yellow: "#FADE2A", label: "#CCCCDC" };

// ---- helpers -------------------------------------------------------------------------------------------------------
const steps = (...s) => ({ mode: "absolute", steps: s.map(([color, value]) => ({ color, value: value ?? null })) });
const OK_WARN_CRIT = steps(["green"], ["orange", 80], ["red", 90]);
const ZERO_GOOD = steps(["green"], ["red", 1]);
const NO_DATA = { type: "special", options: { match: "null", result: { text: "NO DATA", color: "gray" } } };
// A missing series must never look green or red: every stat gets a grey NO DATA unless it defines its own
const withNoData = (m = []) => (m.some((x) => x.type === "special") ? m : [...m, NO_DATA]);
// With no series at all Grafana paints the threshold's base color, so a stat whose base is red or green would show
// "no data" as a verdict. Put a grey base underneath and move the original base to 0 (no stat here goes below 0).
const neutralBase = (t) => ({ ...t, steps: [{ color: "gray", value: null }, ...t.steps.map((s, i) => (i === 0 ? { ...s, value: 0 } : s))] });
const valueMap = (pairs) => ({ type: "value", options: Object.fromEntries(pairs.map(([v, text, color]) => [v, { text, color }])) });
const range = (from, to, text, color) => ({ type: "range", options: { from, to, result: { ...(text && { text }), color } } });
const target = (expr, legend, instant = false) => ({ datasource: DS, expr, legendFormat: legend ?? "", refId: "A", instant, range: !instant });
const withRef = (targets) => targets.map((t, i) => ({ ...t, refId: String.fromCharCode(65 + i) }));
// Canvas elements need a field to exist; -1 stands for "no data" and is mapped to a grey text
const orNone = (expr) => `(${expr}) or on() vector(-1)`;
const noneMap = (text = "NO DATA") => valueMap([[-1, text, "gray"]]);
// A health check term that counts as failed (0) instead of disappearing when its metric is missing
const check = (expr) => `((${expr}) or on() vector(0))`;
const fieldProps = (o) => [
  o.unit && { id: "unit", value: o.unit },
  o.decimals !== undefined && { id: "decimals", value: o.decimals },
  o.mappings && { id: "mappings", value: o.mappings },
  o.thresholds && { id: "thresholds", value: o.thresholds },
  o.color && { id: "color", value: { mode: "fixed", fixedColor: o.color } },
].filter(Boolean);

function builder() {
  let id = 1;
  const panels = [];
  const add = (p) => { panels.push({ id: id++, datasource: DS, ...p }); };
  return {
    panels,
    row: (title, y, collapsed = false) => add({ type: "row", title, collapsed, gridPos: { x: 0, y, w: 24, h: 1 }, panels: [], datasource: undefined }),

    stat: (title, expr, [x, y, w, h], o = {}) => add({
      type: "stat", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef([target(expr, "", true)]),
      fieldConfig: { defaults: { unit: o.unit ?? "none", decimals: o.decimals, thresholds: neutralBase(o.thresholds ?? steps(["green"])), mappings: withNoData(o.mappings), noValue: o.noValue ?? "NO DATA", color: { mode: "thresholds" } }, overrides: [] },
      options: { colorMode: o.colorMode ?? "background", graphMode: "none", textMode: "value", reduceOptions: { calcs: ["lastNotNull"] }, justifyMode: "center", ...(o.valueSize && { text: { valueSize: o.valueSize } }) },
    }),

    // Name / value rows (a check list). rows: [{ name, expr, unit, decimals, mappings, thresholds, color, colorByValue }]
    list: (title, rows, [x, y, w, h], o = {}) => add({
      type: "stat", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef(rows.map((r) => target(orNone(r.expr), r.name, true))),
      fieldConfig: {
        defaults: { unit: "none", color: { mode: "fixed", fixedColor: COL.green }, mappings: [NO_DATA], noValue: "NO DATA", thresholds: steps(["green"]) },
        // Rows with thresholds or value mappings take their colour from the value; the rest are plain text
        overrides: rows.map((r) => ({
          matcher: { id: "byName", options: r.name },
          properties: [
            ...fieldProps({ unit: r.unit, decimals: r.decimals, thresholds: r.thresholds, mappings: [...(r.mappings ?? []), noneMap(r.noneText ?? "–")] }),
            { id: "color", value: r.thresholds || r.colorByValue ? { mode: "thresholds" } : { mode: "fixed", fixedColor: r.color ?? COL.green } },
          ],
        })),
      },
      options: { colorMode: "value", graphMode: "none", textMode: "value_and_name", orientation: "horizontal", justifyMode: "auto", wideLayout: true, reduceOptions: { calcs: ["lastNotNull"] }, text: { titleSize: o.titleSize ?? 13, valueSize: o.valueSize ?? 15 } },
    }),

    gauge: (title, expr, [x, y, w, h], o = {}) => add({
      type: "gauge", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef([target(expr, o.label ?? "", true)]),
      fieldConfig: { defaults: { unit: o.unit ?? "percent", min: 0, max: o.max ?? 100, decimals: o.decimals ?? 1, displayName: o.label, thresholds: o.thresholds ?? OK_WARN_CRIT, color: { mode: "thresholds" }, mappings: [NO_DATA], noValue: "NO DATA" }, overrides: [] },
      options: { reduceOptions: { calcs: ["lastNotNull"] }, showThresholdMarkers: false, showThresholdLabels: false, sizing: "auto", minVizWidth: 75, minVizHeight: 75, text: { valueSize: o.valueSize ?? 30 } },
    }),

    // Horizontal bars, one per series, filled up to max (e.g. memory used of limit per container)
    bars: (title, expr, legend, [x, y, w, h], o = {}) => add({
      type: "bargauge", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef([target(expr, legend, true)]),
      fieldConfig: { defaults: { unit: o.unit ?? "percent", min: 0, max: o.max ?? 100, decimals: o.decimals ?? 0, thresholds: o.thresholds ?? OK_WARN_CRIT, color: { mode: "thresholds" }, mappings: [], noValue: o.noValue ?? "NO DATA" }, overrides: [] },
      options: { displayMode: "gradient", orientation: "horizontal", valueMode: "color", namePlacement: "left", showUnfilled: true, sizing: "manual", minVizHeight: 14, maxVizHeight: 22, minVizWidth: 8, reduceOptions: { calcs: ["lastNotNull"] }, legend: { showLegend: false }, text: { titleSize: 12, valueSize: 13 } },
    }),

    // A status wall: one coloured tile per series (connector, task), named by the legend
    wall: (title, expr, legend, [x, y, w, h], o = {}) => add({
      type: "stat", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef([target(expr, legend, true)]),
      fieldConfig: { defaults: { unit: "none", thresholds: steps(["gray"]), mappings: withNoData(o.mappings), noValue: o.noValue ?? "NO DATA", color: { mode: "thresholds" } }, overrides: [] },
      options: { colorMode: "background", graphMode: "none", textMode: "value_and_name", orientation: "auto", wideLayout: true, justifyMode: "center", reduceOptions: { calcs: ["lastNotNull"] }, text: { titleSize: 13, valueSize: 18 } },
    }),

    ts: (title, targets, [x, y, w, h], o = {}) => add({
      type: "timeseries", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef(targets),
      fieldConfig: {
        defaults: {
          unit: o.unit ?? "none", min: o.min ?? (o.softZero === false ? undefined : 0), max: o.max, decimals: o.decimals,
          color: o.color ? { mode: "fixed", fixedColor: o.color } : { mode: "palette-classic" },
          custom: { drawStyle: o.bars ? "bars" : "line", lineInterpolation: "smooth", lineWidth: 2, fillOpacity: o.fill ?? 20, gradientMode: "opacity", showPoints: "never", spanNulls: true, axisBorderShow: false, axisPlacement: "auto", stacking: { mode: o.stack ? "normal" : "none", group: "A" }, thresholdsStyle: { mode: o.thresholds ? "dashed" : "off" } },
          thresholds: o.thresholds ?? steps(["green"]),
        },
        overrides: (o.colors ?? []).map(([name, color]) => ({ matcher: { id: "byName", options: name }, properties: [{ id: "color", value: { mode: "fixed", fixedColor: color } }] })),
      },
      options: { legend: { displayMode: o.legendTable ? "table" : "list", placement: o.legendTable ? "right" : "bottom", showLegend: targets.length > 1 || !!o.legend, ...(o.legendTable && { calcs: ["lastNotNull", "max"] }) }, tooltip: { mode: "multi", sort: "desc" } },
    }),

    // State over time per series (flapping connectors and tasks are obvious here). mappings colour each state.
    timeline: (title, targets, [x, y, w, h], o = {}) => add({
      type: "state-timeline", title, description: o.desc, gridPos: { x, y, w, h },
      targets: withRef(targets),
      fieldConfig: { defaults: { color: { mode: "thresholds" }, thresholds: steps(["text"]), mappings: [...o.mappings, NO_DATA], custom: { fillOpacity: 85, lineWidth: 0 } }, overrides: [] },
      options: { showValue: "never", mergeValues: true, alignValue: "left", rowHeight: 0.8, legend: { showLegend: true, displayMode: "list", placement: "bottom" }, tooltip: { mode: "single", sort: "none" } },
    }),

    // One row per entity. Every query is instant, aggregated "by (<key>)" (plus at most one extra label per query),
    // and the frames are outer-joined on the key. columns: [{ field: "Value #A" or a label, name, ...cell options }]
    table: (title, [x, y, w, h], t) => {
      const refs = t.queries.map((_, i) => String.fromCharCode(65 + i));
      const exclude = { Time: true };
      for (let i = 1; i <= t.queries.length; i++) exclude[`Time ${i}`] = true;
      t.queries.forEach((q, i) => { if (q.hide) exclude[`Value #${refs[i]}`] = true; });
      const rename = {}, index = {};
      t.columns.forEach((c, i) => { rename[c.field] = c.name; index[c.field] = i; });
      add({
        type: "table", title, description: t.desc, gridPos: { x, y, w, h }, links: t.links,
        targets: withRef(t.queries.map((q) => ({ ...target(q.expr, "", true), format: "table" }))),
        transformations: [
          { id: "joinByField", options: { byField: t.by, mode: "outer" } },
          { id: "organize", options: { excludeByName: exclude, indexByName: index, renameByName: rename } },
          ...(t.sort ? [{ id: "sortBy", options: { sort: [{ field: t.sort, desc: t.sortDesc ?? true }] } }] : []),
        ],
        fieldConfig: {
          defaults: { custom: { align: "auto", cellOptions: { type: "auto" }, inspect: false }, color: { mode: "thresholds" }, thresholds: steps(["text"]), mappings: [], noValue: "–" },
          overrides: t.columns.filter((c) => c.unit || c.decimals !== undefined || c.mappings || c.thresholds || c.cell || c.width || c.align).map((c) => ({
            matcher: { id: "byName", options: c.name },
            properties: [
              ...fieldProps({ unit: c.unit, decimals: c.decimals, mappings: c.mappings, thresholds: c.thresholds }),
              c.cell && { id: "custom.cellOptions", value: c.cell === "bg" ? { type: "color-background", mode: "basic" } : { type: "color-text" } },
              c.width && { id: "custom.width", value: c.width },
              c.align && { id: "custom.align", value: c.align },
            ].filter(Boolean),
          })),
        },
        options: { showHeader: true, cellHeight: "sm", footer: { show: false } },
      });
    },

    // A KPI tile: icon on the left, label, big coloured value, small subtitle. Built on the Canvas panel.
    tile: ([x, y, w, h], t) => {
      const el = (type, name, config, placement, constraint = { horizontal: "leftright", vertical: "top" }) => ({
        type, name, config, constraint, placement: { rotation: 0, ...placement },
        background: { color: { fixed: "transparent" } }, border: { color: { fixed: "transparent" } },
      });
      // Narrow (3-column) tiles get a smaller icon and text so nothing wraps
      const g = w <= 3 ? { icon: 34, iconLeft: 10, textLeft: 52, label: 13, value: 24, sub: 11 } : { icon: 46, iconLeft: 14, textLeft: 72, label: 15, value: 28, sub: 13 };
      const elements = [
        el("icon", "Icon", { path: { mode: "fixed", fixed: `img/icons/unicons/${t.icon}.svg` }, fill: { fixed: t.iconColor } }, { left: g.iconLeft, top: 44 - g.icon / 2 + 16, width: g.icon, height: g.icon }, { horizontal: "left", vertical: "top" }),
        el("text", "Label", { text: { mode: "fixed", fixed: t.label }, color: { fixed: COL.label }, size: g.label, align: "left", valign: "middle" }, { left: g.textLeft, right: 2, top: 12, height: 22 }),
        el("metric-value", "Value", { text: { mode: "field", field: "main", fixed: "" }, color: { field: "main", fixed: COL.green }, size: t.size ?? g.value, align: "left", valign: "middle" }, { left: g.textLeft, right: 2, top: 36, height: 44 }),
        t.sub
          ? el("metric-value", "Subtitle", { text: { mode: "field", field: "sub", fixed: "" }, color: { fixed: COL.label }, size: g.sub, align: "left", valign: "middle" }, { left: g.textLeft, right: 2, top: 84, height: 20 })
          : el("text", "Subtitle", { text: { mode: "fixed", fixed: t.subText ?? "" }, color: { fixed: COL.label }, size: g.sub, align: "left", valign: "middle" }, { left: g.textLeft, right: 2, top: 84, height: 20 }),
      ];
      add({
        type: "canvas", title: "", description: t.desc, gridPos: { x, y, w, h }, links: t.links,
        targets: withRef([target(orNone(t.main.expr), "main", true), ...(t.sub ? [target(orNone(t.sub.expr), "sub", true)] : [])]),
        fieldConfig: {
          defaults: { color: { mode: "thresholds" }, thresholds: steps(["green"]), mappings: [] },
          overrides: [
            { matcher: { id: "byName", options: "main" }, properties: fieldProps({ ...t.main, mappings: [...(t.main.mappings ?? []), noneMap(t.main.noneText)] }).concat([{ id: "color", value: { mode: "thresholds" } }]) },
            ...(t.sub ? [{ matcher: { id: "byName", options: "sub" }, properties: fieldProps({ ...t.sub, mappings: [...(t.sub.mappings ?? []), noneMap(t.sub.noneText ?? "–")] }) }] : []),
          ],
        },
        options: {
          inlineEditing: false, showAdvancedTypes: true, panZoom: false, infinitePan: false,
          root: { type: "frame", name: "Element root", elements, background: { color: { fixed: "transparent" } }, border: { color: { fixed: "transparent" } }, constraint: { horizontal: "left", vertical: "top" }, placement: { left: 0, top: 0, width: 100, height: 100, rotation: 0 } },
        },
      });
    },

    // Current and recent alerts: when they started, severity, component, a readable message and FIRING / Resolved
    alertHistory: (title, [x, y, w, h], messages) => add({
      type: "table", title, gridPos: { x, y, w, h },
      targets: withRef([
        { ...target("max by (alertname, severity, component) (max_over_time(ALERTS_FOR_STATE[24h])) * 1000", "", true), format: "table" },
        { ...target('max by (alertname) (ALERTS{alertstate="firing"}) or (0 * max by (alertname) (max_over_time(ALERTS_FOR_STATE[24h])))', "", true), format: "table" },
      ]),
      transformations: [
        { id: "joinByField", options: { byField: "alertname", mode: "outer" } },
        { id: "organize", options: {
          excludeByName: { "Time 1": true, "Time 2": true, Time: true },
          indexByName: { "Value #A": 0, severity: 1, component: 2, alertname: 3, "Value #B": 4 },
          renameByName: { "Value #A": "Since", severity: "Severity", component: "Component", alertname: "Message", "Value #B": "Status" },
        } },
        { id: "sortBy", options: { sort: [{ field: "Since", desc: true }] } },
      ],
      fieldConfig: {
        defaults: { custom: { align: "left", cellOptions: { type: "auto" } }, noValue: "No alerts in the last 24 hours" },
        overrides: [
          { matcher: { id: "byName", options: "Since" }, properties: [{ id: "unit", value: "time:HH:mm" }, { id: "custom.width", value: 62 }] },
          { matcher: { id: "byName", options: "Severity" }, properties: [{ id: "custom.width", value: 76 }, { id: "custom.align", value: "center" }, { id: "custom.cellOptions", value: { type: "color-background", mode: "basic" } }, { id: "mappings", value: [valueMap([["critical", "CRIT", "red"], ["warning", "WARN", "orange"], ["info", "INFO", "blue"]])] }] },
          { matcher: { id: "byName", options: "Component" }, properties: [{ id: "custom.width", value: 92 }] },
          { matcher: { id: "byName", options: "Message" }, properties: [{ id: "mappings", value: [valueMap(Object.entries(messages).map(([k, v]) => [k, v, "text"]))] }] },
          { matcher: { id: "byName", options: "Status" }, properties: [{ id: "custom.width", value: 78 }, { id: "custom.cellOptions", value: { type: "color-text" } }, { id: "mappings", value: [valueMap([[1, "FIRING", "red"], [0, "Resolved", "green"]])] }] },
        ],
      },
      options: { showHeader: true, cellHeight: "sm", footer: { show: false } },
    }),
  };
}

// ---- dashboard shell -----------------------------------------------------------------------------------------------
const NAV = [["Command Center", "/d/kafka-command-center"], ["Debezium Connectors", "/d/cdc-connectors"], ["Kafka Cluster", "/d/kafka-cluster"], ["Topics & Consumers", "/d/kafka-topics"]];
const queryVar = (name, label, query, regex = "") => ({
  name, label, type: "query", datasource: DS, query, definition: query, regex, refresh: 2, sort: 1,
  multi: true, includeAll: true, allValue: ".*", current: { selected: true, text: ["All"], value: ["$__all"] }, hide: 0,
});
const VARS = {
  connector: queryVar("connector", "Connector", "label_values(kafka_connect_connector_status, connector)"),
  server: queryVar("server", "Debezium server", "label_values(debezium_metrics_connected, name)"),
  topic: queryVar("topic", "Topic", "label_values(kafka_topic_partitions, topic)", "/^(?!__).*/"),
  group: queryVar("group", "Consumer group", "label_values(kafka_consumergroup_members, consumergroup)"),
};
const dashboard = (uid, title, description, panels, vars = []) => ({
  uid, title, description, tags: ["kafka", "debezium", "cdc"], schemaVersion: 39, version: 1,
  editable: false, graphTooltip: 1, refresh: "30s", time: { from: "now-1h", to: "now" },
  timepicker: { refresh_intervals: ["10s", "30s", "1m", "5m"] },
  links: NAV.filter(([, u]) => u !== `/d/${uid}`).map(([t, u]) => ({ title: t, type: "link", url: u, icon: "dashboard", keepTime: true, includeVars: false })),
  templating: { list: [{ name: "ds", label: "Data source", type: "datasource", query: "prometheus", current: {}, hide: 0 }, ...vars.map((v) => VARS[v])] },
  annotations: { list: [] },
  panels,
});
const viewDetails = (url) => [{ title: "View details", url, targetBlank: false }];

// ---- Kafka Connect: connector and task state -------------------------------------------------------------------------
const CF = 'connector=~"$connector"';
const ALL_CONNECTORS = 'connector=~".+"';
const tasks = (kind, f = CF) => `sum by (connector) (kafka_connect_worker_connector_${kind}_task_count{${f}})`;
const status = (re, f = CF) => `max by (connector) (kafka_connect_connector_status{${f},status=~"${re}"})`;
// Health per connector: 0 OK, 1 PAUSED (paused or stopped on purpose), 2 DEGRADED (running with failed or missing tasks,
// or unassigned / restarting), 3 FAILED. A connector can be RUNNING while all of its tasks have FAILED, which is why
// the connector state alone is not a health verdict.
const HEALTH = (f = CF) => [
  `(${status("failed", f)} * 3)`,
  `(${status("paused|stopped", f)} * 1)`,
  `(${status("unassigned|restarting", f)} * 2)`,
  `(${status("running", f)} * 2 * clamp_max((${tasks("failed", f)} > bool 0) + (${tasks("running", f)} < bool ${tasks("total", f)}), 1))`,
  `(${status("running", f)} * 0)`,
].join(" or ");
const HEALTH_MAP = [valueMap([[0, "OK", "green"], [1, "PAUSED", "blue"], [2, "DEGRADED", "orange"], [3, "FAILED", "red"]])];
const STATE = (f = CF) => ["running", "paused", "stopped", "unassigned", "restarting", "failed", "destroyed"].map((s, i) => `(${status(s, f)} * ${i})`).join(" or ");
const STATE_MAP = [valueMap([[0, "RUNNING", "green"], [1, "PAUSED", "blue"], [2, "STOPPED", "blue"], [3, "UNASSIGNED", "orange"], [4, "RESTARTING", "orange"], [5, "FAILED", "red"], [6, "DESTROYED", "red"]])];
const tstatus = (re) => `max by (connector, task) (kafka_connect_connector_task_status{${CF},status=~"${re}"})`;
const TASK_STATE = `(${tstatus("running")} * 0) or (${tstatus("paused|stopped")} * 1) or (${tstatus("unassigned|restarting")} * 2) or (${tstatus("failed")} * 3)`;
const TASK_MAP = [valueMap([[0, "RUNNING", "green"], [1, "PAUSED", "blue"], [2, "RESTARTING", "orange"], [3, "FAILED", "red"]])];
// Records moved by each connector: written to Kafka by source tasks, delivered by sink tasks
const RECORDS_RATE = (f = CF) => `(sum by (connector) (rate(kafka_connect_source_task_source_record_write_total{${f}}[5m]))) or (sum by (connector) (rate(kafka_connect_sink_task_sink_record_send_total{${f}}[5m])))`;
const RECORD_ERRORS = (f = CF, w = "1h") => `sum by (connector) (increase({__name__=~"kafka_connect_task_error_total_record_errors_total|kafka_connect_producer_record_error_total",${f}}[${w}]))`;

const HEALTH_ALL = HEALTH(ALL_CONNECTORS);
const CONNECTORS_TOTAL = `count(${HEALTH_ALL})`;
const CONNECTORS_BAD = `count((${HEALTH_ALL}) >= 2) or on() vector(0)`;
const FAILED_TASKS_ALL = "sum(kafka_connect_worker_connector_failed_task_count) or on() vector(0)";

// ---- Debezium (engine metrics are keyed by the logical server name = topic.prefix) ---------------------------------
const SF = 'name=~"$server"';
const ALL_SERVERS = 'name=~".+"';
const dbz = (m, f = SF, ctx = "streaming") => `debezium_metrics_${m}{context="${ctx}",${f}}`;
const CONNECTED = (f = SF) => `max by (name) (${dbz("connected", f)})`;
const LAG_S = (f = SF) => `(max by (name) (${dbz("millisecondsbehindsource", f)}) >= 0) / 1000`;     // -1 until the first event
const SINCE_EVENT = (f = SF) => `max by (name) (${dbz("millisecondssincelastevent", f)}) >= 0`;
const EVENTS_RATE = (f = SF) => `sum by (name) (rate(${dbz("totalnumberofeventsseen", f)}[5m]))`;
const QUEUE_PCT = (f = SF) => `100 * (1 - min by (name) (${dbz("queueremainingcapacity", f)}) / max by (name) (${dbz("queuetotalcapacity", f)}))`;
const DBZ_ERRORS = (f = SF, w = "1h") => `sum by (name) (increase({__name__=~"debezium_metrics_(errorcount|numberoferroneousevents)",context="streaming",${f}}[${w}]))`;
// 0 no snapshot taken (e.g. snapshot.mode=no_data before it completes), 1 running, 2 completed
const SNAPSHOT = (f = SF) => `(max by (name) (${dbz("snapshotrunning", f, "snapshot")}) * 1) + (max by (name) (${dbz("snapshotcompleted", f, "snapshot")}) * 2)`;
const SNAPSHOT_MAP = [valueMap([[0, "NONE", "text"], [1, "RUNNING", "blue"], [2, "COMPLETED", "green"], [3, "COMPLETED", "green"]])];
const ENGINES_TOTAL = `count(${CONNECTED(ALL_SERVERS)})`;
const ENGINES_DOWN = `count(${CONNECTED(ALL_SERVERS)} == 0) or on() vector(0)`;
const MAX_LAG_S = `max(${LAG_S(ALL_SERVERS)})`;
const LAG_THRESHOLDS = steps(["green"], ["orange", 10], ["red", 30]);

// ---- Kafka cluster -----------------------------------------------------------------------------------------------
const BROKERS = "max(kafka_brokers)";
const ACTIVE_CTRL = "sum(kafka_controller_kafkacontroller_activecontrollercount)";
const OFFLINE = "sum(kafka_controller_kafkacontroller_offlinepartitionscount) or on() vector(0)";
const URP = "sum(kafka_server_replicamanager_underreplicatedpartitions) or on() vector(0)";
const UNDER_MIN_ISR = "sum(kafka_server_replicamanager_underminisrpartitioncount) or on() vector(0)";
const REQ_IDLE = "100 * min(clamp_max(kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent, 1))";
const NET_IDLE = "100 * min(clamp_max(kafka_network_socketserver_networkprocessoravgidlepercent, 1))";
const btm = (m, f = 'topic=""') => `sum(rate(kafka_server_brokertopicmetrics_${m}_total{${f}}[5m]))`;
const BYTES_IN = btm("bytesin"), BYTES_OUT = btm("bytesout"), MSGS_IN = btm("messagesin");
const PRODUCE_P99 = 'max(kafka_network_requestmetrics_totaltimems_p99{request="Produce"})';
const KAFKA_OK = `(clamp_max(${BROKERS}, 1) * (${ACTIVE_CTRL} == bool 1) * ((${OFFLINE}) == bool 0) * ((${URP}) == bool 0) * ((${UNDER_MIN_ISR}) == bool 0)) or on() vector(0)`;
const KAFKA_HEALTH = `100 * (${[`clamp_max(${BROKERS}, 1)`, `${ACTIVE_CTRL} == bool 1`, `(${OFFLINE}) == bool 0`, `(${URP}) == bool 0`, `(${UNDER_MIN_ISR}) == bool 0`, `${REQ_IDLE} > bool 20`].map(check).join(" + ")}) / 6`;
const CTRL_MAP = [valueMap([[0, "NONE", "red"], [1, "ACTIVE", "green"]]), range(2, 1e9, "SPLIT BRAIN", "red")];

// ---- Topics and consumer groups (kafka-exporter) ----------------------------------------------------------------------
const USER_TOPICS = 'topic!~"__.*"';
const TF = 'topic=~"$topic"';
const GF = 'consumergroup=~"$group"';
const TOPICS = `count(count by (topic) (kafka_topic_partitions{${USER_TOPICS}}))`;
const PARTITIONS = `sum(kafka_topic_partitions{${USER_TOPICS}})`;
const GROUP_LAG = (f = GF) => `sum by (consumergroup) (clamp_min(kafka_consumergroup_lag{${f}}, 0))`;
const GROUP_MEMBERS = (f = GF) => `max by (consumergroup) (kafka_consumergroup_members{${f}})`;
const GROUP_RATE = (f = GF) => `sum by (consumergroup) (rate(kafka_consumergroup_current_offset{${f}}[5m]))`;
const GROUPS = "count(count by (consumergroup) (kafka_consumergroup_members))";
const TOTAL_LAG = "sum(clamp_min(kafka_consumergroup_lag, 0)) or on() vector(0)";
const LAG_COUNT = steps(["green"], ["orange", 1000], ["red", 10000]);

// ---- JVM and process (JMX exporter jobs) ------------------------------------------------------------------------------
const HEAP_PCT = (job) => `max(100 * sum by (instance) (jvm_memory_used_bytes{area="heap",job="${job}"}) / sum by (instance) (jvm_memory_max_bytes{area="heap",job="${job}"}))`;
const GC_PCT = (job) => `max(100 * sum by (instance) (rate(jvm_gc_collection_seconds_sum{job="${job}"}[5m])))`;
const CPU = (job) => `max(rate(process_cpu_seconds_total{job="${job}"}[5m]))`;

const ALERT_TEXT = {
  KafkaBrokerDown: "Kafka broker not reachable", KafkaNoActiveController: "No single active KRaft controller", KafkaOfflinePartitions: "Offline partitions",
  KafkaUnderMinIsrPartitions: "Partitions under min ISR", KafkaUnderReplicatedPartitions: "Under-replicated partitions",
  KafkaRequestHandlerSaturated: "Broker request handlers saturated", KafkaProduceLatencyHigh: "Produce p99 latency above 1 s", JvmHeapHigh: "JVM heap above 90%",
  ConnectWorkerDown: "Kafka Connect worker down", ConnectorFailed: "Connector FAILED", ConnectorTaskFailed: "Connector task FAILED",
  ConnectorTasksNotRunning: "Connector tasks not all running", ConnectorPaused: "Connector paused for 30 min", ConnectorRecordErrors: "Connector record errors",
  ConnectorProducerErrors: "Connector cannot write to Kafka", ConnectStartupFailures: "Connector / task startup failure", ConnectRebalanceStuck: "Connect rebalance stuck",
  DebeziumDisconnected: "Debezium lost the source DB", DebeziumSourceLagHigh: "CDC lag above 30 s", DebeziumSourceLagCritical: "CDC lag above 5 min",
  DebeziumQueueSaturated: "Debezium queue almost full", DebeziumErrors: "Debezium capture errors",
  ConsumerGroupLagHigh: "Consumer group lag above 10k", ConsumerGroupLagGrowing: "Consumer group falling behind", ConsumerGroupInactive: "Consumer group has no members",
  ScrapeTargetDown: "A metrics target is down",
  ConnectGuardianDown: "Auto-recovery (connect-guardian) is down", ConnectorNeedsAttention: "Connector cannot recover by itself",
  ConnectorRestartLoop: "Connector keeps failing after restarts", ConnectorOffsetsReset: "Connector lost its position and was reset",
  ConnectorCaptureStalled: "Connector capture stalled and was restarted",
  HostDiskSpaceLow: "Data disk above 75%", HostDiskSpaceCritical: "Data disk above 85%", HostMemoryLow: "Host memory below 10% available",
  HostCpuSaturated: "Host CPU above 90%",
};

// ---- shared tables -------------------------------------------------------------------------------------------------
const connectorFleet = (b, pos) => b.table("Connector fleet (Kafka Connect)", pos, {
  by: "connector", sort: "Health",
  desc: "Every connector on the Connect cluster. Health is DEGRADED when the connector runs but a task failed or is missing: a connector can be RUNNING while its tasks are FAILED.",
  links: viewDetails("/d/cdc-connectors"),
  queries: [
    { expr: HEALTH() },
    { expr: STATE() },
    { expr: `max by (connector, type) (kafka_connect_connector_type{${CF}})`, hide: true },
    { expr: `max by (connector, class) (kafka_connect_connector_class{${CF}})`, hide: true },
    { expr: `max by (connector, version) (kafka_connect_connector_version{${CF}})`, hide: true },
    { expr: tasks("total") },
    { expr: tasks("running") },
    { expr: tasks("failed") },
    { expr: RECORDS_RATE() },
    { expr: RECORD_ERRORS() },
    { expr: `max by (connector) (kafka_connect_task_offset_commit_failure_percentage{${CF}})` },
  ],
  columns: [
    { field: "connector", name: "Connector" },
    { field: "Value #A", name: "Health", mappings: HEALTH_MAP, cell: "bg", width: 100, align: "center" },
    { field: "Value #B", name: "State", mappings: STATE_MAP, cell: "text", width: 110 },
    { field: "type", name: "Type", width: 70 },
    { field: "class", name: "Plugin", mappings: [{ type: "regex", options: { pattern: ".*\\.([A-Za-z0-9]+)$", result: { text: "$1" } } }] },
    { field: "version", name: "Version", width: 110 },
    { field: "Value #F", name: "Tasks", decimals: 0, width: 64 },
    { field: "Value #G", name: "Running", decimals: 0, width: 74 },
    { field: "Value #H", name: "Failed", decimals: 0, thresholds: ZERO_GOOD, cell: "text", width: 64 },
    { field: "Value #I", name: "Records/s", unit: "short", decimals: 2, width: 90 },
    { field: "Value #J", name: "Errors (1h)", decimals: 0, thresholds: ZERO_GOOD, cell: "text", width: 90 },
    { field: "Value #K", name: "Commit fail", unit: "percentunit", decimals: 1, thresholds: steps(["green"], ["red", 0.01]), cell: "text", width: 90 },
  ],
});

const engineTable = (b, pos) => b.table("Debezium capture engines (by logical server = topic.prefix)", pos, {
  by: "name", sort: "Source lag",
  desc: "Debezium's own streaming metrics, one row per logical server. A connector whose task is FAILED has no engine row. The Connector column is filled when the connector sets custom.metric.tags=connector=<name>.",
  links: viewDetails("/d/cdc-connectors"),
  queries: [
    { expr: CONNECTED() },
    { expr: `max by (name, plugin) (${dbz("connected")})`, hide: true },
    { expr: `max by (name, connector) (debezium_metrics_connected{context="streaming",${SF},connector!=""})`, hide: true },
    { expr: LAG_S() },
    { expr: SINCE_EVENT() },
    { expr: EVENTS_RATE() },
    { expr: QUEUE_PCT() },
    { expr: SNAPSHOT() },
    { expr: DBZ_ERRORS() },
  ],
  columns: [
    { field: "name", name: "Server" },
    { field: "connector", name: "Connector" },
    { field: "plugin", name: "Plugin", width: 90 },
    { field: "Value #A", name: "Source DB", mappings: [valueMap([[1, "CONNECTED", "green"], [0, "DISCONNECTED", "red"]])], cell: "bg", width: 120, align: "center" },
    { field: "Value #D", name: "Source lag", unit: "s", decimals: 1, thresholds: LAG_THRESHOLDS, cell: "text", width: 90 },
    { field: "Value #E", name: "Last event", unit: "dtdurationms", width: 150 },
    { field: "Value #F", name: "Events/s", unit: "short", decimals: 2, width: 80 },
    { field: "Value #G", name: "Queue used", unit: "percent", decimals: 0, thresholds: steps(["green"], ["orange", 50], ["red", 90]), cell: "text", width: 90 },
    { field: "Value #H", name: "Snapshot", mappings: SNAPSHOT_MAP, cell: "text", width: 100 },
    { field: "Value #I", name: "Errors (1h)", decimals: 0, thresholds: ZERO_GOOD, cell: "text", width: 90 },
  ],
});

// =====================================================================================================================
// Dashboard 1: Kafka Stack Command Center (the entry point: the whole stack on one screen)
// =====================================================================================================================
// Host and container resources (node-exporter job "node", cAdvisor job "containers")
const HOST_CPU = '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))';
// Available memory counts the page cache as free: the kernel gives it back on demand, and Kafka depends on it
const HOST_RAM = "100 * (1 - max(node_memory_MemAvailable_bytes) / max(node_memory_MemTotal_bytes))";
// The filesystem that holds the Docker volumes (Kafka data): "/" unless /var or /var/lib/docker is mounted separately
const DATA_FS = 'mountpoint=~"/|/var|/var/lib|/var/lib/docker",fstype!~"tmpfs|overlay|squashfs"';
const HOST_DISK = `max(100 * (1 - node_filesystem_avail_bytes{${DATA_FS}} / node_filesystem_size_bytes{${DATA_FS}}))`;
// Stack containers: the fixed names of the core services plus the compose-named monitoring ones (cdc-<service>-1)
const STACK = 'name=~"kafka|connect|connect-guardian|kafka-ui|cdc-.+"';
const shortName = (e) => `label_replace(${e}, "name", "$1", "name", "cdc-(.+)-[0-9]+")`;
const CONTAINER_MEM = shortName(`100 * max by (name) (container_memory_working_set_bytes{${STACK}}) / max by (name) (container_spec_memory_limit_bytes{${STACK}} > 0)`);

// The pipeline: captured by Debezium -> written to Kafka -> waiting for consumers -> consumed
const CAPTURED = `sum(rate(${dbz("totalnumberofeventsseen", ALL_SERVERS)}[5m])) or on() vector(0)`;
const WRITTEN = "sum(rate(kafka_connect_source_task_source_record_write_total[5m])) or on() vector(0)";
const ALL_LAG = GROUP_LAG('consumergroup=~".+"'), ALL_RATE = GROUP_RATE('consumergroup=~".+"');
const CONSUMED = `sum(${ALL_RATE}) or on() vector(0)`;
// Worst group: lag / consume rate. A group with lag and no consumption has no finite catch-up time (-2: STALLED).
const CATCH_UP = `max(${ALL_LAG} / (${ALL_RATE} > 0)) or (max(${ALL_LAG} > 0) * 0 - 2) or on() vector(0)`;

// connect-guardian: automatic recovery per connector (see ../../guardian/README.md)
const GUARD = (m, f = CF) => `max by (connector) (connect_guardian_${m}{${f}})`;
const GUARD_MAP = [valueMap([[0, "WATCHING", "green"], [1, "RECOVERING", "orange"], [2, "NEEDS A PERSON", "red"], [3, "PARKED", "blue"], [4, "OFF", "gray"]])];
const guardActions = (action, w = "24h") => `sum(increase(connect_guardian_actions_total{action="${action}"}[${w}])) or on() vector(0)`;

const PLATFORM_OK = `(${[`clamp_max(${BROKERS}, 1)`, `${ACTIVE_CTRL} == bool 1`, `(${OFFLINE}) == bool 0`, `(${UNDER_MIN_ISR}) == bool 0`, 'clamp_max(max(up{job="connect-jmx"}), 1)', `(${CONNECTORS_BAD}) == bool 0`, `(${ENGINES_DOWN}) == bool 0`].map(check).join(" * ")})`;

const cc = builder();
{
  const b = cc;
  b.stat("", PLATFORM_OK, [0, 0, 24, 2], {
    valueSize: 24,
    mappings: [valueMap([[1, "✔  All Systems Healthy   |   Kafka, Kafka Connect and every Debezium connector are running normally", "green"], [0, "✖  Attention Needed   |   a broker, connector, task or capture engine is down or degraded: see the walls below", "red"]])],
    thresholds: steps(["red"], ["green", 1]),
    desc: "Healthy only when the broker is up with one active controller and no offline or under-min-ISR partitions, the Connect worker is up, no connector is DEGRADED or FAILED, and every Debezium engine is connected to its database. Platform-wide (ignores the Connector filter).",
  });

  // 1. Resources: is the box itself in trouble?
  let y = 2;
  const R = 5;
  b.gauge("Host CPU", HOST_CPU, [0, y, 3, R], { decimals: 0, desc: "All cores of the host, busy share over 5 minutes" });
  b.gauge("Host RAM", HOST_RAM, [3, y, 3, R], { decimals: 0, thresholds: steps(["green"], ["orange", 85], ["red", 95]), desc: "Memory in use, not counting the page cache (which the kernel frees on demand and Kafka uses to serve reads)" });
  b.gauge("Data disk", HOST_DISK, [6, y, 3, R], { decimals: 0, thresholds: steps(["green"], ["orange", 75], ["red", 85]), desc: "Filesystem holding the Docker volumes (Kafka logs). Kafka stops when it is full: act at 75%." });
  b.gauge("Kafka heap", HEAP_PCT("kafka-jmx"), [9, y, 3, R], { decimals: 0, desc: "Broker JVM heap used of max (1 GB). Messages are in the page cache, not here." });
  b.gauge("Connect heap", HEAP_PCT("connect-jmx"), [12, y, 3, R], { decimals: 0, desc: "Kafka Connect + Debezium heap used of max. Debezium buffers open Oracle transactions here; a sawtooth up to 80% is normal G1 behaviour." });
  b.bars("Container memory (used of limit)", CONTAINER_MEM, "{{name}}", [15, y, 9, R], { thresholds: steps(["green"], ["orange", 85], ["red", 95]), noValue: "No container names (cAdvisor on Docker Desktop)", desc: "Working set of each stack container as a share of its mem_limit in docker-compose.yml. At 100% the container is killed (OOM)." });
  y += R;

  // 2. Pipeline flow: captured -> written -> waiting -> consumed, and how far behind
  const P = 4;
  b.tile([0, y, 4, P], { icon: "database", iconColor: COL.purple, label: "Captured from DB",
    main: { expr: CAPTURED, unit: "suffix: ev/s", decimals: 1, thresholds: steps(["green"]) },
    sub: { expr: `sum(increase(${dbz("totalnumberofeventsseen", ALL_SERVERS)}[1h])) or on() vector(0)`, unit: "suffix: events in 1 h", decimals: 0 },
    desc: "Change events Debezium read from the source databases (all engines)", links: viewDetails("/d/cdc-connectors") });
  b.tile([4, y, 4, P], { icon: "arrow-right", iconColor: COL.blue, label: "Written to Kafka",
    main: { expr: WRITTEN, unit: "suffix: rec/s", decimals: 1, thresholds: steps(["green"]) },
    sub: { expr: MSGS_IN, unit: "suffix: msg/s into the broker", decimals: 1 },
    desc: "Records the source connectors wrote to Kafka, and all messages produced to the broker by any client", links: viewDetails("/d/kafka-cluster") });
  b.tile([8, y, 4, P], { icon: "inbox", iconColor: COL.orange, label: "Waiting in queue",
    main: { expr: TOTAL_LAG, unit: "suffix: msgs", decimals: 0, thresholds: LAG_COUNT, mappings: [valueMap([[0, "EMPTY", "green"]])] },
    sub: { expr: `count(${ALL_LAG} > 1000) or on() vector(0)`, unit: "suffix: groups over 1000 behind", decimals: 0 },
    desc: "Messages in Kafka that consumer groups have not processed yet (total consumer lag)", links: viewDetails("/d/kafka-topics") });
  b.tile([12, y, 4, P], { icon: "check-circle", iconColor: COL.green, label: "Consumed",
    main: { expr: CONSUMED, unit: "suffix: msg/s", decimals: 1, thresholds: steps(["green"]) },
    sub: { expr: `count(${GROUP_MEMBERS('consumergroup=~".+"')} > 0) or on() vector(0)`, unit: "suffix: active consumer groups", decimals: 0 },
    desc: "Messages processed (offsets committed) by all consumer groups", links: viewDetails("/d/kafka-topics") });
  b.tile([16, y, 4, P], { icon: "bolt", iconColor: COL.yellow, label: "CDC latency",
    main: { expr: MAX_LAG_S, unit: "s", decimals: 1, thresholds: LAG_THRESHOLDS, noneText: "IDLE" },
    subText: "DB commit → Debezium (worst engine)",
    desc: "MilliSecondsBehindSource of the slowest Debezium engine. IDLE: no change captured since the engine started." });
  b.tile([20, y, 4, P], { icon: "clock", iconColor: COL.teal, label: "Catch-up time",
    main: { expr: CATCH_UP, unit: "s", decimals: 0, thresholds: steps(["green"], ["orange", 60], ["red", 600]), mappings: [valueMap([[0, "IN SYNC", "green"], [-2, "STALLED", "red"]])] },
    subText: "worst consumer group at its current rate",
    desc: "Lag / consume rate of the worst consumer group. STALLED: messages are waiting and the group consumes nothing." });
  y += P;

  // 3. Connectors and tasks: the most important part
  const W = 4;
  b.wall("Connectors", HEALTH(), "{{connector}}", [0, y, 10, W], { mappings: HEALTH_MAP, noValue: "NO CONNECTORS", links: viewDetails("/d/cdc-connectors"),
    desc: "OK, PAUSED (paused or stopped on purpose), DEGRADED (running with a failed or missing task) or FAILED. A connector can be RUNNING while all its tasks have FAILED: this wall shows that as DEGRADED." });
  b.wall("Tasks", TASK_STATE, "{{connector}} · task {{task}}", [10, y, 8, W], { mappings: TASK_MAP, noValue: "NO TASKS", links: viewDetails("/d/cdc-connectors"),
    desc: "Every task of every connector. Tasks do the actual work: a FAILED task captures nothing." });
  b.list("Auto-recovery (24 h)", [
    { name: "Guardian", expr: 'max(connect_guardian_connect_up) * clamp_max(max(up{job="connect-guardian"}), 1)', mappings: [valueMap([[1, "ACTIVE", "green"], [0, "DOWN", "red"]])], colorByValue: true, noneText: "NOT DEPLOYED" },
    { name: "Task restarts", expr: guardActions("restart"), decimals: 0, thresholds: steps(["green"], ["orange", 1]) },
    { name: "Stall restarts", expr: guardActions("restart_stalled"), decimals: 0, thresholds: steps(["green"], ["orange", 1]) },
    { name: "Offset resets", expr: guardActions("reset"), decimals: 0, thresholds: steps(["green"], ["red", 1]) },
  ], [18, y, 6, W], { titleSize: 12, valueSize: 13, desc: "What connect-guardian did: restarts of failed and stalled tasks, and offset resets of connectors whose source position was lost (for example purged archive logs)." });
  y += W;

  b.table("Connector details", [0, y, 24, 6], {
    by: "connector", sort: "Health",
    desc: "One row per connector. Auto-recovery is connect-guardian's view: WATCHING (healthy), RECOVERING (restarting with backoff, or just reset), NEEDS A PERSON (permanent error such as a bad password or missing privilege), PARKED (paused or stopped by an operator).",
    links: viewDetails("/d/cdc-connectors"),
    queries: [
      { expr: HEALTH() },
      { expr: STATE() },
      { expr: tasks("running") },
      { expr: tasks("total") },
      { expr: RECORDS_RATE() },
      { expr: GUARD("connector_condition") },
      { expr: GUARD("consecutive_restarts") },
      { expr: `${GUARD("last_reset_timestamp_seconds")} * 1000` },
      { expr: RECORD_ERRORS() },
    ],
    columns: [
      { field: "connector", name: "Connector" },
      { field: "Value #A", name: "Health", mappings: HEALTH_MAP, cell: "bg", width: 100, align: "center" },
      { field: "Value #B", name: "State", mappings: STATE_MAP, cell: "text", width: 110 },
      { field: "Value #C", name: "Tasks running", decimals: 0, width: 110 },
      { field: "Value #D", name: "of", decimals: 0, width: 50 },
      { field: "Value #E", name: "Records/s", unit: "short", decimals: 2, width: 100 },
      { field: "Value #F", name: "Auto-recovery", mappings: GUARD_MAP, cell: "bg", width: 140, align: "center" },
      { field: "Value #G", name: "Restarts", decimals: 0, thresholds: steps(["green"], ["orange", 1], ["red", 5]), cell: "text", width: 80 },
      { field: "Value #H", name: "Last offset reset", unit: "dateTimeFromNow", width: 140 },
      { field: "Value #I", name: "Errors (1h)", decimals: 0, thresholds: ZERO_GOOD, cell: "text", width: 90 },
    ],
  });
  y += 6;

  // 4. Trends: flow, lag, latency, backpressure
  const C = 7;
  b.ts("Pipeline flow (/s)", [target(CAPTURED, "captured from DB"), target(WRITTEN, "written to Kafka"), target(CONSUMED, "consumed")], [0, y, 6, C],
    { unit: "short", colors: [["captured from DB", COL.purple], ["written to Kafka", COL.blue], ["consumed", COL.green]], desc: "The three stages should move together. Written but not consumed: consumers are behind. Captured high but written low: Kafka or the Connect producer is the bottleneck." });
  b.ts("Waiting in queue by consumer group (msgs)", [target(`topk(8, ${ALL_LAG})`, "{{consumergroup}}")], [6, y, 6, C], { legend: true, thresholds: steps(["transparent"], ["orange", 1000], ["red", 10000]) });
  b.ts("CDC latency by engine (s)", [target(LAG_S(ALL_SERVERS), "{{name}}")], [12, y, 6, C], { unit: "s", legend: true, thresholds: steps(["transparent"], ["orange", 10], ["red", 30]), desc: "Seconds between a commit in the source database and Debezium processing it. Dashed lines at 10 s and 30 s." });
  b.ts("Backpressure: Debezium queue used (%)", [target(QUEUE_PCT(ALL_SERVERS), "{{name}}")], [18, y, 6, C], { unit: "percent", max: 100, legend: true, thresholds: steps(["transparent"], ["orange", 50], ["red", 90]), desc: "Debezium's buffer between the database reader and Kafka. Near 100%: Kafka cannot take records as fast as they are captured, and capture slows down." });
  y += C;

  // 5. Kafka health, resources over time and alerts
  const K = 8;
  b.list("Kafka broker", [
    { name: "Brokers online", expr: BROKERS, decimals: 0, thresholds: steps(["red"], ["green", 1]) },
    { name: "Active controller", expr: ACTIVE_CTRL, mappings: CTRL_MAP, colorByValue: true },
    { name: "Offline partitions", expr: OFFLINE, decimals: 0, thresholds: ZERO_GOOD },
    { name: "Under min ISR", expr: UNDER_MIN_ISR, decimals: 0, thresholds: ZERO_GOOD },
    { name: "Produce p99", expr: PRODUCE_P99, unit: "ms", decimals: 0, thresholds: steps(["green"], ["orange", 500], ["red", 1000]) },
    { name: "Handlers idle", expr: REQ_IDLE, unit: "percent", decimals: 0, thresholds: steps(["red"], ["orange", 20], ["green", 40]) },
    { name: "Data on disk", expr: "sum(kafka_log_size)", unit: "bytes", decimals: 1, color: COL.label },
  ], [0, y, 6, K], { links: viewDetails("/d/kafka-cluster"), titleSize: 12, valueSize: 13 });
  b.ts("Resources over time (%)", [target(HOST_CPU, "host CPU"), target(HOST_RAM, "host RAM"), target(HOST_DISK, "data disk"), target(HEAP_PCT("kafka-jmx"), "Kafka heap"), target(HEAP_PCT("connect-jmx"), "Connect heap")], [6, y, 8, K],
    { unit: "percent", max: 100, fill: 0, legend: true, colors: [["host CPU", COL.blue], ["host RAM", COL.purple], ["data disk", COL.orange], ["Kafka heap", COL.teal], ["Connect heap", COL.yellow]] });
  b.alertHistory("Alerts (24 h)", [14, y, 10, K], ALERT_TEXT);
}

// =====================================================================================================================
// Dashboard 2: Debezium Connectors (connector and engine drill-down)
// =====================================================================================================================
const con = builder();
{
  const b = con;
  let y = 0;
  b.row("Connector fleet", y++);
  const H_ = HEALTH();
  b.stat("Connectors", `count(${H_})`, [0, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Healthy", `count((${H_}) == 0) or on() vector(0)`, [3, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Degraded", `count((${H_}) == 2) or on() vector(0)`, [6, y, 3, 4], { decimals: 0, thresholds: steps(["green"], ["orange", 1]) });
  b.stat("Failed", `count((${H_}) == 3) or on() vector(0)`, [9, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Paused / Stopped", `count((${H_}) == 1) or on() vector(0)`, [12, y, 3, 4], { decimals: 0, thresholds: steps(["green"], ["blue", 1]) });
  b.stat("Tasks running", `sum(${tasks("running")})`, [15, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Tasks failed", `sum(${tasks("failed")}) or on() vector(0)`, [18, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Connect workers", 'count(up{job="connect-jmx"} == 1) or on() vector(0)', [21, y, 3, 4], { decimals: 0, thresholds: steps(["red"], ["green", 1]) });
  y += 4;
  connectorFleet(b, [0, y, 24, 8]);
  y += 8;
  b.timeline("Connector health over time", [target(HEALTH(), "{{connector}}")], [0, y, 12, 7], { mappings: HEALTH_MAP, desc: "Flapping between OK and DEGRADED means a task keeps failing and being restarted (for example by a watchdog)." });
  b.timeline("Task state over time", [target(TASK_STATE, "{{connector}} / task {{task}}")], [12, y, 12, 7], { mappings: TASK_MAP });
  y += 7;

  b.row("Debezium streaming (per logical server)", y++);
  engineTable(b, [0, y, 24, 7]);
  y += 7;
  b.ts("Source lag (seconds behind the source database)", [target(LAG_S(), "{{name}}")], [0, y, 8, 7], { unit: "s", thresholds: steps(["transparent"], ["orange", 10], ["red", 30]), desc: "MilliSecondsBehindSource: commit time in the source vs processing time in Debezium. Dashed lines at 10 s and 30 s." });
  b.ts("Time since last event", [target(SINCE_EVENT(), "{{name}}")], [8, y, 8, 7], { unit: "ms", desc: "Grows while the source tables do not change. Only a concern when changes are expected." });
  b.ts("Change events captured (/s)", [target(EVENTS_RATE(), "{{name}}")], [16, y, 8, 7], { unit: "short" });
  y += 7;
  b.ts("Events by operation (/s)", [
    target(`sum(rate(${dbz("totalnumberofcreateeventsseen")}[5m]))`, "insert"),
    target(`sum(rate(${dbz("totalnumberofupdateeventsseen")}[5m]))`, "update"),
    target(`sum(rate(${dbz("totalnumberofdeleteeventsseen")}[5m]))`, "delete"),
    target(`sum(rate(${dbz("numberofeventsfiltered")}[5m]))`, "filtered"),
  ], [0, y, 8, 7], { unit: "short", stack: true, colors: [["insert", COL.green], ["update", COL.blue], ["delete", COL.red], ["filtered", COL.label]] });
  b.ts("Internal queue used (%)", [target(QUEUE_PCT(), "{{name}}")], [8, y, 8, 7], { unit: "percent", max: 100, thresholds: steps(["transparent"], ["orange", 50], ["red", 90]), desc: "Debezium's queue between the database reader and Kafka. Near 100% means Kafka (or the Connect producer) is the bottleneck." });
  b.ts("Capture errors and erroneous events (per 5 min)", [
    target(`sum by (name) (increase(${dbz("numberoferroneousevents")}[5m]))`, "{{name}} erroneous events"),
    target(`sum by (name) (increase(${dbz("errorcount")}[5m]))`, "{{name}} errors"),
    target(`sum by (name) (increase(${dbz("warningcount")}[5m]))`, "{{name}} warnings"),
  ], [16, y, 8, 7], { bars: true, fill: 80 });
  y += 7;
  b.ts("Committed transactions (/s)", [target(`sum by (name) (rate(${dbz("numberofcommittedtransactions")}[5m]))`, "{{name}}")], [0, y, 8, 6], { unit: "short" });
  b.stat("Engines connected", `count(${CONNECTED()} == 1) or on() vector(0)`, [8, y, 4, 6], { colorMode: "value", decimals: 0 });
  b.stat("Engines disconnected", `count(${CONNECTED()} == 0) or on() vector(0)`, [12, y, 4, 6], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Worst source lag", `max(${LAG_S()})`, [16, y, 4, 6], { unit: "s", decimals: 1, thresholds: LAG_THRESHOLDS, noValue: "IDLE" });
  b.stat("Events captured (1h)", `sum(increase(${dbz("totalnumberofeventsseen")}[1h])) or on() vector(0)`, [20, y, 4, 6], { colorMode: "value", decimals: 0, unit: "short" });
  y += 6;

  b.row("Snapshot", y++);
  b.timeline("Snapshot state", [target(SNAPSHOT(), "{{name}}")], [0, y, 8, 6], { mappings: SNAPSHOT_MAP });
  b.ts("Tables remaining in snapshot", [target(`max by (name) (${dbz("remainingtablecount", SF, "snapshot")})`, "{{name}} remaining"), target(`max by (name) (${dbz("totaltablecount", SF, "snapshot")})`, "{{name}} total")], [8, y, 8, 6], { decimals: 0 });
  b.ts("Snapshot duration", [target(`max by (name) (${dbz("snapshotdurationinseconds", SF, "snapshot")})`, "{{name}}")], [16, y, 8, 6], { unit: "s" });
  y += 6;

  b.row("Oracle LogMiner (Oracle connectors only; other plugins show no data)", y++);
  b.ts("Active transactions in buffer", [target(`max by (name) (${dbz("numberofactivetransactions")})`, "{{name}}")], [0, y, 6, 7], { decimals: 0, desc: "Long-running uncommitted transactions held in Debezium's buffer" });
  b.ts("LogMiner batch processing time", [target(`max by (name) (${dbz("lastbatchprocessingtimeinmilliseconds")})`, "{{name}}")], [6, y, 6, 7], { unit: "ms" });
  b.ts("LogMiner fetch query duration", [target(`max by (name) (${dbz("lastdurationoffetchqueryinmilliseconds")})`, "{{name}}")], [12, y, 6, 7], { unit: "ms" });
  b.ts("SCN gap (current - offset)", [target(`max by (name) (${dbz("currentscn")}) - max by (name) (${dbz("offsetscn")})`, "{{name}}")], [18, y, 6, 7], { decimals: 0, desc: "How far the connector's resume position is behind the database's current SCN" });
  y += 7;

  b.row("Kafka Connect task performance", y++);
  b.ts("Records polled from source (/s)", [target(`sum by (connector) (rate(kafka_connect_source_task_source_record_poll_total{${CF}}[5m]))`, "{{connector}}")], [0, y, 8, 7], { unit: "short" });
  b.ts("Records written to Kafka / delivered by sinks (/s)", [target(RECORDS_RATE(), "{{connector}}")], [8, y, 8, 7], { unit: "short" });
  b.ts("In-flight records (polled, not yet acknowledged)", [target(`sum by (connector) (kafka_connect_source_task_source_record_active_count{${CF}})`, "{{connector}}")], [16, y, 8, 7], { decimals: 0, desc: "Grows when Kafka acknowledges slower than the connector polls" });
  y += 7;
  b.ts("Poll batch time (avg)", [target(`max by (connector) (kafka_connect_source_task_poll_batch_avg_time_ms{${CF}})`, "{{connector}}")], [0, y, 6, 7], { unit: "ms" });
  b.ts("Offset commit time (avg)", [target(`max by (connector) (kafka_connect_task_offset_commit_avg_time_ms{${CF}})`, "{{connector}}")], [6, y, 6, 7], { unit: "ms" });
  b.ts("Producer request latency (avg)", [target(`max by (connector) (kafka_connect_producer_request_latency_avg{${CF}})`, "{{connector}}")], [12, y, 6, 7], { unit: "ms" });
  b.ts("Task running ratio", [target(`min by (connector) (kafka_connect_task_running_ratio{${CF}})`, "{{connector}}")], [18, y, 6, 7], { unit: "percentunit", max: 1, desc: "Share of time the tasks spent RUNNING (1 = always)" });
  y += 7;

  b.row("Errors, retries and dead letter queue", y++);
  b.ts("Record errors (per 5 min)", [target(`sum by (connector) (increase(kafka_connect_task_error_total_record_errors_total{${CF}}[5m]))`, "{{connector}} errors"), target(`sum by (connector) (increase(kafka_connect_task_error_total_records_skipped_total{${CF}}[5m]))`, "{{connector}} skipped")], [0, y, 8, 7], { bars: true, fill: 80, desc: "Conversion / transformation errors. Skipped records only happen with errors.tolerance=all." });
  b.ts("Producer send errors and retries (/s)", [target(`sum by (connector) (rate(kafka_connect_producer_record_error_total{${CF}}[5m]))`, "{{connector}} errors"), target(`sum by (connector) (rate(kafka_connect_producer_record_retry_total{${CF}}[5m]))`, "{{connector}} retries")], [8, y, 8, 7], { unit: "short", desc: "Errors here mean records could not be written to Kafka" });
  b.ts("Dead letter queue (per 5 min)", [target(`sum by (connector) (increase(kafka_connect_task_error_deadletterqueue_produce_requests_total{${CF}}[5m]))`, "{{connector}} sent to DLQ"), target(`sum by (connector) (increase(kafka_connect_task_error_deadletterqueue_produce_failures_total{${CF}}[5m]))`, "{{connector}} DLQ failures")], [16, y, 8, 7], { bars: true, fill: 80, desc: "Only for connectors with errors.deadletterqueue.topic.name (sink connectors)" });
  y += 7;

  b.row("Connect workers", y++);
  b.stat("Rebalancing", "max(kafka_connect_worker_rebalance_rebalancing)", [0, y, 4, 4], { mappings: [valueMap([[0, "STABLE", "green"], [1, "REBALANCING", "orange"]])], thresholds: steps(["green"], ["orange", 1]) });
  b.stat("Rebalances (24h)", "sum(increase(kafka_connect_worker_rebalance_completed_rebalances_total[24h])) or on() vector(0)", [4, y, 4, 4], { decimals: 0, colorMode: "value" });
  b.stat("Connector startup failures (24h)", "sum(increase(kafka_connect_worker_connector_startup_failure_total[24h])) or on() vector(0)", [8, y, 4, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Task startup failures (24h)", "sum(increase(kafka_connect_worker_task_startup_failure_total[24h])) or on() vector(0)", [12, y, 4, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Connect heap", HEAP_PCT("connect-jmx"), [16, y, 4, 4], { unit: "percent", decimals: 0, thresholds: OK_WARN_CRIT });
  b.stat("Connect GC time", GC_PCT("connect-jmx"), [20, y, 4, 4], { unit: "percent", decimals: 1, thresholds: steps(["green"], ["orange", 5], ["red", 10]) });
  y += 4;
  b.ts("Connectors and tasks per worker", [target('sum by (instance) (kafka_connect_worker_connector_count)', "{{instance}} connectors"), target('sum by (instance) (kafka_connect_worker_task_count)', "{{instance}} tasks")], [0, y, 12, 7], { decimals: 0 });
  b.ts("Connect JVM heap", [target('sum by (instance) (jvm_memory_used_bytes{area="heap",job="connect-jmx"})', "{{instance}} used"), target('sum by (instance) (jvm_memory_max_bytes{area="heap",job="connect-jmx"})', "{{instance}} max")], [12, y, 12, 7], { unit: "bytes" });
}

// =====================================================================================================================
// Dashboard 3: Kafka Cluster (broker and KRaft)
// =====================================================================================================================
const kc = builder();
{
  const b = kc;
  let y = 0;
  b.row("Cluster health", y++);
  b.stat("Brokers online", BROKERS, [0, y, 3, 4], { decimals: 0, thresholds: steps(["red"], ["green", 1]) });
  b.stat("Active controller", ACTIVE_CTRL, [3, y, 3, 4], { mappings: CTRL_MAP, thresholds: steps(["red"], ["green", 1], ["red", 2]) });
  b.stat("Offline partitions", OFFLINE, [6, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Under-replicated", URP, [9, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Under min ISR", UNDER_MIN_ISR, [12, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD, desc: "Producers with acks=all fail on these partitions" });
  b.stat("Fenced brokers", "sum(kafka_controller_kafkacontroller_fencedbrokercount) or on() vector(0)", [15, y, 3, 4], { decimals: 0, thresholds: ZERO_GOOD });
  b.stat("Topics", "max(kafka_controller_kafkacontroller_globaltopiccount)", [18, y, 3, 4], { decimals: 0, colorMode: "value" });
  b.stat("Partitions", "max(kafka_controller_kafkacontroller_globalpartitioncount)", [21, y, 3, 4], { decimals: 0, colorMode: "value" });
  y += 4;

  b.row("Throughput", y++);
  b.ts("Bytes in / out", [target(BYTES_IN, "in"), target(BYTES_OUT, "out")], [0, y, 12, 7], { unit: "Bps", colors: [["in", COL.blue], ["out", COL.orange]] });
  b.ts("Messages in (/s)", [target(MSGS_IN, "all topics"), target(`topk(5, sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!="",${USER_TOPICS}}[5m])))`, "{{topic}}")], [12, y, 12, 7], { unit: "short" });
  y += 7;
  b.ts("Produce / fetch requests (/s)", [target(btm("totalproducerequests"), "produce"), target(btm("totalfetchrequests"), "fetch")], [0, y, 12, 7], { unit: "reqps", colors: [["produce", COL.blue], ["fetch", COL.orange]] });
  b.ts("Failed requests and request errors (/s)", [target(btm("failedproducerequests"), "failed produce"), target(btm("failedfetchrequests"), "failed fetch"), target('sum by (request, error) (rate(kafka_network_requestmetrics_errors_total{error!="NONE"}[5m])) > 0', "{{request}}: {{error}}")], [12, y, 12, 7], { unit: "reqps", desc: "Some errors are normal (e.g. UNKNOWN_TOPIC_OR_PARTITION while a topic is created); look for sustained rates." });
  y += 7;

  b.row("Latency and saturation", y++);
  b.ts("Request total time (p99 / mean)", [
    target('max by (request) (kafka_network_requestmetrics_totaltimems_p99{request=~"Produce|FetchConsumer"})', "{{request}} p99"),
    target('max by (request) (kafka_network_requestmetrics_totaltimems_mean{request=~"Produce|FetchConsumer"})', "{{request}} mean"),
  ], [0, y, 8, 7], { unit: "ms", desc: "FetchConsumer includes the consumer's fetch.max.wait.ms (500 ms by default), so a high value with no traffic is normal." });
  b.ts("Produce time breakdown (p99)", [
    target('max(kafka_network_requestmetrics_requestqueuetimems_p99{request="Produce"})', "request queue"),
    target('max(kafka_network_requestmetrics_localtimems_p99{request="Produce"})', "local (log append)"),
    target('max(kafka_network_requestmetrics_remotetimems_p99{request="Produce"})', "remote (replication)"),
    target('max(kafka_network_requestmetrics_responsesendtimems_p99{request="Produce"})', "response send"),
  ], [8, y, 8, 7], { unit: "ms", stack: true, desc: "Where produce time goes: queue = saturated handlers, local = disk, remote = followers (acks=all)" });
  b.ts("Thread pool idle (%)", [target(REQ_IDLE, "request handlers"), target(NET_IDLE, "network processors")], [16, y, 8, 7], { unit: "percent", max: 100, thresholds: steps(["red"], ["transparent", 20]), desc: "Below 20% idle the broker is saturated" });
  y += 7;
  b.ts("Request queue size", [target("max(kafka_network_requestchannel_requestqueuesize)", "request queue"), target("max(kafka_network_requestchannel_responsequeuesize)", "response queue")], [0, y, 8, 6], { decimals: 0 });
  b.ts("Client connections by listener", [target("sum by (listener) (kafka_server_socket_server_connection_count)", "{{listener}}")], [8, y, 8, 6], { decimals: 0, stack: true });
  b.ts("Requests (/s) by type", [target('topk(8, sum by (request) (rate(kafka_network_requestmetrics_requests_total[5m])))', "{{request}}")], [16, y, 8, 6], { unit: "reqps" });
  y += 6;

  b.row("Replication and KRaft metadata", y++);
  b.ts("ISR shrinks / expands (/s)", [target("sum(rate(kafka_server_replicamanager_isrshrinks_total[5m]))", "shrinks"), target("sum(rate(kafka_server_replicamanager_isrexpands_total[5m]))", "expands")], [0, y, 8, 7], { unit: "short", colors: [["shrinks", COL.red], ["expands", COL.green]], desc: "Frequent shrinks mean followers fall behind (only possible with more than one broker)" });
  b.ts("Partitions and leaders per broker", [target("sum by (instance) (kafka_server_replicamanager_partitioncount)", "{{instance}} partitions"), target("sum by (instance) (kafka_server_replicamanager_leadercount)", "{{instance}} leaders")], [8, y, 8, 7], { decimals: 0 });
  b.ts("Metadata apply lag and Raft commit latency", [target("max(kafka_controller_kafkacontroller_lastappliedrecordlagms)", "controller apply lag"), target("max(kafka_server_raft_metrics_commit_latency_avg)", "raft commit latency")], [16, y, 8, 7], { unit: "ms" });
  y += 7;

  b.row("Storage", y++);
  b.ts("Log size by topic (top 10)", [target(`topk(10, sum by (topic) (kafka_log_size{${USER_TOPICS}}))`, "{{topic}}")], [0, y, 12, 7], { unit: "bytes", legendTable: true });
  b.ts("Total log size and disk I/O", [target("sum(kafka_log_size)", "log size (all topics)"), target("sum(rate(kafka_server_disk_write_bytes_total[5m]))", "disk write/s"), target("sum(rate(kafka_server_disk_read_bytes_total[5m]))", "disk read/s")], [12, y, 12, 7], { unit: "bytes", desc: "Disk I/O is Linux only (reported by the broker from /proc)" });
  y += 7;

  b.row("JVM and container resources", y++);
  b.ts("Heap used of max (%)", [target(`${HEAP_PCT("kafka-jmx")}`, "broker"), target(`${HEAP_PCT("connect-jmx")}`, "connect")], [0, y, 8, 7], { unit: "percent", max: 100, thresholds: steps(["transparent"], ["orange", 80], ["red", 90]) });
  b.ts("Time in garbage collection (%)", [target(GC_PCT("kafka-jmx"), "broker"), target(GC_PCT("connect-jmx"), "connect")], [8, y, 8, 7], { unit: "percent" });
  b.ts("Process CPU (cores)", [target(CPU("kafka-jmx"), "broker"), target(CPU("connect-jmx"), "connect")], [16, y, 8, 7], { decimals: 2 });
  y += 7;
  b.ts("Container memory (cAdvisor)", [target('max by (name) (container_memory_working_set_bytes{name=~"kafka|connect|kafka-ui|kafka-exporter.*"})', "{{name}}")], [0, y, 12, 7], { unit: "bytes", desc: "Needs cAdvisor on a Linux Docker host (on Docker Desktop containers have no name label)" });
  b.ts("Container CPU (cores, cAdvisor)", [target('sum by (name) (rate(container_cpu_usage_seconds_total{name=~"kafka|connect|kafka-ui|kafka-exporter.*"}[5m]))', "{{name}}")], [12, y, 12, 7], { decimals: 2 });
}

// =====================================================================================================================
// Dashboard 4: Topics & Consumer Groups
// =====================================================================================================================
const tc = builder();
{
  const b = tc;
  let y = 0;
  b.row("Overview", y++);
  b.stat("Topics", TOPICS, [0, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Partitions", PARTITIONS, [3, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Messages in/s", MSGS_IN, [6, y, 4, 4], { colorMode: "value", unit: "short", decimals: 2 });
  b.stat("Consumer groups", GROUPS, [10, y, 3, 4], { colorMode: "value", decimals: 0 });
  b.stat("Total lag", TOTAL_LAG, [13, y, 4, 4], { decimals: 0, thresholds: LAG_COUNT, unit: "short" });
  b.stat("Groups lagging (>1000)", `count(${GROUP_LAG('consumergroup=~".+"')} > 1000) or on() vector(0)`, [17, y, 4, 4], { decimals: 0, thresholds: steps(["green"], ["orange", 1]) });
  b.stat("Inactive groups with lag", `count((${GROUP_MEMBERS('consumergroup=~".+"')} == 0) and on (consumergroup) (${GROUP_LAG('consumergroup=~".+"')} > 0)) or on() vector(0)`, [21, y, 3, 4], { decimals: 0, thresholds: steps(["green"], ["orange", 1]), desc: "Groups with unread messages and no running consumer" });
  y += 4;

  b.row("Topics", y++);
  b.table("Topics", [0, y, 24, 9], {
    by: "topic", sort: "Messages/s",
    queries: [
      { expr: `max by (topic) (kafka_topic_partitions{${TF}})` },
      { expr: `max by (topic) (kafka_topic_partition_replicas{${TF}})` },
      { expr: `sum by (topic) (kafka_topic_partition_under_replicated_partition{${TF}})` },
      { expr: `sum by (topic) (rate(kafka_topic_partition_current_offset{${TF}}[5m]))` },
      { expr: `sum by (topic) (kafka_topic_partition_current_offset{${TF}} - kafka_topic_partition_oldest_offset{${TF}})` },
      { expr: `sum by (topic) (kafka_log_size{${TF}})` },
      { expr: `count by (topic) (count by (topic, consumergroup) (kafka_consumergroup_lag{${TF}}))` },
      { expr: `sum by (topic) (clamp_min(kafka_consumergroup_lag{${TF}}, 0))` },
    ],
    columns: [
      { field: "topic", name: "Topic" },
      { field: "Value #A", name: "Partitions", decimals: 0, width: 90 },
      { field: "Value #B", name: "Replicas", decimals: 0, width: 80 },
      { field: "Value #C", name: "Under-replicated", decimals: 0, thresholds: ZERO_GOOD, cell: "text", width: 130 },
      { field: "Value #D", name: "Messages/s", unit: "short", decimals: 2, width: 100 },
      { field: "Value #E", name: "Retained messages", unit: "short", decimals: 0, width: 140 },
      { field: "Value #F", name: "Size", unit: "bytes", width: 100 },
      { field: "Value #G", name: "Consumer groups", decimals: 0, width: 130 },
      { field: "Value #H", name: "Consumer lag", decimals: 0, thresholds: LAG_COUNT, cell: "text", width: 110 },
    ],
  });
  y += 9;
  b.ts("Messages in by topic (/s, top 10)", [target(`topk(10, sum by (topic) (rate(kafka_topic_partition_current_offset{${TF}}[5m])))`, "{{topic}}")], [0, y, 12, 7], { unit: "short", legendTable: true });
  b.ts("Retained messages by topic (top 10)", [target(`topk(10, sum by (topic) (kafka_topic_partition_current_offset{${TF}} - kafka_topic_partition_oldest_offset{${TF}}))`, "{{topic}}")], [12, y, 12, 7], { unit: "short", legendTable: true });
  y += 7;

  b.row("Consumer groups", y++);
  const L = GROUP_LAG(), M = GROUP_MEMBERS(), R = GROUP_RATE();
  b.table("Consumer groups", [0, y, 24, 8], {
    by: "consumergroup", sort: "Lag",
    desc: "Catch-up time = lag / current consume rate. INACTIVE: messages are waiting but no consumer is in the group.",
    queries: [
      { expr: `clamp_max((${L} > bool 1000) + 2 * ((${M} == bool 0) * (${L} > bool 0)), 2)` },
      { expr: M },
      { expr: L },
      { expr: R },
      { expr: `${L} / (${R} > 0)` },
      { expr: `count by (consumergroup) (count by (consumergroup, topic) (kafka_consumergroup_lag{${GF}}))` },
    ],
    columns: [
      { field: "consumergroup", name: "Consumer group" },
      { field: "Value #A", name: "Status", mappings: [valueMap([[0, "OK", "green"], [1, "LAGGING", "orange"], [2, "INACTIVE", "red"]])], cell: "bg", width: 100, align: "center" },
      { field: "Value #B", name: "Members", decimals: 0, width: 90 },
      { field: "Value #C", name: "Lag", decimals: 0, thresholds: LAG_COUNT, cell: "text", width: 110 },
      { field: "Value #D", name: "Consume rate", unit: "short", decimals: 2, width: 120 },
      { field: "Value #E", name: "Catch-up time", unit: "s", decimals: 0, width: 120 },
      { field: "Value #F", name: "Topics", decimals: 0, width: 80 },
    ],
  });
  y += 8;
  b.ts("Lag by consumer group (messages)", [target(L, "{{consumergroup}}")], [0, y, 12, 7], { legendTable: true, thresholds: steps(["transparent"], ["orange", 1000], ["red", 10000]) });
  b.ts("Consume rate by group (/s)", [target(R, "{{consumergroup}}")], [12, y, 12, 7], { unit: "short", legendTable: true });
  y += 7;
  b.ts("Lag by group and topic (messages)", [target(`sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag{${GF},${TF}}, 0))`, "{{consumergroup}} / {{topic}}")], [0, y, 24, 7], { legendTable: true });
}

const write = (file, d) => writeFileSync(join(out, file), JSON.stringify(d, null, 2) + "\n");
write("kafka-command-center.json", dashboard("kafka-command-center", "Kafka Stack Command Center", "The whole Kafka stack on one screen: resources, pipeline flow, connectors and tasks, auto-recovery, lag, latency, backpressure and alerts", cc.panels, ["connector"]));
write("cdc-connectors.json", dashboard("cdc-connectors", "Debezium Connectors", "Connector and task state, Debezium streaming and snapshot metrics, task performance, errors and workers", con.panels, ["connector", "server"]));
write("kafka-cluster.json", dashboard("kafka-cluster", "Kafka Cluster", "Broker and KRaft health, throughput, request latency, saturation, replication, storage and JVM", kc.panels));
write("kafka-topics.json", dashboard("kafka-topics", "Topics & Consumer Groups", "Topic inventory and throughput, consumer group lag and catch-up time", tc.panels, ["topic", "group"]));
console.log("written to", out);
