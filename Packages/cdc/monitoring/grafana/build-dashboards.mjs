// Generates the Grafana dashboards in ./dashboards. Run: node build-dashboards.mjs
// Edit the panels here, regenerate, and commit both this file and the JSON. Grafana reloads the JSON every 30s.
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const out = join(dirname(fileURLToPath(import.meta.url)), "dashboards");
mkdirSync(out, { recursive: true });

const DS = { type: "prometheus", uid: "${ds}" };

// ---- palette (matches the reference design) ------------------------------------------------------------------------
const COL = { green: "#73BF69", blue: "#5794F2", orange: "#FF9830", red: "#F2495C", purple: "#B877D9", cyan: "#8AB8FF", teal: "#5AD7C9", label: "#CCCCDC", muted: "#8E8E9E" };

// ---- helpers -------------------------------------------------------------------------------------------------------
const steps = (...s) => ({ mode: "absolute", steps: s.map(([color, value]) => ({ color, value: value ?? null })) });
const OK_WARN_CRIT = steps(["green"], ["orange", 80], ["red", 90]);
const NO_DATA = { type: "special", options: { match: "null", result: { text: "NO DATA", color: "gray" } } };
// A missing series must never look green or red: every stat gets a grey NO DATA unless it defines its own
const withNoData = (m = []) => (m.some((x) => x.type === "special") ? m : [...m, NO_DATA]);
// With no series at all Grafana paints the threshold's base color, so a stat whose base is red or green would show
// "no data" as a verdict. Put a grey base underneath and move the original base to 0 (no stat here goes below 0).
const neutralBase = (t) => ({ ...t, steps: [{ color: "gray", value: null }, ...t.steps.map((s, i) => (i === 0 ? { ...s, value: 0 } : s))] });
const valueMap = (pairs) => ({ type: "value", options: Object.fromEntries(pairs.map(([v, text, color]) => [v, { text, color }])) });
const upDown = [valueMap([[1, "UP", "green"], [0, "DOWN", "red"]]), NO_DATA];
const target = (expr, legend, instant = false) => ({
  datasource: DS, expr, legendFormat: legend ?? "", refId: "A", instant, range: !instant,
});
const withRef = (targets) => targets.map((t, i) => ({ ...t, refId: String.fromCharCode(65 + i) }));
// Canvas elements need a field to exist; -1 stands for "no data" and is mapped to a grey text
const orNone = (expr) => `(${expr}) or on() vector(-1)`;
const noneMap = (text = "NO DATA") => valueMap([[-1, text, "gray"]]);
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
    row: (title, y) => add({ type: "row", title, collapsed: false, gridPos: { x: 0, y, w: 24, h: 1 }, panels: [], datasource: undefined }),

    stat: (title, expr, [x, y, w, h], o = {}) => add({
      type: "stat", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef([target(expr, "", true)]),
      fieldConfig: { defaults: { unit: o.unit ?? "none", decimals: o.decimals, thresholds: neutralBase(o.thresholds ?? steps(["green"])), mappings: withNoData(o.mappings), noValue: o.noValue ?? "NO DATA", color: { mode: "thresholds" } }, overrides: [] },
      options: { colorMode: o.colorMode ?? "background", graphMode: "none", textMode: "value", reduceOptions: { calcs: ["lastNotNull"] }, justifyMode: "center", ...(o.valueSize && { text: { valueSize: o.valueSize } }) },
    }),

    // Name / value rows, like the check lists of the reference design. rows: [{ name, expr, unit, decimals, mappings, thresholds, color }]
    list: (title, rows, [x, y, w, h], o = {}) => add({
      type: "stat", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef(rows.map((r) => target(orNone(r.expr), r.name, true))),
      fieldConfig: {
        defaults: { unit: "none", color: { mode: "fixed", fixedColor: COL.green }, mappings: [NO_DATA], noValue: "NO DATA", thresholds: steps(["green"]) },
        // Rows with thresholds or value mappings take their colour from the value; the rest are plain green text
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

    ts: (title, targets, [x, y, w, h], o = {}) => add({
      type: "timeseries", title, description: o.desc, gridPos: { x, y, w, h }, links: o.links,
      targets: withRef(targets),
      fieldConfig: {
        defaults: {
          unit: o.unit ?? "none", min: o.min ?? (o.softZero === false ? undefined : 0), max: o.max, decimals: o.decimals,
          color: o.color ? { mode: "fixed", fixedColor: o.color } : { mode: "palette-classic" },
          custom: { drawStyle: "line", lineInterpolation: "smooth", lineWidth: 2, fillOpacity: o.fill ?? 25, gradientMode: "opacity", showPoints: "never", spanNulls: true, axisBorderShow: false, axisPlacement: "auto", thresholdsStyle: { mode: o.thresholds ? "dashed" : "off" } },
          thresholds: o.thresholds ?? steps(["green"]),
        },
        overrides: (o.colors ?? []).map(([name, color]) => ({ matcher: { id: "byName", options: name }, properties: [{ id: "color", value: { mode: "fixed", fixedColor: color } }] })),
      },
      options: { legend: { displayMode: "list", placement: "bottom", showLegend: targets.length > 1 || !!o.legend }, tooltip: { mode: "multi", sort: "desc" } },
    }),

    bar: (title, expr, legend, [x, y, w, h], o = {}) => add({
      type: "bargauge", title, description: o.desc, gridPos: { x, y, w, h },
      targets: withRef([target(expr, legend, true)]),
      fieldConfig: { defaults: { unit: o.unit ?? "percent", min: 0, max: o.max ?? 100, thresholds: o.thresholds ?? OK_WARN_CRIT, color: { mode: "thresholds" } }, overrides: [] },
      options: { displayMode: "gradient", orientation: "horizontal", reduceOptions: { calcs: ["lastNotNull"] }, showUnfilled: true },
    }),

    // A KPI tile: icon on the left, label, big coloured value, small subtitle. Built on the Canvas panel.
    tile: ([x, y, w, h], t) => {
      const el = (type, name, config, placement, constraint = { horizontal: "leftright", vertical: "top" }) => ({
        type, name, config, constraint, placement: { rotation: 0, ...placement },
        background: { color: { fixed: "transparent" } }, border: { color: { fixed: "transparent" } },
      });
      // Narrow (3-column) tiles get a smaller icon and text so nothing wraps
      const g = w <= 3 ? { icon: 34, iconLeft: 10, textLeft: 52, label: 13, value: 26, sub: 11 } : { icon: 46, iconLeft: 14, textLeft: 72, label: 15, value: 30, sub: 13 };
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
          { matcher: { id: "byName", options: "Component" }, properties: [{ id: "custom.width", value: 88 }] },
          { matcher: { id: "byName", options: "Message" }, properties: [{ id: "mappings", value: [valueMap(Object.entries(messages).map(([k, v]) => [k, v, "text"]))] }] },
          { matcher: { id: "byName", options: "Status" }, properties: [{ id: "custom.width", value: 78 }, { id: "custom.cellOptions", value: { type: "color-text" } }, { id: "mappings", value: [valueMap([[1, "FIRING", "red"], [0, "Resolved", "green"]])] }] },
        ],
      },
      options: { showHeader: true, cellHeight: "sm", footer: { show: false } },
    }),
  };
}

const NAV = [["Executive health", "/d/cdc-ops"], ["Pipeline details", "/d/cdc-details"], ["Archive log storage", "/d/cdc-archive"]];
const dashboard = (uid, title, description, panels) => ({
  uid, title, description, tags: ["cdc", "oracle", "debezium"], schemaVersion: 39, version: 1,
  editable: false, graphTooltip: 1, refresh: "30s", time: { from: "now-1h", to: "now" },
  timepicker: { refresh_intervals: ["10s", "30s", "1m", "5m"] },
  links: NAV.filter(([, u]) => u !== `/d/${uid}`).map(([t, u]) => ({ title: t, type: "link", url: u, icon: "dashboard", keepTime: true })),
  templating: {
    list: [
      { name: "ds", label: "Data source", type: "datasource", query: "prometheus", current: {}, hide: 0 },
      { name: "connector", label: "Connector", type: "query", datasource: DS, query: "label_values(kafka_connect_worker_connector_running_task_count, connector)", refresh: 2, current: {}, hide: 0 },
    ],
  },
  annotations: { list: [] },
  panels,
});
const viewDetails = (url) => [{ title: "View details", url, targetBlank: false }];

// ---- shared expressions --------------------------------------------------------------------------------------------
const C = '{connector=~"$connector"}';
const LAG_MS = 'max(debezium_metrics_millisecondsbehindsource{context="streaming"})';
const LAG_S = `(${LAG_MS} / 1000) and on() (${LAG_MS} >= 0)`;      // Debezium reports -1 until its first event
const FRA_PCT = "100 * oracledb_fra_used_bytes / oracledb_fra_limit_bytes";
const FRA_FREE = "(oracledb_fra_limit_bytes - oracledb_fra_used_bytes + oracledb_fra_reclaimable_bytes)";
const HOURS_TO_FULL = `(${FRA_FREE} / oracledb_archive_log_bytes_last_hour) and on() (oracledb_archive_log_bytes_last_hour > 0)`;
const RETENTION = "oracledb_archive_log_retention_days and on() (oracledb_archive_log_files_on_disk > 0)";
const RUNNING = `clamp_max(min(kafka_connect_worker_connector_running_task_count${C}), 1)`;
const NOT_FAILED = `(max(kafka_connect_worker_connector_failed_task_count${C}) == bool 0)`;
const KAFKA_OK = "clamp_max(max(kafka_brokers), 1) * ((sum(kafka_topic_partition_under_replicated_partition) or vector(0)) == bool 0)";
const HEALTH = `(max(oracledb_db_open_rw) * max(debezium_metrics_connected{context="streaming"}) * ${RUNNING} * ${NOT_FAILED} * clamp_max(max(kafka_brokers), 1) * min(oracledb_archive_dest_valid) * (max(oracledb_fra_used_bytes / oracledb_fra_limit_bytes) < bool 0.9)) or vector(0)`;
// 2 healthy, 1 recovery area 80-90 %, 0 over 90 % or archive destination in error
const STORAGE_STATE = `(((${FRA_PCT}) < bool 80) + ((${FRA_PCT}) < bool 90)) * on() min(oracledb_archive_dest_valid)`;
// 1 while the connector can still resume from its stored position, 0 when that archive log has already been deleted
const POSITION_SAFE = '(max(debezium_metrics_offsetscn{context="streaming"}) >= bool scalar(oracledb_archive_log_oldest_scn)) and on() (oracledb_archive_log_oldest_scn > 0)';
// Section health: share of checks that pass, in percent
const DBZ_HEALTH = `100 * (max(debezium_metrics_connected{context="streaming"}) + ${RUNNING} + ${NOT_FAILED} + (${LAG_MS} < bool 30000)) / 4`;
const KAFKA_HEALTH = '100 * (clamp_max(max(kafka_brokers), 1) + ((sum(kafka_topic_partition_under_replicated_partition) or vector(0)) == bool 0) + ((sum(kafka_consumergroup_lag) or vector(0)) < bool 1000) + max(up{job="kafka"})) / 4';
const ORACLE_HEALTH = `100 * (max(oracledb_db_open_rw) + max(oracledb_db_archivelog) + min(oracledb_archive_dest_valid) + (max(oracledb_fra_used_bytes / oracledb_fra_limit_bytes) < bool 0.9)) / 4`;

const ALERT_TEXT = {
  OracleExporterDown: "Oracle metrics unavailable", ArchiveDestinationInvalid: "Archive destination not VALID",
  ArchivelogModeOff: "Database not in ARCHIVELOG mode", FraUsageHigh: "Recovery area above 80%", FraUsageCritical: "Recovery area above 90%",
  FraFullWithin6Hours: "Recovery area full within 6 h", ConnectorBehindOldestArchiveLog: "Connector position no longer on disk",
  ConnectorTaskNotRunning: "Debezium task not running", DebeziumDisconnected: "Debezium lost Oracle connection", CdcLagHigh: "Source lag above 30 s",
  KafkaBrokerDown: "Kafka broker not reachable", UnderReplicatedPartitions: "Under-replicated partitions", SyncConsumerLagHigh: "Sync more than 1000 messages behind",
  SyncStalled: "Sync stalled with messages waiting", SyncErrors: "Sync errors or dead letters", ScrapeTargetDown: "A metrics target is down",
};

// ---- archive log storage panels (client priority) ------------------------------------------------------------------
function archivePanels(b, y0) {
  b.stat("FRA used", FRA_PCT, [0, y0, 4, 4], { unit: "percent", decimals: 1, thresholds: OK_WARN_CRIT, desc: "Recovery area used. Above 90% Oracle can stop archiving and hang the database." });
  b.stat("Used", "oracledb_fra_used_bytes", [4, y0, 4, 4], { unit: "bytes", colorMode: "value", desc: "Limit: db_recovery_file_dest_size" });
  b.stat("Limit", "oracledb_fra_limit_bytes", [8, y0, 3, 4], { unit: "bytes", colorMode: "value" });
  b.stat("Reclaimable", "oracledb_fra_reclaimable_bytes", [11, y0, 3, 4], { unit: "bytes", colorMode: "value", desc: "Space Oracle frees automatically when it needs it" });
  b.stat("Archive logs on disk", "oracledb_archive_log_bytes_on_disk", [14, y0, 4, 4], { unit: "bytes", colorMode: "value" });
  b.stat("Archive generation", "oracledb_archive_log_bytes_last_hour", [18, y0, 3, 4], { unit: "bytes", colorMode: "value", desc: "Bytes archived in the last hour (per hour)" });
  b.stat("Hours until full", HOURS_TO_FULL, [21, y0, 3, 4], { unit: "h", decimals: 1, thresholds: steps(["red"], ["orange", 6], ["green", 24]), noValue: "nothing archived", desc: "Free plus reclaimable space divided by the last hour's archive rate. Blank while no archive log was written in the last hour." });

  b.ts("Recovery area usage %", [target(FRA_PCT, "FRA used %")], [0, y0 + 4, 12, 8], { unit: "percent", max: 100, color: COL.green, thresholds: steps(["transparent"], ["orange", 80], ["red", 90]), desc: "Dashed lines at 80% (warning) and 90% (critical)" });
  b.ts("Archive log generation (per hour)", [target("oracledb_archive_log_bytes_last_hour", "last hour"), target("oracledb_archive_log_bytes_last_day / 24", "24h average")], [12, y0 + 4, 12, 8], { unit: "bytes", colors: [["last hour", COL.blue], ["24h average", COL.orange]] });
  b.bar("Recovery area by file type", "oracledb_fra_usage_percent_used", "{{file_type}}", [0, y0 + 12, 8, 7], { desc: "Which kind of file fills the recovery area" });
  b.ts("Archive logs on disk", [target("oracledb_archive_log_bytes_on_disk", "bytes")], [8, y0 + 12, 8, 7], { unit: "bytes", color: COL.cyan });
  b.ts("Redo log switches per hour", [target("oracledb_redo_switches_last_hour", "switches")], [16, y0 + 12, 8, 7], { color: COL.purple, desc: "Frequent switches mean heavy write load" });
  b.stat("Archive destination", "min(oracledb_archive_dest_valid)", [0, y0 + 19, 4, 4], { mappings: [valueMap([[1, "VALID", "green"], [0, "ERROR", "red"]]), NO_DATA], thresholds: steps(["red"], ["green", 1]), desc: "Debezium needs a LOCAL destination with status VALID (ORA-16032 breaks it)" });
  b.stat("Retention", RETENTION, [4, y0 + 19, 4, 4], { unit: "d", decimals: 1, thresholds: steps(["red"], ["orange", 1], ["green", 3]), desc: "Age of the oldest archive log still on disk" });
  b.stat("Oldest archive SCN", "oracledb_archive_log_oldest_scn and on() (oracledb_archive_log_files_on_disk > 0)", [8, y0 + 19, 5, 4], { colorMode: "value", decimals: 0 });
  b.stat("Connector offset SCN", 'max(debezium_metrics_offsetscn{context="streaming"})', [13, y0 + 19, 5, 4], { colorMode: "value", decimals: 0 });
  b.stat("Connector can resume", POSITION_SAFE, [18, y0 + 19, 6, 4], { mappings: [valueMap([[1, "SAFE", "green"], [0, "AT RISK", "red"]]), NO_DATA], thresholds: steps(["red"], ["green", 1]), desc: "SAFE while the archive log holding the connector's position still exists. AT RISK is the 'no longer available on the server' failure." });
  return y0 + 23;
}

// ---- Dashboard 1: Executive health (the overview, modelled on the client's reference) -------------------------------
const ops = builder();
{
  const b = ops;
  b.stat("", HEALTH, [0, 0, 24, 2], {
    valueSize: 26,
    mappings: [valueMap([[1, "✔  All Systems Healthy   |   Oracle CDC pipeline is running normally", "green"], [0, "✖  Attention Needed   |   a component is down, missing or over a limit", "red"]])],
    thresholds: steps(["red"], ["green", 1]), desc: "Healthy only when Oracle is open, LogMiner connected, the task running, Kafka up, the archive destination valid and the recovery area under 90%" });

  // KPI tiles
  const T = 2, H = 4;
  b.tile([0, T, 3, H], { icon: "database", iconColor: COL.blue, label: "Oracle DB",
    main: { expr: "max(oracledb_db_open_rw)", mappings: [valueMap([[1, "UP", "green"], [0, "DOWN", "red"]])], thresholds: steps(["red"], ["green", 1]) },
    sub: { expr: "max(oracledb_db_archivelog)", mappings: [valueMap([[1, "ARCHIVELOG: ON", "text"], [0, "ARCHIVELOG: OFF", "red"]])] } });
  b.tile([3, T, 4, H], { icon: "cog", iconColor: COL.purple, label: "LogMiner",
    main: { expr: 'max(debezium_metrics_connected{context="streaming"})', mappings: [valueMap([[1, "ACTIVE", "green"], [0, "INACTIVE", "red"]])], thresholds: steps(["red"], ["green", 1]) },
    sub: { expr: "max(oracledb_db_current_scn)", unit: "prefix:SCN: ", decimals: 0 } });
  b.tile([7, T, 4, H], { icon: "puzzle-piece", iconColor: COL.green, label: "Connector",
    main: { expr: RUNNING, mappings: [valueMap([[1, "RUNNING", "green"], [0, "STOPPED", "red"]])], thresholds: steps(["red"], ["green", 1]) },
    sub: { expr: `max(kafka_connect_worker_connector_failed_task_count${C})`, mappings: [valueMap([[0, "Task: RUNNING", "text"]]), { type: "range", options: { from: 1, to: 1e9, result: { text: "Task: FAILED", color: "red" } } }] } });
  b.tile([11, T, 4, H], { icon: "wifi", iconColor: COL.orange, label: "Kafka",
    main: { expr: KAFKA_OK, mappings: [valueMap([[1, "HEALTHY", "green"], [0, "DEGRADED", "red"]])], thresholds: steps(["red"], ["green", 1]) },
    sub: { expr: "max(kafka_brokers)", unit: "prefix:Brokers: ", decimals: 0 } });
  b.tile([15, T, 3, H], { icon: "bolt", iconColor: COL.purple, label: "CDC Latency", subText: "(Target < 30 s)",
    main: { expr: LAG_S, unit: "s", decimals: 1, thresholds: steps(["green"], ["orange", 10], ["red", 30]), noneText: "IDLE" },
    desc: "Seconds Debezium is behind Oracle. IDLE: no change captured since the connector started." });
  b.tile([18, T, 3, H], { icon: "shield-check", iconColor: COL.teal, label: "Archive Storage",
    main: { expr: STORAGE_STATE, mappings: [valueMap([[2, "SAFE", "green"], [1, "WATCH", "orange"], [0, "CRITICAL", "red"]])], thresholds: steps(["red"], ["orange", 1], ["green", 2]) },
    sub: { expr: RETENTION, unit: "suffix: days kept", decimals: 1, noneText: "no logs yet" }, links: viewDetails("/d/cdc-archive"),
    desc: "SAFE: archive destination VALID and recovery area under 80%. Subtitle: age of the oldest archive log on disk." });
  b.tile([21, T, 3, H], { icon: "exclamation-triangle", iconColor: COL.red, label: "Active Alerts",
    main: { expr: 'count(ALERTS{alertstate="firing"}) or vector(0)', decimals: 0, thresholds: steps(["green"], ["orange", 1], ["red", 3]) },
    sub: { expr: 'count(ALERTS{alertstate="firing"}) or vector(0)', mappings: [valueMap([[0, "No Active Alerts", "text"]]), { type: "range", options: { from: 1, to: 1e9, result: { text: "See alert table", color: "text" } } }] } });

  // Section cards: health gauge + check list + trend
  const S = 6;
  const card = (x, title, gauge, rows, chart, link) => {
    b.gauge("", gauge.expr, [x, S, 3, 7], { label: gauge.label, decimals: gauge.decimals ?? 0, thresholds: steps(["red"], ["orange", 75], ["green", 99.5]) });
    b.list(title, rows, [x + 3, S, 5, 7], { links: link && viewDetails(link) });
    b.ts(chart.title, chart.targets, [x, S + 7, 8, 6], chart.o);
  };
  card(0, "1. Oracle Database & LogMiner", { expr: ORACLE_HEALTH, label: "Healthy" }, [
    { name: "Database Status", expr: "max(oracledb_db_open_rw)", mappings: [valueMap([[1, "OPEN", "green"], [0, "NOT OPEN", "red"]])], colorByValue: true },
    { name: "Log Mode", expr: "max(oracledb_db_archivelog)", mappings: [valueMap([[1, "ARCHIVELOG", "green"], [0, "NOARCHIVELOG", "red"]])], colorByValue: true },
    { name: "Current SCN", expr: "max(oracledb_db_current_scn)", decimals: 0 },
    { name: "Archive Log Rate", expr: "oracledb_archive_log_bytes_last_hour", unit: "bytes" },
    { name: "FRA Usage", expr: FRA_PCT, unit: "percent", decimals: 1, thresholds: OK_WARN_CRIT },
    { name: "Archive Dest", expr: "min(oracledb_archive_dest_valid)", mappings: [valueMap([[1, "VALID", "green"], [0, "ERROR", "red"]])], colorByValue: true },
  ], { title: "Archive Log Generation", targets: [target("oracledb_archive_log_bytes_last_hour", "per hour")], o: { unit: "bytes", color: COL.blue } }, "/d/cdc-details");

  card(8, "2. Debezium Connector", { expr: DBZ_HEALTH, label: "Healthy" }, [
    { name: "Connector Status", expr: 'max(debezium_metrics_connected{context="streaming"})', mappings: [valueMap([[1, "RUNNING", "green"], [0, "DISCONNECTED", "red"]])], colorByValue: true },
    { name: "Task Status", expr: RUNNING, mappings: [valueMap([[1, "RUNNING", "green"], [0, "STOPPED", "red"]])], colorByValue: true },
    { name: "Records Read/s", expr: `sum(rate(kafka_connect_source_source_record_poll${C}[5m]))`, unit: "short", decimals: 2 },
    { name: "Source Lag", expr: LAG_S, unit: "s", decimals: 1, thresholds: steps(["green"], ["orange", 10], ["red", 30]) },
    { name: "Last Event", expr: 'max(debezium_metrics_millisecondssincelastevent{context="streaming"}) >= 0', unit: "dtdurationms" },
    { name: "Queue Free", expr: 'min(debezium_metrics_queueremainingcapacity{context="streaming"})', unit: "short" },
  ], { title: "Connector Lag (seconds)", targets: [target('(debezium_metrics_millisecondsbehindsource{context="streaming"} / 1000) >= 0', "source lag")], o: { unit: "s", color: COL.green, thresholds: steps(["transparent"], ["red", 30]) } }, "/d/cdc-details");

  card(16, "3. Kafka & Topics", { expr: KAFKA_HEALTH, label: "Healthy" }, [
    { name: "Brokers", expr: "max(kafka_brokers)", decimals: 0 },
    { name: "Under Replicated", expr: "sum(kafka_topic_partition_under_replicated_partition) or vector(0)", decimals: 0, thresholds: steps(["green"], ["red", 1]) },
    { name: "CDC Topics", expr: 'count(count by (topic) (kafka_topic_partitions{topic=~"emob\\\\..*"}))', decimals: 0 },
    { name: "Consumer Lag", expr: "sum(kafka_consumergroup_lag) or vector(0)", decimals: 0, thresholds: steps(["green"], ["orange", 1000], ["red", 10000]) },
    { name: "Messages/s", expr: 'sum(rate(kafka_topic_partition_current_offset{topic=~"emob\\\\..*"}[5m]))', unit: "short", decimals: 2 },
  ], { title: "Topic Throughput", targets: [target('sum(rate(kafka_topic_partition_current_offset{topic=~"emob\\\\..*"}[5m]))', "Produced"), target("sum(rate(cdc_events_total[5m])) or vector(0)", "Consumed")], o: { unit: "ops", colors: [["Produced", COL.blue], ["Consumed", COL.orange]], legend: true } }, "/d/cdc-details");

  // Bottom row
  const B = S + 13;
  b.list("4. Retention & CDC Lag", [
    { name: "Required SCN", expr: 'max(debezium_metrics_offsetscn{context="streaming"})', decimals: 0 },
    { name: "Oldest Archive SCN", expr: "oracledb_archive_log_oldest_scn and on() (oracledb_archive_log_files_on_disk > 0)", decimals: 0 },
    { name: "Retention Window", expr: RETENTION, unit: "d", decimals: 1, thresholds: steps(["red"], ["orange", 1], ["green", 3]) },
    { name: "FRA Usage", expr: FRA_PCT, unit: "percent", decimals: 1, thresholds: OK_WARN_CRIT },
    { name: "Can Resume", expr: POSITION_SAFE, mappings: [valueMap([[1, "SAFE", "green"], [0, "AT RISK", "red"]])], colorByValue: true },
    { name: "CDC Lag (E2E)", expr: LAG_S, unit: "s", decimals: 1, thresholds: steps(["green"], ["orange", 10], ["red", 30]) },
  ], [0, B, 5, 9], { links: viewDetails("/d/cdc-archive"), desc: "Required SCN is where the connector resumes from; it must stay at or above the oldest archive SCN." });
  b.ts("FRA Usage Trend", [target(FRA_PCT, "FRA used %")], [5, B, 3, 9], { unit: "percent", max: 100, color: COL.green, fill: 35, thresholds: steps(["transparent"], ["orange", 80], ["red", 90]) });

  b.list("5. Target Apply & Data Consistency", [
    { name: "Target (sync → ES)", expr: 'max(up{job="cdc-es-sync"})', mappings: [valueMap([[1, "ONLINE", "green"], [0, "OFFLINE", "red"]])], colorByValue: true },
    { name: "Apply Process", expr: 'max(up{job="cdc-es-sync"}) * ((sum(increase(cdc_errors_total[10m])) or vector(0)) == bool 0)', mappings: [valueMap([[1, "RUNNING", "green"], [0, "ERRORS", "red"]])], colorByValue: true },
    { name: "Apply Lag", expr: "sum(cdc_consumer_lag) or vector(0)", unit: "suffix: msgs", decimals: 0, thresholds: steps(["green"], ["orange", 1000], ["red", 10000]) },
  ], [8, B, 8, 3], { titleSize: 12, valueSize: 14 });
  b.ts("Apply Backlog & Throughput", [target("sum(cdc_consumer_lag) or vector(0)", "backlog (msgs)"), target("sum(rate(cdc_events_total[5m])) or vector(0)", "applied/s")], [8, B + 3, 8, 6], { colors: [["backlog (msgs)", COL.purple], ["applied/s", COL.teal]] });

  b.alertHistory("6. Alerts & Incident History (24h)", [16, B, 8, 9], ALERT_TEXT);
}

// ---- Dashboard 2: Pipeline details (the "View details" target) -----------------------------------------------------
const det = builder();
{
  const b = det;
  let y = 0;
  b.row("1. Oracle database and LogMiner", y++);
  b.stat("Database", "max(oracledb_db_open_rw)", [0, y, 3, 4], { mappings: upDown, thresholds: steps(["red"], ["green", 1]) });
  b.stat("ARCHIVELOG mode", "max(oracledb_db_archivelog)", [3, y, 3, 4], { mappings: [valueMap([[1, "ON", "green"], [0, "OFF", "red"]])], thresholds: steps(["red"], ["green", 1]) });
  b.stat("PDB open", "min(oracledb_pdb_open_rw)", [6, y, 3, 4], { mappings: [valueMap([[1, "READ WRITE", "green"], [0, "CLOSED", "red"]]), NO_DATA], thresholds: steps(["red"], ["green", 1]) });
  b.stat("Current SCN", "max(oracledb_db_current_scn)", [9, y, 5, 4], { colorMode: "value", decimals: 0 });
  b.stat("Uptime", "max(oracledb_instance_uptime_seconds)", [14, y, 4, 4], { unit: "s", colorMode: "value" });
  b.stat("Archive rate", "oracledb_archive_log_bytes_last_hour", [18, y, 3, 4], { unit: "bytes", colorMode: "value", desc: "per hour" });
  b.stat("FRA used", FRA_PCT, [21, y, 3, 4], { unit: "percent", decimals: 1, thresholds: OK_WARN_CRIT });
  y += 4;
  b.ts("Archive log generation (bytes per hour)", [target("oracledb_archive_log_bytes_last_hour", "per hour")], [0, y, 12, 7], { unit: "bytes", color: COL.blue });
  b.ts("Database SCN", [target("oracledb_db_current_scn", "Oracle current SCN"), target('max(debezium_metrics_offsetscn{context="streaming"})', "Connector offset SCN")], [12, y, 12, 7], { decimals: 0, softZero: false, fill: 0, colors: [["Oracle current SCN", COL.blue], ["Connector offset SCN", COL.orange]], desc: "The two lines should stay close together" });
  y += 7;

  b.row("2. Debezium connector", y++);
  b.stat("Task", RUNNING, [0, y, 3, 4], { mappings: [valueMap([[1, "RUNNING", "green"], [0, "NOT RUNNING", "red"]]), NO_DATA], thresholds: steps(["red"], ["green", 1]) });
  b.stat("Failed tasks", `sum(kafka_connect_worker_connector_failed_task_count${C})`, [3, y, 3, 4], { thresholds: steps(["green"], ["red", 1]) });
  b.stat("Records read/s", `sum(rate(kafka_connect_source_source_record_poll${C}[5m]))`, [6, y, 4, 4], { unit: "ops", decimals: 2, colorMode: "value" });
  b.stat("Source lag", LAG_S, [10, y, 4, 4], { unit: "s", decimals: 1, thresholds: steps(["green"], ["orange", 10], ["red", 30]), noValue: "IDLE" });
  b.stat("Active transactions", 'max(debezium_metrics_numberofactivetransactions{context="streaming"})', [14, y, 5, 4], { colorMode: "value" });
  b.stat("Queue free", 'min(debezium_metrics_queueremainingcapacity{context="streaming"})', [19, y, 5, 4], { thresholds: steps(["red"], ["orange", 1000], ["green", 4000]), desc: "Remaining capacity of the internal event queue. Near 0 means Kafka is the bottleneck." });
  y += 4;
  b.ts("Source lag (seconds)", [target('(debezium_metrics_millisecondsbehindsource{context="streaming"} / 1000) >= 0', "behind source"), target('(debezium_metrics_lagfromsourceinmilliseconds{context="streaming"} / 1000) >= 0', "lag from source")], [0, y, 12, 7], { unit: "s", thresholds: steps(["transparent"], ["red", 30]), colors: [["behind source", COL.green], ["lag from source", COL.blue]] });
  b.ts("Events captured and records written", [target('rate(debezium_metrics_totalcaptureddmlcount{context="streaming"}[5m])', "DML captured/s"), target(`sum(rate(kafka_connect_source_source_record_write${C}[5m]))`, "written to Kafka/s")], [12, y, 12, 7], { unit: "ops", colors: [["DML captured/s", COL.purple], ["written to Kafka/s", COL.orange]] });
  y += 7;

  b.row("3. Kafka and topics", y++);
  b.stat("Brokers", "max(kafka_brokers)", [0, y, 4, 4], { thresholds: steps(["red"], ["green", 1]) });
  b.stat("Under-replicated", "sum(kafka_topic_partition_under_replicated_partition) or vector(0)", [4, y, 4, 4], { thresholds: steps(["green"], ["red", 1]) });
  b.stat("CDC topics", 'count(count by (topic) (kafka_topic_partitions{topic=~"emob\\\\..*"}))', [8, y, 4, 4], { colorMode: "value" });
  b.stat("Consumer lag", "sum(kafka_consumergroup_lag) or vector(0)", [12, y, 4, 4], { thresholds: steps(["green"], ["orange", 1000], ["red", 10000]) });
  b.stat("Messages/s", 'sum(rate(kafka_topic_partition_current_offset{topic=~"emob\\\\..*"}[5m]))', [16, y, 8, 4], { unit: "ops", decimals: 2, colorMode: "value" });
  y += 4;
  b.ts("Messages per second by topic", [target('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"emob\\\\..*"}[5m]))', "{{topic}}")], [0, y, 12, 7], { unit: "ops" });
  b.ts("Consumer group lag", [target("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")], [12, y, 12, 7]);
  y += 7;

  b.row("4. Target apply (cdc-es-sync to Elasticsearch)", y++);
  b.stat("Sync service", 'max(up{job="cdc-es-sync"})', [0, y, 4, 4], { mappings: upDown, thresholds: steps(["red"], ["green", 1]) });
  b.stat("Consumer lag", "sum(cdc_consumer_lag) or vector(0)", [4, y, 4, 4], { thresholds: steps(["green"], ["orange", 1000], ["red", 10000]) });
  b.stat("Last batch", "(time() - max(cdc_last_batch_timestamp)) and on() (max(cdc_last_batch_timestamp) > 0)", [8, y, 4, 4], { unit: "s", decimals: 0, thresholds: steps(["green"], ["orange", 600], ["red", 1800]), noValue: "none since restart", colorMode: "value", desc: "Seconds since the last committed batch. Large values are normal when no data changes. The service reports 0 until its first batch after a restart." });
  b.stat("Errors (1h)", "sum(increase(cdc_errors_total[1h])) or vector(0)", [12, y, 4, 4], { thresholds: steps(["green"], ["red", 1]) });
  b.stat("Dead-lettered (1h)", "sum(increase(cdc_dlq_total[1h])) or vector(0)", [16, y, 4, 4], { thresholds: steps(["green"], ["red", 1]) });
  b.stat("Docs written (1h)", "(sum(increase(cdc_docs_indexed_total[1h])) or vector(0)) + (sum(increase(cdc_docs_updated_total[1h])) or vector(0))", [20, y, 4, 4], { colorMode: "value", decimals: 0 });
  y += 4;
  b.ts("Change events per second by table", [target("sum by (table) (rate(cdc_events_total[5m]))", "{{table}}")], [0, y, 12, 7], { unit: "ops" });
  b.ts("Documents written per second by index", [target("sum by (index) (rate(cdc_docs_indexed_total[5m]))", "{{index}} (new)"), target("sum by (index) (rate(cdc_docs_updated_total[5m]))", "{{index}} (updated)")], [12, y, 12, 7], { unit: "ops" });
}

// ---- Dashboard 3: Archive Log Storage ------------------------------------------------------------------------------
const arch = builder();
{
  arch.stat("", STORAGE_STATE, [0, 0, 24, 2], {
    valueSize: 26,
    mappings: [valueMap([[2, "✔  Healthy   |   archive destination valid and recovery area under 80%", "green"], [1, "⚠  Watch   |   recovery area between 80% and 90%", "orange"], [0, "✖  Critical   |   recovery area over 90% or archive destination in error", "red"]]), { type: "special", options: { match: "null", result: { text: "NO DATA: is oracle-exporter running?", color: "gray" } } }],
    thresholds: steps(["red"], ["orange", 1], ["green", 2]) });
  archivePanels(arch, 2);
}

const write = (file, d) => writeFileSync(join(out, file), JSON.stringify(d, null, 2) + "\n");
write("cdc-operations-center.json", dashboard("cdc-ops", "Oracle CDC Operations Center", "End-to-End Monitoring for the Oracle Debezium CDC Pipeline", ops.panels));
write("cdc-pipeline-details.json", dashboard("cdc-details", "CDC Pipeline Details", "Detailed Oracle, Debezium, Kafka and target-apply metrics", det.panels));
write("cdc-archive-storage.json", dashboard("cdc-archive", "CDC Archive Log Storage", "Oracle archive log volume, recovery area usage, retention, and whether Debezium can still resume", arch.panels));
console.log("written to", out);
