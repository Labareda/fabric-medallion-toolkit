"use strict";

import powerbi from "powerbi-visuals-api";
import * as d3 from "d3";
import { FormattingSettingsService } from "powerbi-visuals-utils-formattingmodel";
import { VisualFormattingSettingsModel } from "./settings";

import DataView = powerbi.DataView;
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisualHost = powerbi.extensibility.visual.IVisualHost;
import ISelectionManager = powerbi.extensibility.ISelectionManager;
import ISelectionId = powerbi.visuals.ISelectionId;
import ITooltipService = powerbi.extensibility.ITooltipService;
import VisualTooltipDataItem = powerbi.extensibility.VisualTooltipDataItem;

/** One node in the task tree (an issue, or an ancestor grouping that is also an issue). */
interface TaskNode {
    id: string;              // stable key = full level path
    label: string;           // "ISSUE-CODE: Summary" (or the raw level text if no own row)
    depth: number;           // 1-based depth in the tree
    sortKey: string;         // Sort Path (or "" )
    start?: Date;            // planned start
    end?: Date;              // planned end
    aStart?: Date;           // actual start
    aEnd?: Date;             // actual end
    lead: string;
    resources: string;
    status: string;
    isMilestone: boolean;
    children: TaskNode[];
    hasData: boolean;        // whether an issue row was attached to this node
    isFirst?: boolean;       // first child of its parent (top of its group / standalone)
    selectionId?: ISelectionId;         // for native selection, context menu, drill-through
    tooltip?: VisualTooltipDataItem[];  // native tooltip rows
}

const SCROLLBAR_W = 16;
const HEADER_H = 40;
const INDENT = 14;

export class Visual implements IVisual {
    private target: HTMLElement;
    private host: IVisualHost;
    private selectionManager: ISelectionManager;
    private tooltipService: ITooltipService;
    private formattingSettingsService: FormattingSettingsService;
    private settings: VisualFormattingSettingsModel;

    private root: TaskNode | null = null;
    private collapsed: Set<string> = new Set<string>();
    private selectedId: string | null = null;   // highlighted / selected issue (drives the drill-through button)
    private lastWidth = 0;
    private lastHeight = 0;
    private zoomTransform: any = null;   // d3 zoom transform, preserved across re-renders
    private dataToken = "";              // changes when the underlying data changes -> reset zoom

    constructor(options: VisualConstructorOptions) {
        this.target = options.element;
        this.target.style.overflow = "hidden";
        this.host = options.host;
        this.selectionManager = this.host.createSelectionManager();
        this.tooltipService = this.host.tooltipService;
        this.formattingSettingsService = new FormattingSettingsService();
    }

    public update(options: VisualUpdateOptions): void {
        this.settings = this.formattingSettingsService.populateFormattingSettingsModel(
            VisualFormattingSettingsModel,
            options.dataViews && options.dataViews[0]
        );

        this.lastWidth = options.viewport.width;
        this.lastHeight = options.viewport.height;

        const dv: DataView = options.dataViews && options.dataViews[0];
        // Reset zoom only when the data itself changes, not on a plain resize,
        // so resizing keeps the user's current zoom/pan.
        const token = dv && dv.table
            ? `${dv.table.rows.length}|${dv.table.columns.length}`
            : "";
        if (token !== this.dataToken) {
            this.zoomTransform = null;
            this.dataToken = token;
        }
        this.root = this.buildTree(dv);
        this.render();
    }

    // ---- data ------------------------------------------------------------

    private buildTree(dv: DataView): TaskNode | null {
        if (!dv || !dv.table || !dv.table.columns || !dv.table.rows) return null;
        const cols = dv.table.columns;
        const rows = dv.table.rows;

        const firstIdx = (role: string): number =>
            cols.findIndex(c => c.roles && (c.roles as any)[role]);

        const iIssue = firstIdx("issueCode");
        const iName = firstIdx("taskName");
        const iStart = firstIdx("startDate");
        const iEnd = firstIdx("endDate");
        const iAStart = firstIdx("actualStart");
        const iAEnd = firstIdx("actualEnd");
        const iLead = firstIdx("lead");
        const iRes = firstIdx("resources");
        const iStatus = firstIdx("status");
        const iMile = firstIdx("milestone");
        const iSort = firstIdx("sortPath");
        const tooltipIdx: number[] = cols.map((c, i) => (c.roles && (c.roles as any)["tooltips"]) ? i : -1).filter(i => i >= 0);

        const asStr = (v: any): string => (v === null || v === undefined) ? "" : String(v);
        const asDate = (v: any): Date | undefined => {
            if (v === null || v === undefined || v === "") return undefined;
            if (v instanceof Date) return isNaN(v.getTime()) ? undefined : v;
            const d = new Date(v);
            return isNaN(d.getTime()) ? undefined : d;
        };
        const asBool = (v: any): boolean =>
            v === true || v === 1 || v === "true" || v === "True" || v === "1";

        const rootNode: TaskNode = {
            id: "__root__", label: "", depth: 0, sortKey: "",
            lead: "", resources: "", status: "", isMilestone: false,
            children: [], hasData: false
        };

        // One node per issue row. The tree is reconstructed ENTIRELY from
        // Sort_Path, whose Gold design guarantees: a parent's path is a literal
        // prefix of each child's, joined by "!" (a separator chosen to sort
        // below every rank character), and a plain ascending lexical sort of
        // Sort_Path reproduces the whole tree -- parent immediately before its
        // children, siblings in rank order. So we sort by Sort_Path as text
        // (NEVER numerically -- the ranks are LexoRank, which is lexical) and
        // rebuild parent/child from the prefix relationship. The bound Level
        // columns are deliberately NOT used: they are type-placed and ragged,
        // not depth-placed, so they don't describe the parent chain.
        const SEP = "!";
        const nodes: TaskNode[] = [];
        const fmtDate = (d?: Date) => d ? d.toLocaleDateString() : undefined;
        rows.forEach((r, rowIndex) => {
            const issueCode = iIssue >= 0 ? asStr(r[iIssue]) : "";
            const summary = iName >= 0 ? asStr(r[iName]) : "";
            const lead = iLead >= 0 ? asStr(r[iLead]) : "";
            const resources = iRes >= 0 ? asStr(r[iRes]) : "";
            const status = iStatus >= 0 ? asStr(r[iStatus]) : "";
            const start = iStart >= 0 ? asDate(r[iStart]) : undefined;
            const end = iEnd >= 0 ? asDate(r[iEnd]) : undefined;
            const aStart = iAStart >= 0 ? asDate(r[iAStart]) : undefined;
            const aEnd = iAEnd >= 0 ? asDate(r[iAEnd]) : undefined;
            let path = iSort >= 0 ? asStr(r[iSort]) : "";
            // Rows with no Sort_Path still appear, ordered after everything else.
            if (path === "") path = "￿" + String(rowIndex).padStart(9, "0");

            // native tooltip rows: key fields, plus anything dropped in the Tooltips well
            const tip: VisualTooltipDataItem[] = [];
            const addTip = (name: string, val?: string) => { if (val !== undefined && val !== "") tip.push({ displayName: name, value: val }); };
            addTip("Issue", issueCode);
            addTip("Task", summary);
            addTip("Status", status);
            addTip("Lead", lead);
            addTip("Resources", resources);
            addTip("Planned start", fmtDate(start));
            addTip("Planned end", fmtDate(end));
            addTip("Actual start", fmtDate(aStart));
            addTip("Actual end", fmtDate(aEnd));
            tooltipIdx.forEach(i => addTip(cols[i].displayName, asStr(r[i])));

            nodes.push({
                id: path,
                label: issueCode && summary ? `${issueCode}: ${summary}` : (issueCode || summary || issueCode),
                depth: 1,
                sortKey: path,
                start, end, aStart, aEnd, lead, resources, status,
                isMilestone: iMile >= 0 ? asBool(r[iMile]) : false,
                children: [], hasData: true,
                selectionId: this.host.createSelectionIdBuilder().withTable(dv.table, rowIndex).createSelectionId(),
                tooltip: tip
            });
        });

        // Plain lexical sort of Sort_Path == the exact tree (DFS) order.
        nodes.sort((a, b) => a.sortKey < b.sortKey ? -1 : (a.sortKey > b.sortKey ? 1 : 0));

        // Stack reconstruction: a node's parent is the nearest preceding node
        // whose path + "!" is a prefix of this node's path. This is robust to
        // a missing intermediate (the node just attaches to its nearest present
        // ancestor) and to multiple root trees (project-prefixed roots).
        const anc: TaskNode[] = [];
        for (const node of nodes) {
            while (anc.length && node.sortKey.indexOf(anc[anc.length - 1].sortKey + SEP) !== 0) {
                anc.pop();
            }
            const parent = anc.length ? anc[anc.length - 1] : rootNode;
            node.depth = parent === rootNode ? 1 : parent.depth + 1;
            node.isFirst = parent.children.length === 0;   // first task of its group / standalone
            parent.children.push(node);
            anc.push(node);
        }
        return rootNode;
    }

    // ---- render ----------------------------------------------------------

    private render(): void {
        const el = this.target;
        while (el.firstChild) el.removeChild(el.firstChild);
        if (!this.root || this.root.children.length === 0) {
            const msg = document.createElement("div");
            msg.style.cssText = "padding:12px;font:12px 'Segoe UI';color:#888";
            msg.textContent = "Bind Issue Code, Task Name, Sort Path (required for the tree), " +
                "Planned Start/End Date, Lead, Resources, Status and Is Milestone.";
            el.appendChild(msg);
            return;
        }

        const s = this.settings;
        const rowH = Math.max(14, s.appearance.rowHeight.value);
        const fontSize = Math.max(7, s.appearance.fontSize.value);
        const textColor = s.appearance.textColor.value.value;
        const headerColor = s.appearance.headerColor.value.value;
        const showLead = s.columns.showLead.value;
        const showRes = s.columns.showResources.value;
        const nameW = Math.max(80, s.columns.nameWidth.value);
        const leadW = showLead ? Math.max(40, s.columns.leadWidth.value) : 0;
        const resW = showRes ? Math.max(40, s.columns.resourceWidth.value) : 0;
        const leftW = nameW + leadW + resW;

        const totalW = this.lastWidth;
        const totalH = this.lastHeight;
        const timelineW = Math.max(180, totalW - leftW - SCROLLBAR_W);

        // visible rows (respecting collapse state)
        const visible: TaskNode[] = [];
        const walk = (n: TaskNode) => {
            for (const c of n.children) {
                visible.push(c);
                if (c.children.length > 0 && !this.collapsed.has(c.id)) walk(c);
            }
        };
        walk(this.root);

        const contentH = visible.length * rowH;

        // time scale across all tasks
        let minD: Date | undefined, maxD: Date | undefined;
        const statusSet: string[] = [];
        let anyMilestone = false, anyActual = false;
        const scan = (n: TaskNode) => {
            const lo = [n.start, n.aStart].filter(Boolean) as Date[];
            const hi = [n.end, n.aEnd, n.start, n.aStart].filter(Boolean) as Date[];
            for (const d of lo) if (!minD || d < minD) minD = d;
            for (const d of hi) if (!maxD || d > maxD) maxD = d;
            const hasBar = !!((n.start && n.end) || (n.aStart && n.aEnd));
            if (hasBar && n.status && statusSet.indexOf(n.status) < 0) statusSet.push(n.status);
            if (n.isMilestone && n.end) anyMilestone = true;
            if (n.start && n.end && n.aStart && n.aEnd) anyActual = true;
            n.children.forEach(scan);
        };
        scan(this.root);
        if (!minD || !maxD) { minD = new Date(); maxD = d3.timeMonth.offset(minD, 3); }
        const pad = Math.max(1, Math.round((maxD.getTime() - minD.getTime()) / 40));
        const domainMin = new Date(minD.getTime() - pad);
        const domainMax = new Date(maxD.getTime() + pad);
        const x = d3.scaleTime().domain([domainMin, domainMax]).range([0, timelineW]);

        // ---- outer flex column: header + scroll body ----
        const container = document.createElement("div");
        container.style.cssText = `width:${totalW}px;height:${totalH}px;` +
            `display:flex;flex-direction:column;font-family:'Segoe UI',sans-serif;` +
            `box-sizing:border-box;overflow:hidden;`;
        el.appendChild(container);

        // ---- header ----
        const header = document.createElement("div");
        header.style.cssText = `display:flex;height:${HEADER_H}px;flex:0 0 auto;` +
            `background:${headerColor};color:#fff;`;
        container.appendChild(header);

        const colHeader = (text: string, w: number) => {
            const d = document.createElement("div");
            d.textContent = text;
            d.style.cssText = `width:${w}px;flex:0 0 ${w}px;display:flex;align-items:center;` +
                `padding:0 8px;box-sizing:border-box;font-size:${fontSize + 1}px;` +
                `font-weight:600;border-right:1px solid rgba(255,255,255,.35);`;
            return d;
        };
        header.appendChild(colHeader("Name", nameW));
        if (showLead) header.appendChild(colHeader("Lead", leadW));
        if (showRes) header.appendChild(colHeader("Resources", resW));

        // axis in header (adaptive year/quarter/month/day bands, drawn by drawTimeline)
        const axisWrap = document.createElement("div");
        axisWrap.style.cssText = `flex:0 0 ${timelineW + SCROLLBAR_W}px;position:relative;`;
        header.appendChild(axisWrap);
        const axisSvg = d3.select(axisWrap).append("svg")
            .attr("width", timelineW).attr("height", HEADER_H);
        const gAxis = axisSvg.append("g");

        // ---- scroll body ----
        const body = document.createElement("div");
        body.style.cssText = `flex:1 1 auto;overflow-y:scroll;overflow-x:hidden;` +
            `display:flex;position:relative;`;
        container.appendChild(body);

        // left panel
        const left = document.createElement("div");
        left.style.cssText = `flex:0 0 ${leftW}px;width:${leftW}px;position:relative;` +
            `height:${contentH}px;border-right:1px solid #E0E0E0;`;
        body.appendChild(left);

        // right timeline svg + layered groups (redrawn on every zoom/pan)
        const rightSvg = d3.select(body).append("svg")
            .attr("width", timelineW).attr("height", contentH)
            .style("flex", `0 0 ${timelineW}px`)
            .style("cursor", "grab");
        const gBands = rightSvg.append("g");   // static row banding + row lines (behind everything)
        const gGrid = rightSvg.append("g");    // time gridlines (redrawn on zoom)
        const gBars = rightSvg.append("g");
        const gToday = rightSvg.append("g");

        // matrix-style row banding + horizontal row lines (independent of zoom)
        const grid = s.grid;
        const bandedRows = grid.bandedRows.value;
        const bandColor = grid.bandColor.value.value;
        const rowLines = grid.rowBorders.value;
        const rowLineColor = grid.rowBorderColor.value.value;
        const colLines = grid.colBorders.value;
        const colLineColor = grid.colBorderColor.value.value;
        visible.forEach((n, i) => {
            const y = i * rowH;
            // selected-row highlight spans the WHOLE row (behind the bars)
            if (n.id === this.selectedId) {
                gBands.append("rect").attr("x", 0).attr("y", y)
                    .attr("width", timelineW).attr("height", rowH).attr("fill", "#D6E8FB");
            } else if (bandedRows && i % 2 === 1) {
                gBands.append("rect").attr("x", 0).attr("y", y)
                    .attr("width", timelineW).attr("height", rowH).attr("fill", bandColor);
            }
            if (rowLines) {
                gBands.append("line")
                    .attr("x1", 0).attr("x2", timelineW)
                    .attr("y1", (i + 1) * rowH).attr("y2", (i + 1) * rowH)
                    .attr("stroke", rowLineColor).attr("stroke-width", 1);
            }
            // transparent full-width click target: click ANYWHERE on the row to
            // select the issue (behind the bars, so bars still handle their own).
            gBands.append("rect").attr("x", 0).attr("y", y)
                .attr("width", timelineW).attr("height", rowH).attr("fill", "transparent")
                .style("cursor", "pointer")
                .on("click", (event: any) => {
                    event.stopPropagation();
                    if (n.selectionId) this.selectionManager.select(n.selectionId, event.ctrlKey || event.metaKey);
                    this.selectedId = this.selectedId === n.id ? null : n.id;
                    this.render();
                });
        });

        const statusColor = (status: string): string => {
            if (!s.bars.colorByStatus.value) return s.bars.defaultColor.value.value;
            const st = (status || "").toLowerCase();
            if (st.indexOf("done") >= 0 || st.indexOf("complete") >= 0 || st.indexOf("closed") >= 0)
                return s.bars.doneColor.value.value;
            if (st.indexOf("progress") >= 0 || st.indexOf("doing") >= 0)
                return s.bars.inProgressColor.value.value;
            if (st.indexOf("to do") >= 0 || st.indexOf("todo") >= 0 || st.indexOf("open") >= 0 ||
                st.indexOf("backlog") >= 0 || st.indexOf("new") >= 0)
                return s.bars.toDoColor.value.value;
            return s.bars.defaultColor.value.value;
        };
        const barColor = (n: TaskNode): string => statusColor(n.status);
        const barH = Math.max(4, Math.round(rowH * 0.55));
        const corner = Math.max(0, s.bars.cornerRadius.value);
        const showLabels = s.bars.showBarLabels.value;
        const mColor = s.milestone.milestoneColor.value.value;
        const mSize = Math.max(4, s.milestone.milestoneSize.value);
        const gridColor = s.appearance.gridlineColor.value.value;
        const showGrid = s.appearance.showGridlines.value;
        const showActual = s.actualBar.show.value;
        const actualColor = s.actualBar.actualColor.value.value;
        const actualH = Math.max(3, Math.round(barH * 0.32));

        // Redraw everything that depends on the (zoomed) time scale.
        const drawTimeline = (cx: any) => {
            gGrid.selectAll("*").remove();
            gBars.selectAll("*").remove();
            gToday.selectAll("*").remove();
            gAxis.selectAll("*").remove();

            const [d0, d1] = cx.domain();
            const pxPerDay = cx(d3.timeDay.offset(d0, 1)) - cx(d0);

            // pick major (top) + minor (bottom) granularity from zoom level
            let minorInt: d3.CountableTimeInterval, minorStep = 1;
            let minorFmt: (d: Date) => string;
            let majorInt: d3.CountableTimeInterval, majorFmt: (d: Date) => string;
            if (pxPerDay >= 11) {
                majorInt = d3.timeMonth; majorFmt = d3.timeFormat("%B %Y");
                minorInt = d3.timeDay; minorStep = Math.max(1, Math.ceil(20 / pxPerDay));
                minorFmt = d3.timeFormat("%-d");
            } else if (pxPerDay >= 2.6) {
                majorInt = d3.timeYear; majorFmt = d3.timeFormat("%Y");
                minorInt = d3.timeMonth; minorFmt = d3.timeFormat("%b");
            } else if (pxPerDay >= 0.55) {
                majorInt = d3.timeYear; majorFmt = d3.timeFormat("%Y");
                minorInt = d3.timeMonth; minorStep = 3;
                minorFmt = (d: Date) => "Q" + (Math.floor(d.getMonth() / 3) + 1);
            } else {
                majorInt = d3.timeYear; majorFmt = d3.timeFormat("%Y");
                minorInt = d3.timeYear; minorFmt = d3.timeFormat("%Y");
            }

            // minor boundaries within the visible window
            const minors = (minorStep > 1 ? minorInt.every(minorStep) : minorInt)!
                .range(minorInt.floor(d0), minorInt.offset(d1, 1));

            // gridlines
            if (showGrid) {
                gGrid.selectAll("line").data(minors).enter().append("line")
                    .attr("x1", (d: Date) => cx(d)).attr("x2", (d: Date) => cx(d))
                    .attr("y1", 0).attr("y2", contentH)
                    .attr("stroke", gridColor).attr("stroke-width", 1);
            }

            // minor band (bottom row of header)
            minors.forEach((b: Date) => {
                const segEnd = minorStep > 1 ? minorInt.offset(b, minorStep) : minorInt.offset(b, 1);
                const a = b < d0 ? d0 : b;
                const e = segEnd > d1 ? d1 : segEnd;
                if (e <= a) return;
                gAxis.append("line").attr("x1", cx(b)).attr("x2", cx(b))
                    .attr("y1", 18).attr("y2", HEADER_H).attr("stroke", "rgba(255,255,255,.2)");
                const mid = (cx(a) + cx(e)) / 2;
                if (mid > -20 && mid < timelineW + 20) {
                    gAxis.append("text").attr("x", mid).attr("y", 31)
                        .attr("text-anchor", "middle").attr("fill", "rgba(255,255,255,.92)")
                        .attr("font-size", `${fontSize}px`).text(minorFmt(b));
                }
            });

            // major band (top row of header)
            const majors = majorInt.range(majorInt.floor(d0), majorInt.offset(d1, 1));
            majors.forEach((b: Date) => {
                const segEnd = majorInt.offset(b, 1);
                const a = b < d0 ? d0 : b;
                const e = segEnd > d1 ? d1 : segEnd;
                if (e <= a) return;
                gAxis.append("line").attr("x1", cx(b)).attr("x2", cx(b))
                    .attr("y1", 2).attr("y2", HEADER_H).attr("stroke", "rgba(255,255,255,.4)");
                const mid = (cx(a) + cx(e)) / 2;
                if (mid > -40 && mid < timelineW + 40) {
                    gAxis.append("text").attr("x", mid).attr("y", 13)
                        .attr("text-anchor", "middle").attr("fill", "#fff")
                        .attr("font-size", `${fontSize + 1}px`).attr("font-weight", 600)
                        .text(majorFmt(b));
                }
            });

            // bars + milestones + labels
            visible.forEach((n, i) => {
                const cy = i * rowH + rowH / 2;
                if (n.isMilestone && n.end) {
                    const mx = cx(n.end);
                    if (mx < -mSize || mx > timelineW + mSize) return;
                    this.attach(gBars.append("path")
                        .attr("d", `M${mx} ${cy - mSize} L${mx + mSize} ${cy} L${mx} ${cy + mSize} L${mx - mSize} ${cy} Z`)
                        .attr("fill", mColor), n);
                    if (showLabels) this.barLabel(gBars, mx + mSize + 4, cy, n.label, fontSize, timelineW);
                } else {
                    // main bar = planned range if present, else actual range
                    const mS = n.start || n.aStart;
                    const mE = n.end || n.aEnd;
                    if (!mS || !mE) return;
                    const x0 = cx(mS);
                    const x1 = Math.max(x0 + 2, cx(mE));
                    if (x1 < 0 || x0 > timelineW) return;
                    this.attach(gBars.append("rect")
                        .attr("x", x0).attr("y", cy - barH / 2)
                        .attr("width", x1 - x0).attr("height", barH)
                        .attr("rx", corner).attr("ry", corner)
                        .attr("fill", barColor(n)), n);
                    // actual baseline: thin bar along the bottom, only when BOTH
                    // planned and actual are present (so it reads as plan vs actual)
                    if (showActual && n.start && n.end && n.aStart && n.aEnd) {
                        const ax0 = cx(n.aStart);
                        const ax1 = Math.max(ax0 + 2, cx(n.aEnd));
                        gBars.append("rect")
                            .attr("x", ax0).attr("y", cy + barH / 2 - actualH + 1)
                            .attr("width", ax1 - ax0).attr("height", actualH)
                            .attr("rx", 1).attr("ry", 1)
                            .attr("fill", actualColor);
                    }
                    if (showLabels) this.barLabel(gBars, x1 + 4, cy, n.label, fontSize, timelineW);
                }
            });

            // today line
            if (s.todayLine.show.value) {
                const now = new Date();
                const nx = cx(now);
                if (nx >= 0 && nx <= timelineW) {
                    gToday.append("line")
                        .attr("x1", nx).attr("x2", nx).attr("y1", 0).attr("y2", contentH)
                        .attr("stroke", s.todayLine.lineColor.value.value)
                        .attr("stroke-width", 1.5).attr("stroke-dasharray", "4,3");
                }
            }
        };

        // zoom + pan (horizontal): wheel to zoom, drag to pan; scaleExtent caps
        // how far in (down to days) and out (fit-to-width) you can go.
        const zoom = d3.zoom<SVGSVGElement, unknown>()
            .scaleExtent([1, 120])
            .translateExtent([[0, 0], [timelineW, contentH]])
            .extent([[0, 0], [timelineW, contentH]])
            .filter((event: any) => {
                // block the browser page-zoom (ctrl+wheel) but allow wheel/drag
                if (event.type === "wheel") return !event.ctrlKey;
                return !event.button;
            })
            .on("zoom", (event: any) => {
                this.zoomTransform = event.transform;
                rightSvg.style("cursor", event.sourceEvent && event.sourceEvent.type === "mousemove" ? "grabbing" : "grab");
                drawTimeline(event.transform.rescaleX(x));
            });
        (rightSvg as any).call(zoom);
        // restore prior zoom (preserved across expand/collapse); triggers first draw
        (rightSvg as any).call(zoom.transform, this.zoomTransform || d3.zoomIdentity);

        // zoom controls (top-right of the axis header)
        const mkBtn = (label: string, title: string, right: number, fn: () => void) => {
            const b = document.createElement("div");
            b.textContent = label;
            b.title = title;
            b.style.cssText = `position:absolute;top:2px;right:${right}px;width:18px;height:16px;` +
                `line-height:16px;text-align:center;background:rgba(255,255,255,.9);color:#333;` +
                `border-radius:3px;cursor:pointer;font-size:12px;font-weight:600;user-select:none;`;
            b.onclick = fn;
            axisWrap.appendChild(b);
        };
        mkBtn("−", "Zoom out", 4 + SCROLLBAR_W, () => (rightSvg as any).transition().duration(150).call(zoom.scaleBy, 1 / 1.6));
        mkBtn("↺", "Reset zoom", 26 + SCROLLBAR_W, () => (rightSvg as any).transition().duration(150).call(zoom.transform, d3.zoomIdentity));
        mkBtn("+", "Zoom in", 48 + SCROLLBAR_W, () => (rightSvg as any).transition().duration(150).call(zoom.scaleBy, 1.6));

        // left tree rows
        visible.forEach((n, i) => {
            const row = document.createElement("div");
            row.style.cssText = `position:absolute;top:${i * rowH}px;left:0;` +
                `width:${leftW}px;height:${rowH}px;display:flex;align-items:center;cursor:pointer;` +
                `box-sizing:border-box;font-size:${fontSize}px;color:${textColor};` +
                (n.id === this.selectedId ? "background:#D6E8FB;"
                    : (bandedRows && i % 2 === 1 ? `background:${bandColor};` : "")) +
                (rowLines ? `border-bottom:1px solid ${rowLineColor};` : "");
            // click the issue in the name column -> select it (sets the filter
            // context a drill-through button reacts to); right-click -> native menu.
            row.onclick = (ev) => {
                if (n.selectionId) this.selectionManager.select(n.selectionId, ev.ctrlKey || ev.metaKey);
                this.selectedId = this.selectedId === n.id ? null : n.id;
                this.render();
            };
            left.appendChild(row);

            const colBorder = colLines ? `border-right:1px solid ${colLineColor};` : "";

            // name cell (chevron + indent + label)
            const nameCell = document.createElement("div");
            nameCell.style.cssText = `width:${nameW}px;flex:0 0 ${nameW}px;display:flex;` +
                `align-items:center;box-sizing:border-box;` +
                `padding-left:${4 + (n.depth - 1) * INDENT}px;padding-right:6px;overflow:hidden;` +
                ((showLead || showRes) ? colBorder : "");
            const hasChildren = n.children.length > 0;
            const chevron = document.createElement("span");
            chevron.textContent = hasChildren ? (this.collapsed.has(n.id) ? "▶" : "▼") : "";
            chevron.style.cssText = `flex:0 0 12px;width:12px;cursor:pointer;color:#888;` +
                `font-size:${Math.max(7, fontSize - 2)}px;user-select:none;text-align:center;`;
            if (hasChildren) {
                chevron.onclick = (ev) => {
                    ev.stopPropagation();   // don't also select the row
                    if (this.collapsed.has(n.id)) this.collapsed.delete(n.id);
                    else this.collapsed.add(n.id);
                    this.render();
                };
            }
            nameCell.appendChild(chevron);
            const labelSpan = document.createElement("span");
            labelSpan.textContent = n.label;
            labelSpan.title = n.label;
            // bold only the TOP-LEVEL tasks (depth 1) -- the outermost items;
            // everything nested beneath them is normal weight.
            labelSpan.style.cssText = `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;` +
                (n.depth === 1 ? "font-weight:600;" : "");
            nameCell.appendChild(labelSpan);
            row.appendChild(nameCell);

            const textCell = (text: string, w: number, withBorder: boolean) => {
                const c = document.createElement("div");
                c.textContent = text;
                c.title = text;
                c.style.cssText = `width:${w}px;flex:0 0 ${w}px;padding:0 8px;box-sizing:border-box;` +
                    `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;` +
                    (withBorder ? colBorder : "");
                return c;
            };
            // Lead gets a right border only when Resources follows it; Resources
            // is the last column, so no trailing separator.
            if (showLead) row.appendChild(textCell(n.lead, leadW, showRes));
            if (showRes) row.appendChild(textCell(n.resources, resW, false));
        });

        // ---- legend (status colour key + milestone / actual markers) ----
        if (s.legend.show.value) {
            const legend = document.createElement("div");
            legend.style.cssText = `flex:0 0 auto;display:flex;flex-wrap:wrap;align-items:center;` +
                `gap:4px 14px;padding:4px 10px;box-sizing:border-box;font-size:${fontSize}px;` +
                `color:${textColor};background:transparent;`;

            const item = (swatch: HTMLElement, text: string) => {
                const wrap = document.createElement("div");
                wrap.style.cssText = "display:flex;align-items:center;gap:5px;";
                wrap.appendChild(swatch);
                const t = document.createElement("span");
                t.textContent = text;
                wrap.appendChild(t);
                legend.appendChild(wrap);
            };
            const box = (color: string) => {
                const b = document.createElement("span");
                b.style.cssText = `width:12px;height:12px;border-radius:2px;background:${color};` +
                    `display:inline-block;flex:0 0 12px;`;
                return b;
            };

            // status entries, ordered To Do -> In Progress -> Done -> other
            if (s.bars.colorByStatus.value) {
                const rank = (st: string): number => {
                    const l = st.toLowerCase();
                    if (l.indexOf("to do") >= 0 || l.indexOf("todo") >= 0 || l.indexOf("open") >= 0 ||
                        l.indexOf("backlog") >= 0 || l.indexOf("new") >= 0) return 0;
                    if (l.indexOf("progress") >= 0 || l.indexOf("doing") >= 0) return 1;
                    if (l.indexOf("done") >= 0 || l.indexOf("complete") >= 0 || l.indexOf("closed") >= 0) return 2;
                    return 3;
                };
                statusSet.sort((a, b) => {
                    const r = rank(a) - rank(b);
                    return r !== 0 ? r : (a < b ? -1 : 1);
                });
                statusSet.forEach(st => item(box(statusColor(st)), st));
            }

            if (anyActual && s.actualBar.show.value) {
                const bar = document.createElement("span");
                bar.style.cssText = `width:14px;height:4px;border-radius:1px;` +
                    `background:${s.actualBar.actualColor.value.value};display:inline-block;flex:0 0 14px;`;
                item(bar, "Actual dates");
            }
            if (anyMilestone) {
                const dia = document.createElement("span");
                const c = s.milestone.milestoneColor.value.value;
                dia.style.cssText = `width:10px;height:10px;background:${c};display:inline-block;` +
                    `flex:0 0 10px;transform:rotate(45deg);`;
                item(dia, "Milestone");
            }
            if (s.todayLine.show.value) {
                const ln = document.createElement("span");
                ln.style.cssText = `width:14px;height:0;border-top:2px dashed ${s.todayLine.lineColor.value.value};` +
                    `display:inline-block;flex:0 0 14px;`;
                item(ln, "Today");
            }

            if (legend.childNodes.length > 0) {
                if (s.legend.atBottom.value) container.appendChild(legend);
                else container.insertBefore(legend, header);
            }
        }
    }

    private barLabel(svg: any, xPos: number, cy: number, text: string, fontSize: number, timelineW: number): void {
        if (xPos > timelineW - 10) return;
        svg.append("text")
            .attr("x", xPos).attr("y", cy)
            .attr("dominant-baseline", "middle")
            .attr("font-size", `${Math.max(7, fontSize - 1)}px`)
            .attr("fill", "#8A8A8A")
            .text(text);
    }

    /** Wire native tooltip, click-to-select and right-click (drill-through) onto a bar. */
    private attach(sel: any, n: TaskNode): void {
        const ids = n.selectionId ? [n.selectionId] : [];
        sel.style("cursor", "pointer")
            .on("click", (event: any) => {
                event.stopPropagation();
                if (n.selectionId) this.selectionManager.select(n.selectionId, event.ctrlKey || event.metaKey);
                this.selectedId = this.selectedId === n.id ? null : n.id;
                this.render();
            })
            .on("mouseover", (event: any) =>
                this.tooltipService.show({ dataItems: n.tooltip || [], identities: ids, coordinates: [event.clientX, event.clientY], isTouchEvent: false }))
            .on("mousemove", (event: any) =>
                this.tooltipService.move({ dataItems: n.tooltip || [], identities: ids, coordinates: [event.clientX, event.clientY], isTouchEvent: false }))
            .on("mouseout", () => this.tooltipService.hide({ immediately: false, isTouchEvent: false }));
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        return this.formattingSettingsService.buildFormattingModel(this.settings);
    }
}
