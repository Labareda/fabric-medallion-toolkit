"use strict";

import powerbi from "powerbi-visuals-api";
import * as d3 from "d3";
import { FormattingSettingsService } from "powerbi-visuals-utils-formattingmodel";
import { VisualFormattingSettingsModel } from "./settings";

import DataView = powerbi.DataView;
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;

/** One node in the task tree (an issue, or an ancestor grouping that is also an issue). */
interface TaskNode {
    id: string;              // stable key = full level path
    label: string;           // "ISSUE-CODE: Summary" (or the raw level text if no own row)
    depth: number;           // 1-based depth in the tree
    sortKey: string;         // Sort Path (or "" )
    start?: Date;
    end?: Date;
    lead: string;
    resources: string;
    status: string;
    isMilestone: boolean;
    children: TaskNode[];
    hasData: boolean;        // whether an issue row was attached to this node
}

const SCROLLBAR_W = 16;
const HEADER_H = 40;
const INDENT = 14;

export class Visual implements IVisual {
    private target: HTMLElement;
    private formattingSettingsService: FormattingSettingsService;
    private settings: VisualFormattingSettingsModel;

    private root: TaskNode | null = null;
    private collapsed: Set<string> = new Set<string>();
    private lastWidth = 0;
    private lastHeight = 0;

    constructor(options: VisualConstructorOptions) {
        this.target = options.element;
        this.target.style.overflow = "hidden";
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
        const levelIdx: number[] = cols
            .map((c, i) => (c.roles && (c.roles as any)["level"] ? i : -1))
            .filter(i => i >= 0);

        const iIssue = firstIdx("issueCode");
        const iName = firstIdx("taskName");
        const iStart = firstIdx("startDate");
        const iEnd = firstIdx("endDate");
        const iLead = firstIdx("lead");
        const iRes = firstIdx("resources");
        const iStatus = firstIdx("status");
        const iMile = firstIdx("milestone");
        const iSort = firstIdx("sortPath");

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
        const byId: { [id: string]: TaskNode } = { "__root__": rootNode };

        for (const r of rows) {
            const levels: string[] = levelIdx.map(i => asStr(r[i])).filter(s => s !== "");
            const issueCode = iIssue >= 0 ? asStr(r[iIssue]) : "";
            const path = levels.length > 0 ? levels : (issueCode ? [issueCode] : []);
            if (path.length === 0) continue;

            // walk/create the path, creating placeholder ancestors as needed
            let parent = rootNode;
            let acc = "";
            for (let k = 0; k < path.length; k++) {
                acc = acc === "" ? path[k] : acc + " ▸ " + path[k];
                let node = byId[acc];
                if (!node) {
                    node = {
                        id: acc, label: path[k], depth: k + 1, sortKey: "",
                        lead: "", resources: "", status: "", isMilestone: false,
                        children: [], hasData: false
                    };
                    byId[acc] = node;
                    parent.children.push(node);
                }
                parent = node;
            }

            // attach this issue's data to the deepest node on its path
            const leaf = parent;
            const summary = iName >= 0 ? asStr(r[iName]) : "";
            leaf.hasData = true;
            leaf.label = issueCode && summary ? `${issueCode}: ${summary}`
                : (issueCode || summary || leaf.label);
            leaf.start = iStart >= 0 ? asDate(r[iStart]) : undefined;
            leaf.end = iEnd >= 0 ? asDate(r[iEnd]) : undefined;
            leaf.lead = iLead >= 0 ? asStr(r[iLead]) : "";
            leaf.resources = iRes >= 0 ? asStr(r[iRes]) : "";
            leaf.status = iStatus >= 0 ? asStr(r[iStatus]) : "";
            leaf.isMilestone = iMile >= 0 ? asBool(r[iMile]) : false;
            leaf.sortKey = iSort >= 0 ? asStr(r[iSort]) : "";
        }

        // sort every level by Sort Path (fall back to label)
        const sortRec = (n: TaskNode) => {
            n.children.sort((a, b) => {
                const ak = a.sortKey || this.minSort(a);
                const bk = b.sortKey || this.minSort(b);
                if (ak < bk) return -1;
                if (ak > bk) return 1;
                return a.label < b.label ? -1 : 1;
            });
            n.children.forEach(sortRec);
        };
        sortRec(rootNode);
        return rootNode;
    }

    private minSort(n: TaskNode): string {
        let m = n.sortKey || "￿";
        for (const c of n.children) {
            const cm = this.minSort(c);
            if (cm < m) m = cm;
        }
        return m;
    }

    // ---- render ----------------------------------------------------------

    private render(): void {
        const el = this.target;
        while (el.firstChild) el.removeChild(el.firstChild);
        if (!this.root || this.root.children.length === 0) {
            const msg = document.createElement("div");
            msg.style.cssText = "padding:12px;font:12px 'Segoe UI';color:#888";
            msg.textContent = "Bind Issue Code, Task Name, Hierarchy Levels, " +
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
        const scan = (n: TaskNode) => {
            if (n.start && (!minD || n.start < minD)) minD = n.start;
            if (n.end && (!maxD || n.end > maxD)) maxD = n.end;
            if (n.start && (!maxD || n.start > maxD)) maxD = n.start;
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

        // axis in header (years + quarters)
        const axisWrap = document.createElement("div");
        axisWrap.style.cssText = `flex:0 0 ${timelineW + SCROLLBAR_W}px;position:relative;`;
        header.appendChild(axisWrap);
        const axisSvg = d3.select(axisWrap).append("svg")
            .attr("width", timelineW).attr("height", HEADER_H);
        this.renderAxisHeader(axisSvg, x, timelineW, fontSize);

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

        // right timeline svg
        const rightSvg = d3.select(body).append("svg")
            .attr("width", timelineW).attr("height", contentH)
            .style("flex", `0 0 ${timelineW}px`);

        // gridlines (quarter boundaries)
        if (s.appearance.showGridlines.value) {
            const gridColor = s.appearance.gridlineColor.value.value;
            const quarters = d3.timeMonth.range(
                d3.timeMonth.floor(domainMin), domainMax, 3);
            rightSvg.append("g").selectAll("line").data(quarters).enter()
                .append("line")
                .attr("x1", d => x(d)).attr("x2", d => x(d))
                .attr("y1", 0).attr("y2", contentH)
                .attr("stroke", gridColor).attr("stroke-width", 1);
        }

        // bars + labels + milestones
        const barColor = (n: TaskNode): string => {
            if (!s.bars.colorByStatus.value) return s.bars.defaultColor.value.value;
            const st = (n.status || "").toLowerCase();
            if (st.indexOf("done") >= 0 || st.indexOf("complete") >= 0 || st.indexOf("closed") >= 0)
                return s.bars.doneColor.value.value;
            if (st.indexOf("progress") >= 0 || st.indexOf("doing") >= 0)
                return s.bars.inProgressColor.value.value;
            if (st.indexOf("to do") >= 0 || st.indexOf("todo") >= 0 || st.indexOf("open") >= 0 ||
                st.indexOf("backlog") >= 0 || st.indexOf("new") >= 0)
                return s.bars.toDoColor.value.value;
            return s.bars.defaultColor.value.value;
        };
        const barH = Math.max(4, Math.round(rowH * 0.55));
        const corner = Math.max(0, s.bars.cornerRadius.value);
        const showLabels = s.bars.showBarLabels.value;
        const mColor = s.milestone.milestoneColor.value.value;
        const mSize = Math.max(4, s.milestone.milestoneSize.value);

        visible.forEach((n, i) => {
            const cy = i * rowH + rowH / 2;
            if (n.isMilestone && n.end) {
                const cx = x(n.end);
                rightSvg.append("path")
                    .attr("d", `M${cx} ${cy - mSize} L${cx + mSize} ${cy} L${cx} ${cy + mSize} L${cx - mSize} ${cy} Z`)
                    .attr("fill", mColor);
                if (showLabels) this.barLabel(rightSvg, cx + mSize + 4, cy, n.label, fontSize, timelineW);
            } else if (n.start && n.end) {
                const x0 = x(n.start);
                const x1 = Math.max(x0 + 2, x(n.end));
                rightSvg.append("rect")
                    .attr("x", x0).attr("y", cy - barH / 2)
                    .attr("width", x1 - x0).attr("height", barH)
                    .attr("rx", corner).attr("ry", corner)
                    .attr("fill", barColor(n));
                if (showLabels) this.barLabel(rightSvg, x1 + 4, cy, n.label, fontSize, timelineW);
            }
        });

        // today line
        if (s.todayLine.show.value) {
            const now = new Date();
            if (now >= domainMin && now <= domainMax) {
                rightSvg.append("line")
                    .attr("x1", x(now)).attr("x2", x(now))
                    .attr("y1", 0).attr("y2", contentH)
                    .attr("stroke", s.todayLine.lineColor.value.value)
                    .attr("stroke-width", 1.5).attr("stroke-dasharray", "4,3");
            }
        }

        // left tree rows
        visible.forEach((n, i) => {
            const row = document.createElement("div");
            row.style.cssText = `position:absolute;top:${i * rowH}px;left:0;` +
                `width:${leftW}px;height:${rowH}px;display:flex;align-items:center;` +
                `box-sizing:border-box;font-size:${fontSize}px;color:${textColor};` +
                (i % 2 === 1 ? "background:#FafafA;" : "");
            left.appendChild(row);

            // name cell (chevron + indent + label)
            const nameCell = document.createElement("div");
            nameCell.style.cssText = `width:${nameW}px;flex:0 0 ${nameW}px;display:flex;` +
                `align-items:center;box-sizing:border-box;` +
                `padding-left:${4 + (n.depth - 1) * INDENT}px;padding-right:6px;overflow:hidden;`;
            const hasChildren = n.children.length > 0;
            const chevron = document.createElement("span");
            chevron.textContent = hasChildren ? (this.collapsed.has(n.id) ? "▶" : "▼") : "";
            chevron.style.cssText = `flex:0 0 12px;width:12px;cursor:pointer;color:#888;` +
                `font-size:${Math.max(7, fontSize - 2)}px;user-select:none;text-align:center;`;
            if (hasChildren) {
                chevron.onclick = () => {
                    if (this.collapsed.has(n.id)) this.collapsed.delete(n.id);
                    else this.collapsed.add(n.id);
                    this.render();
                };
            }
            nameCell.appendChild(chevron);
            const labelSpan = document.createElement("span");
            labelSpan.textContent = n.label;
            labelSpan.title = n.label;
            labelSpan.style.cssText = `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;` +
                (hasChildren ? "font-weight:600;" : "");
            nameCell.appendChild(labelSpan);
            row.appendChild(nameCell);

            const textCell = (text: string, w: number) => {
                const c = document.createElement("div");
                c.textContent = text;
                c.title = text;
                c.style.cssText = `width:${w}px;flex:0 0 ${w}px;padding:0 8px;box-sizing:border-box;` +
                    `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
                return c;
            };
            if (showLead) row.appendChild(textCell(n.lead, leadW));
            if (showRes) row.appendChild(textCell(n.resources, resW));
        });
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

    private renderAxisHeader(svg: any, x: any, timelineW: number, fontSize: number): void {
        const [d0, d1] = x.domain();
        // year band (top half)
        const years = d3.timeYear.range(d3.timeYear.floor(d0), d3.timeYear.offset(d3.timeYear.ceil(d1), 0));
        years.push(d3.timeYear.ceil(d1));
        for (let i = 0; i < years.length - 1; i++) {
            const a = years[i] < d0 ? d0 : years[i];
            const b = years[i + 1] > d1 ? d1 : years[i + 1];
            if (b <= a) continue;
            const xa = x(a), xb = x(b);
            svg.append("text")
                .attr("x", (xa + xb) / 2).attr("y", 13)
                .attr("text-anchor", "middle").attr("fill", "#fff")
                .attr("font-size", `${fontSize + 1}px`).attr("font-weight", 600)
                .text(years[i].getFullYear());
            svg.append("line").attr("x1", xa).attr("x2", xa).attr("y1", 2).attr("y2", HEADER_H)
                .attr("stroke", "rgba(255,255,255,.35)");
        }
        // quarter band (bottom half)
        const quarters = d3.timeMonth.range(d3.timeMonth.floor(d0), d1, 3);
        quarters.forEach(q => {
            const qEnd = d3.timeMonth.offset(q, 3);
            const a = q < d0 ? d0 : q;
            const b = qEnd > d1 ? d1 : qEnd;
            if (b <= a) return;
            const qi = Math.floor(q.getMonth() / 3) + 1;
            svg.append("text")
                .attr("x", (x(a) + x(b)) / 2).attr("y", 31)
                .attr("text-anchor", "middle").attr("fill", "rgba(255,255,255,.9)")
                .attr("font-size", `${fontSize}px`)
                .text("Q" + qi);
            svg.append("line").attr("x1", x(q)).attr("x2", x(q)).attr("y1", 18).attr("y2", HEADER_H)
                .attr("stroke", "rgba(255,255,255,.2)");
        });
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        return this.formattingSettingsService.buildFormattingModel(this.settings);
    }
}
