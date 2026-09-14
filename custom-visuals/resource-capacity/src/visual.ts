"use strict";

import powerbi from "powerbi-visuals-api";
import { FormattingSettingsService } from "powerbi-visuals-utils-formattingmodel";
import { VisualFormattingSettingsModel } from "./settings";

import DataView = powerbi.DataView;
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisualHost = powerbi.extensibility.visual.IVisualHost;
import ISelectionManager = powerbi.extensibility.ISelectionManager;
import ISelectionId = powerbi.visuals.ISelectionId;

interface ItemInfo { code: string; detail: string[]; days: Map<number, number>; selectionIds: ISelectionId[]; }  // detail = the "Detail" field values, in order; days: canonical ymd -> summed value
interface Resource { name: string; items: Map<string, ItemInfo>; selectionIds: ISelectionId[]; }
interface Period { start: Date; label: string; top: string; }

type Gran = "year" | "quarter" | "month" | "week" | "day";
// Only Week and Day are offered -- the resource plan is read at those grains.
const PRESETS: { key: Gran; label: string; px: number }[] = [
    { key: "week", label: "Week", px: 8 }, { key: "day", label: "Day", px: 34 }
];

const MS_DAY = 86400000;
const H1 = 20;
const H2 = 22;

export class Visual implements IVisual {
    private target: HTMLElement;
    private host: IVisualHost;
    private selectionManager: ISelectionManager;
    private fmtService: FormattingSettingsService;
    private settings: VisualFormattingSettingsModel;

    private resources: Resource[] = [];
    private hasValue = false;
    private minDate: Date | null = null;
    private maxDate: Date | null = null;
    private expanded: Set<string> = new Set<string>();
    private pxPerDay = 34;             // zoom level (like the timeline's pxPerDay)
    private dataToken = "";
    private popover: HTMLElement | null = null;
    private vw = 0; private vh = 0;

    constructor(options: VisualConstructorOptions) {
        this.target = options.element;
        this.target.style.overflow = "hidden";
        this.host = options.host;
        this.selectionManager = this.host.createSelectionManager();
        this.fmtService = new FormattingSettingsService();
    }

    public update(options: VisualUpdateOptions): void {
        const dv: DataView = options.dataViews && options.dataViews[0];
        this.settings = this.fmtService.populateFormattingSettingsModel(VisualFormattingSettingsModel, dv);
        const token = dv && dv.table ? `${dv.table.rows.length}|${dv.table.columns.length}` : "";
        if (token !== this.dataToken) { this.expanded.clear(); this.dataToken = token; }
        this.vw = options.viewport.width; this.vh = options.viewport.height;
        this.build(dv);
        this.render();
    }

    private static ymd(d: Date): number { return d.getFullYear() * 10000 + d.getMonth() * 100 + d.getDate(); }
    private static fromYmd(c: number): Date { return new Date(Math.floor(c / 10000), Math.floor(c / 100) % 100, c % 100); }
    private static monday(d: Date): Date {
        const x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
        x.setDate(x.getDate() - ((x.getDay() + 6) % 7));
        return x;
    }

    private build(dv: DataView): void {
        this.resources = []; this.minDate = null; this.maxDate = null; this.hasValue = false;
        if (!dv || !dv.table || !dv.table.columns || !dv.table.rows) return;
        const cols = dv.table.columns;
        const idx = (role: string) => cols.findIndex(c => c.roles && (c.roles as any)[role]);
        const iRes = idx("resource"), iDate = idx("date"), iCode = idx("issueCode"), iValue = idx("value");
        // every column bound to the "Detail" well, in the order the user added them
        const detailIdx: number[] = cols.map((c, i) => (c.roles && (c.roles as any)["detailFields"]) ? i : -1).filter(i => i >= 0);
        this.hasValue = iValue >= 0;
        const asStr = (v: any) => (v === null || v === undefined) ? "" : String(v);
        const asNum = (v: any) => { if (v === null || v === undefined || v === "") return 0; const n = typeof v === "number" ? v : parseFloat(v); return isNaN(n) ? 0 : n; };
        const asDate = (v: any): Date | undefined => {
            if (v === null || v === undefined || v === "") return undefined;
            if (v instanceof Date) return isNaN(v.getTime()) ? undefined : v;
            const d = new Date(v); return isNaN(d.getTime()) ? undefined : d;
        };

        const byName = new Map<string, Resource>();
        const table = dv.table;
        table.rows.forEach((r, rowIndex) => {
            const name = iRes >= 0 ? asStr(r[iRes]) : "";
            const d = iDate >= 0 ? asDate(r[iDate]) : undefined;
            const code = iCode >= 0 ? asStr(r[iCode]) : "";
            if (name === "" || !d || code === "") return;
            const dm = new Date(d.getFullYear(), d.getMonth(), d.getDate());
            if (!this.minDate || dm < this.minDate) this.minDate = dm;
            if (!this.maxDate || dm > this.maxDate) this.maxDate = dm;
            let res = byName.get(name);
            if (!res) { res = { name, items: new Map(), selectionIds: [] }; byName.set(name, res); }
            const sid = this.host.createSelectionIdBuilder().withTable(table, rowIndex).createSelectionId();
            res.selectionIds.push(sid);
            let item = res.items.get(code);
            if (!item) { item = { code, detail: detailIdx.map(i => asStr(r[i])), days: new Map(), selectionIds: [] }; res.items.set(code, item); }
            item.selectionIds.push(sid);
            const c = Visual.ymd(dm);
            const v = iValue >= 0 ? asNum(r[iValue]) : 0;
            item.days.set(c, (item.days.get(c) || 0) + v);
        });
        this.resources = Array.from(byName.values()).sort((a, b) => a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
    }

    /** grain + column width from the continuous zoom (pxPerDay), like the timeline. */
    private grainFromZoom(): { g: Gran; colW: number } {
        const px = this.pxPerDay;
        const minW = Math.max(28, this.settings.appearance.weekWidth.value);
        let g: Gran, daysPer: number;
        if (px >= 20) { g = "day"; daysPer = 1; }
        else if (px >= 5.5) { g = "week"; daysPer = 7; }
        else if (px >= 1.4) { g = "month"; daysPer = 30.4; }
        else if (px >= 0.45) { g = "quarter"; daysPer = 91.3; }
        else { g = "year"; daysPer = 365; }
        return { g, colW: Math.max(minW, Math.min(360, Math.round(px * daysPer))) };
    }

    private buildPeriods(g: Gran): { periods: Period[]; indexOf: (c: number) => number; nowIndex: number } {
        const periods: Period[] = [];
        const min = this.minDate as Date, max = this.maxDate as Date;
        const monthYear = (d: Date) => d.toLocaleDateString(undefined, { month: "short", year: "numeric" });
        const shortMonth = (d: Date) => d.toLocaleDateString(undefined, { month: "short" });
        let indexOf: (c: number) => number;

        if (g === "day") {
            const base = Date.UTC(min.getFullYear(), min.getMonth(), min.getDate());
            const WD = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"];
            const weekTop = (d: Date) => {
                const s = Visual.monday(d), e = new Date(s.getTime() + 6 * MS_DAY);
                return `${monthYear(s)} ${s.getDate()}–${e.getDate()}`;
            };
            for (let t = min.getTime(); t <= max.getTime(); t += MS_DAY) {
                const d = new Date(t);
                periods.push({ start: d, label: `${WD[(d.getDay() + 6) % 7]} ${d.getDate()}`, top: weekTop(d) });
            }
            indexOf = (c) => { const d = Visual.fromYmd(c); return Math.round((Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()) - base) / MS_DAY); };
        } else if (g === "week") {
            const first = Visual.monday(min);
            for (let t = first.getTime(); t <= max.getTime(); t += 7 * MS_DAY) {
                const s = new Date(t), e = new Date(t + 6 * MS_DAY);
                periods.push({ start: s, label: `${s.getDate()}–${e.getDate()}`, top: monthYear(s) });
            }
            const fb = Date.UTC(first.getFullYear(), first.getMonth(), first.getDate());
            indexOf = (c) => { const m = Visual.monday(Visual.fromYmd(c)); return Math.round((Date.UTC(m.getFullYear(), m.getMonth(), m.getDate()) - fb) / (7 * MS_DAY)); };
        } else if (g === "month") {
            const baseY = min.getFullYear(), baseM = min.getMonth();
            let y = baseY, m = baseM;
            while (y < max.getFullYear() || (y === max.getFullYear() && m <= max.getMonth())) {
                const d = new Date(y, m, 1);
                periods.push({ start: d, label: shortMonth(d), top: String(y) });
                m++; if (m > 11) { m = 0; y++; }
            }
            indexOf = (c) => { const d = Visual.fromYmd(c); return (d.getFullYear() - baseY) * 12 + (d.getMonth() - baseM); };
        } else if (g === "quarter") {
            const baseY = min.getFullYear(), baseQ = Math.floor(min.getMonth() / 3);
            const lastIdx = (max.getFullYear() - baseY) * 4 + (Math.floor(max.getMonth() / 3) - baseQ);
            for (let i = 0; i <= lastIdx; i++) {
                const y = baseY + Math.floor((baseQ + i) / 4), q = (baseQ + i) % 4;
                periods.push({ start: new Date(y, q * 3, 1), label: `Q${q + 1}`, top: String(y) });
            }
            indexOf = (c) => { const d = Visual.fromYmd(c); return (d.getFullYear() - baseY) * 4 + (Math.floor(d.getMonth() / 3) - baseQ); };
        } else {
            const baseY = min.getFullYear();
            for (let y = baseY; y <= max.getFullYear(); y++) periods.push({ start: new Date(y, 0, 1), label: String(y), top: "" });
            indexOf = (c) => Visual.fromYmd(c).getFullYear() - baseY;
        }

        const today = new Date();
        const nowIndex = (today >= min && today <= max) ? indexOf(Visual.ymd(today)) : -999;
        return { periods, indexOf, nowIndex };
    }

    private itemPeriodValue(it: ItemInfo, pi: number, indexOf: (c: number) => number): number {
        let v = 0; it.days.forEach((val, c) => { if (indexOf(c) === pi) v += val; }); return v;
    }

    private itemLabel(it: ItemInfo): string {
        const parts = it.detail.filter(x => x !== "");
        return parts.length ? parts.join("  ") : it.code;
    }

    private render(): void {
        const el = this.target;
        this.closePopover();
        while (el.firstChild) el.removeChild(el.firstChild);
        if (this.resources.length === 0 || !this.minDate) {
            const msg = document.createElement("div");
            msg.style.cssText = "padding:12px;font:12px 'Segoe UI';color:#888";
            msg.textContent = "Bind Resource Name, Date and Issue Code. Add any fields to the Detail well " +
                "to control what each item shows; add a Value field for the sum metric. " +
                "Everything shown is driven only by these field wells.";
            el.appendChild(msg);
            return;
        }

        const s = this.settings;
        const sumMode = s.metric.sumValue.value && this.hasValue;
        const det = s.detail;
        const rowH = Math.max(16, s.appearance.rowHeight.value);
        const fontSize = Math.max(8, s.appearance.fontSize.value);
        const textColor = s.appearance.textColor.value.value;
        const headerColor = s.appearance.headerColor.value.value;
        const headerText = s.appearance.headerTextColor.value.value;
        const nameW = Math.max(90, s.appearance.nameWidth.value);
        const gridColor = s.grid.gridColor.value.value;
        const banded = s.grid.bandedRows.value;
        const bandColor = s.grid.bandColor.value.value;
        const todayColor = s.grid.todayColor.value.value;
        const chipBg = s.detail.chipBackground.value.value;
        const chipText = s.detail.chipTextColor.value.value;
        const workingOnly = s.appearance.workingDaysOnly.value;
        const lowMax = s.thresholds.lowMax.value, midMax = s.thresholds.midMax.value;
        const zeroColor = s.thresholds.zeroColor.value.value, lowColor = s.thresholds.lowColor.value.value;
        const midColor = s.thresholds.midColor.value.value, highColor = s.thresholds.highColor.value.value;
        const cellText = s.thresholds.cellTextColor.value.value;
        const conflictAt = s.conflicts.conflictAt.value, minConflicts = s.conflicts.minConflicts.value;
        const conflictColor = s.conflicts.conflictColor.value.value;
        const loadColor = (n: number) => n <= 0 ? zeroColor : (n <= lowMax ? lowColor : (n <= midMax ? midColor : highColor));
        const fmt = (v: number) => sumMode ? String(Math.round(v * 10) / 10) : String(v);

        const { g, colW } = this.grainFromZoom();
        const { periods, indexOf, nowIndex } = this.buildPeriods(g);
        // which period columns to actually show: at day grain, optionally drop Sat/Sun
        const displayed = periods.map((p, pi) => ({ p, pi }))
            .filter(x => !(workingOnly && g === "day" && (x.p.start.getDay() === 0 || x.p.start.getDay() === 6)));

        interface RR { res: Resource; byPeriod: Map<number, ItemInfo[]>; byValue: Map<number, number>; conflicts: number; }
        const rrs: RR[] = [];
        this.resources.forEach(res => {
            const byPeriod = new Map<number, ItemInfo[]>();
            const byValue = new Map<number, number>();
            res.items.forEach(it => {
                const perP = new Map<number, number>();
                it.days.forEach((val, c) => { const pi = indexOf(c); perP.set(pi, (perP.get(pi) || 0) + val); });
                perP.forEach((val, pi) => {
                    (byPeriod.get(pi) || byPeriod.set(pi, []).get(pi))!.push(it);
                    byValue.set(pi, (byValue.get(pi) || 0) + val);
                });
            });
            const metric = (pi: number) => sumMode ? (byValue.get(pi) || 0) : (byPeriod.get(pi) ? byPeriod.get(pi)!.length : 0);
            let conflicts = 0;
            byPeriod.forEach((_l, pi) => { if (metric(pi) >= conflictAt) conflicts++; });
            rrs.push({ res, byPeriod, byValue, conflicts });
        });
        const visibleRRs = rrs.filter(rr => rr.conflicts >= minConflicts);
        const metricOf = (rr: RR, pi: number) => sumMode ? (rr.byValue.get(pi) || 0) : (rr.byPeriod.get(pi) ? rr.byPeriod.get(pi)!.length : 0);

        const container = document.createElement("div");
        container.style.cssText = `width:${this.vw}px;height:${this.vh}px;display:flex;flex-direction:column;` +
            `font-family:'Segoe UI',sans-serif;box-sizing:border-box;overflow:hidden;`;
        el.appendChild(container);

        // ---- toolbar: zoom presets + summary ----
        const toolbar = document.createElement("div");
        toolbar.style.cssText = `flex:0 0 auto;display:flex;align-items:center;gap:6px;padding:4px 8px;` +
            `box-sizing:border-box;font-size:${fontSize}px;color:${textColor};`;
        const gl = document.createElement("span"); gl.textContent = "Zoom:"; gl.style.cssText = "color:#888;margin-right:2px;";
        toolbar.appendChild(gl);
        PRESETS.forEach(p => {
            const b = document.createElement("div");
            b.textContent = p.label;
            const active = g === p.key;
            b.style.cssText = `padding:2px 8px;border-radius:3px;cursor:pointer;user-select:none;` +
                `border:1px solid ${active ? headerColor : "#CCC"};background:${active ? headerColor : "#FFF"};color:${active ? "#FFF" : "#333"};`;
            b.onclick = () => { this.pxPerDay = p.px; this.render(); };
            toolbar.appendChild(b);
        });
        const totalConf = visibleRRs.reduce((a, r) => a + r.conflicts, 0);
        const cs = document.createElement("span");
        cs.textContent = `${totalConf} conflict${totalConf === 1 ? "" : "s"} · ${visibleRRs.length} people`;
        cs.style.cssText = `margin-left:auto;color:${conflictColor};font-weight:600;`;
        toolbar.appendChild(cs);
        container.appendChild(toolbar);

        const legend = s.legend.show.value ? this.buildLegend(fontSize, textColor, lowMax, midMax, lowColor, midColor, highColor, conflictColor) : null;
        if (legend && !s.legend.atBottom.value) container.appendChild(legend);

        const scroller = document.createElement("div");
        scroller.style.cssText = "flex:1 1 auto;overflow:auto;position:relative;";
        container.appendChild(scroller);
        // No wheel-zoom: the wheel scrolls the table normally; zoom is by the buttons.

        const table = document.createElement("table");
        table.style.cssText = `border-collapse:separate;border-spacing:0;table-layout:fixed;font-size:${fontSize}px;color:${textColor};`;
        scroller.appendChild(table);

        const twoRow = g !== "year";
        const thead = document.createElement("thead");
        table.appendChild(thead);

        if (twoRow) {
            const topRow = document.createElement("tr");
            const corner = document.createElement("th");
            corner.textContent = "Resource"; corner.rowSpan = 2;
            corner.style.cssText = `position:sticky;left:0;top:0;z-index:5;width:${nameW}px;min-width:${nameW}px;` +
                `height:${H1}px;background:${headerColor};color:${headerText};text-align:left;padding:0 8px;box-sizing:border-box;font-weight:600;`;
            topRow.appendChild(corner);
            let i = 0;
            while (i < displayed.length) {
                const label = displayed[i].p.top; let span = 1;
                while (i + span < displayed.length && displayed[i + span].p.top === label) span++;
                const th = document.createElement("th");
                th.textContent = label; th.colSpan = span;
                th.style.cssText = `position:sticky;top:0;z-index:3;height:${H1}px;background:${headerColor};color:${headerText};` +
                    `font-weight:600;text-align:center;border-left:1px solid rgba(255,255,255,.35);box-sizing:border-box;`;
                topRow.appendChild(th); i += span;
            }
            thead.appendChild(topRow);
        }

        const labelRow = document.createElement("tr");
        if (!twoRow) {
            const corner = document.createElement("th");
            corner.textContent = "Resource";
            corner.style.cssText = `position:sticky;left:0;top:0;z-index:5;width:${nameW}px;min-width:${nameW}px;` +
                `height:${H2}px;background:${headerColor};color:${headerText};text-align:left;padding:0 8px;box-sizing:border-box;font-weight:600;`;
            labelRow.appendChild(corner);
        }
        displayed.forEach(({ p, pi }) => {
            const th = document.createElement("th");
            th.textContent = p.label; th.title = p.start.toLocaleDateString();
            const bg = pi === nowIndex ? todayColor : headerColor;
            const top = twoRow ? `top:${H1}px;` : "top:0;";
            th.style.cssText = `position:sticky;${top}z-index:3;width:${colW}px;min-width:${colW}px;height:${H2}px;` +
                `background:${bg};color:${headerText};text-align:center;font-weight:400;overflow:hidden;` +
                `border-left:1px solid rgba(255,255,255,.25);box-sizing:border-box;`;
            labelRow.appendChild(th);
        });
        thead.appendChild(labelRow);

        const tbody = document.createElement("tbody");
        table.appendChild(tbody);

        visibleRRs.forEach((rr, ri) => {
            const res = rr.res;
            const rowBg = banded && ri % 2 === 1 ? bandColor : "#FFFFFF";
            const isExp = this.expanded.has(res.name);
            const tr = document.createElement("tr");

            const nameCell = document.createElement("th");
            nameCell.style.cssText = `position:sticky;left:0;z-index:1;width:${nameW}px;min-width:${nameW}px;height:${rowH}px;` +
                `background:${rowBg};text-align:left;padding:3px 6px;box-sizing:border-box;vertical-align:top;` +
                `border-bottom:1px solid ${gridColor};border-right:1px solid ${gridColor};font-weight:600;` +
                `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
            const chev = document.createElement("span");
            chev.textContent = res.items.size > 0 ? (isExp ? "▼ " : "▶ ") : "";
            chev.style.cssText = "cursor:pointer;color:#888;user-select:none;";
            chev.onclick = () => { if (isExp) this.expanded.delete(res.name); else this.expanded.add(res.name); this.render(); };
            nameCell.appendChild(chev);
            const nm = document.createElement("span");
            nm.textContent = res.name;
            nm.title = res.name + "  —  click to cross-filter other visuals (Ctrl+click to multi-select)";
            nm.style.cssText = "cursor:pointer;";
            nm.onclick = (ev) => {
                ev.stopPropagation();
                this.selectionManager.select(res.selectionIds, (ev as MouseEvent).ctrlKey || (ev as MouseEvent).metaKey);
            };
            nameCell.appendChild(nm);
            if (rr.conflicts > 0) {
                const badge = document.createElement("span");
                badge.textContent = String(rr.conflicts);
                badge.title = `${rr.conflicts} conflict period(s) — click for detail`;
                badge.style.cssText = `margin-left:6px;padding:0 5px;border-radius:8px;background:${conflictColor};` +
                    `color:#fff;font-size:${Math.max(7, fontSize - 2)}px;cursor:pointer;`;
                badge.onclick = (ev) => this.showConflictDetail(res, rr.byPeriod, periods, conflictAt, sumMode, indexOf, ev as MouseEvent, textColor, headerColor, conflictColor, fontSize);
                nameCell.appendChild(badge);
            }
            tr.appendChild(nameCell);

            const CAP = 30;
            displayed.forEach(({ pi }) => {
                const list = rr.byPeriod.get(pi) || [];
                const val = metricOf(rr, pi);
                const isConf = val >= conflictAt;
                const conf = isConf ? `box-shadow:inset 0 0 0 2px ${conflictColor};` : "";
                const td = document.createElement("td");

                if (!isExp) {
                    td.textContent = val > 0 ? fmt(val) : "";
                    td.title = val > 0 ? `${res.name}: ${fmt(val)} — click to open` : "";
                    td.style.cssText = `width:${colW}px;min-width:${colW}px;height:${rowH}px;text-align:center;` +
                        `background:${loadColor(val)};color:${cellText};box-sizing:border-box;font-weight:${isConf ? 700 : 400};` +
                        `border-bottom:1px solid ${gridColor};border-left:1px solid ${gridColor};${conf}` +
                        (list.length > 0 ? "cursor:pointer;" : "");
                    if (list.length > 0) td.onclick = () => { this.expanded.add(res.name); this.render(); };
                } else {
                    td.style.cssText = `width:${colW}px;min-width:${colW}px;vertical-align:top;` +
                        `background:${loadColor(val)};color:${cellText};box-sizing:border-box;` +
                        `border-bottom:1px solid ${gridColor};border-left:1px solid ${gridColor};${conf}cursor:pointer;padding:2px;`;
                    if (list.length > 0) {
                        const cnt = document.createElement("div");
                        cnt.textContent = fmt(val);
                        cnt.style.cssText = `text-align:center;font-weight:700;margin-bottom:2px;font-size:${Math.max(7, fontSize - 1)}px;` + (isConf ? `color:${conflictColor};` : "");
                        td.appendChild(cnt);
                        const sorted = list.slice().sort((a, b) => a.code < b.code ? -1 : 1);
                        sorted.slice(0, CAP).forEach(it => {
                            const chip = document.createElement("div");
                            // the chip shows exactly the fields dropped into the "Detail" well
                            const parts = it.detail.filter(x => x !== "");
                            if (sumMode && det.showValue.value) parts.push(String(Math.round(this.itemPeriodValue(it, pi, indexOf) * 10) / 10));
                            const label = parts.length ? parts.join("  ") : it.code;
                            chip.textContent = label;
                            chip.title = label + "  —  click to show details in linked visuals";
                            chip.style.cssText = `background:${chipBg};border:1px solid ${gridColor};border-radius:2px;margin:1px 0;padding:0 3px;color:${chipText};` +
                                `font-size:${Math.max(7, fontSize - 2)}px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:pointer;`;
                            // click an item -> cross-filter other visuals to that item (detail elsewhere)
                            chip.onclick = (ev) => {
                                ev.stopPropagation();
                                this.selectionManager.select(it.selectionIds, (ev as MouseEvent).ctrlKey || (ev as MouseEvent).metaKey);
                            };
                            td.appendChild(chip);
                        });
                        if (list.length > CAP) {
                            const more = document.createElement("div");
                            more.textContent = `+${list.length - CAP} more`;
                            more.style.cssText = `text-align:center;color:#1a6db0;cursor:pointer;font-size:${Math.max(7, fontSize - 2)}px;`;
                            more.onclick = (ev) => {
                                ev.stopPropagation();
                                this.openPopover(`${res.name} — ${periods[pi].top ? periods[pi].top + " " : ""}${periods[pi].label}`,
                                    `${list.length} items`, sorted.map(it => ({ code: it.code, name: this.itemLabel(it), extra: "" })),
                                    ev as MouseEvent, textColor, headerColor, conflictColor, fontSize);
                            };
                            td.appendChild(more);
                        }
                    }
                    td.onclick = (ev) => { if (ev.target === td) { this.expanded.delete(res.name); this.render(); } };
                }
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });

        if (legend && s.legend.atBottom.value) container.appendChild(legend);
    }

    private closePopover(): void {
        if (this.popover && this.popover.parentNode) this.popover.parentNode.removeChild(this.popover);
        this.popover = null;
    }

    private showConflictDetail(res: Resource, byPeriod: Map<number, ItemInfo[]>, periods: Period[], conflictAt: number,
        sumMode: boolean, indexOf: (c: number) => number,
        ev: MouseEvent, textColor: string, headerColor: string, conflictColor: string, fontSize: number): void {
        const rows: { code: string; name: string; extra: string }[] = [];
        byPeriod.forEach((list, pi) => {
            const metric = sumMode ? list.reduce((a, it) => a + this.itemPeriodValue(it, pi, indexOf), 0) : list.length;
            if (metric >= conflictAt) {
                const p = periods[pi];
                list.slice().sort((a, b) => a.code < b.code ? -1 : 1)
                    .forEach(it => rows.push({ code: it.code, name: this.itemLabel(it), extra: p ? `${p.top ? p.top + " " : ""}${p.label}` : "" }));
            }
        });
        this.openPopover(`${res.name} — conflicts`, `${rows.length} item-instances in conflict periods`,
            rows, ev, textColor, headerColor, conflictColor, fontSize);
    }

    private openPopover(title: string, subtitle: string, rows: { code: string; name: string; extra: string }[],
        ev: MouseEvent, textColor: string, headerColor: string, conflictColor: string, fontSize: number): void {
        this.closePopover();
        ev.stopPropagation();
        const pop = document.createElement("div");
        const maxW = 340, maxH = 300;
        let left = ev.clientX + 8, top = ev.clientY + 8;
        if (left + maxW > this.vw) left = Math.max(4, this.vw - maxW - 4);
        if (top + maxH > this.vh) top = Math.max(4, this.vh - maxH - 4);
        pop.style.cssText = `position:fixed;left:${left}px;top:${top}px;width:${maxW}px;max-height:${maxH}px;` +
            `background:#fff;border:1px solid #CCC;border-radius:4px;box-shadow:0 4px 16px rgba(0,0,0,.2);` +
            `z-index:1000;display:flex;flex-direction:column;font-size:${fontSize}px;color:${textColor};overflow:hidden;`;
        pop.onclick = (e) => e.stopPropagation();

        const head = document.createElement("div");
        head.style.cssText = `background:${headerColor};color:#fff;padding:6px 8px;display:flex;align-items:center;`;
        const ht = document.createElement("div");
        ht.style.cssText = "flex:1 1 auto;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;";
        ht.textContent = title; head.appendChild(ht);
        const close = document.createElement("div");
        close.textContent = "✕"; close.style.cssText = "cursor:pointer;padding:0 2px;";
        close.onclick = () => this.closePopover(); head.appendChild(close);
        pop.appendChild(head);

        const sub = document.createElement("div");
        sub.style.cssText = `padding:3px 8px;color:${conflictColor};border-bottom:1px solid #EEE;`;
        sub.textContent = subtitle; pop.appendChild(sub);

        const listWrap = document.createElement("div");
        listWrap.style.cssText = "overflow:auto;padding:2px 0;";
        rows.forEach(r => {
            const row = document.createElement("div");
            row.style.cssText = "padding:3px 8px;border-bottom:1px solid #F3F3F3;display:flex;gap:6px;";
            const code = document.createElement("span");
            code.textContent = r.code; code.style.cssText = "font-weight:600;flex:0 0 auto;color:#B21C1A;";
            row.appendChild(code);
            const nm = document.createElement("span");
            nm.textContent = r.name + (r.extra ? `  (${r.extra})` : "");
            nm.style.cssText = "flex:1 1 auto;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;";
            nm.title = r.name + (r.extra ? `  (${r.extra})` : "");
            row.appendChild(nm);
            listWrap.appendChild(row);
        });
        pop.appendChild(listWrap);

        this.target.appendChild(pop);
        this.popover = pop;
        setTimeout(() => {
            const onDoc = () => { this.closePopover(); document.removeEventListener("click", onDoc); };
            document.addEventListener("click", onDoc);
        }, 0);
    }

    private buildLegend(fontSize: number, textColor: string, lowMax: number, midMax: number,
        lowColor: string, midColor: string, highColor: string, conflictColor: string): HTMLElement {
        const legend = document.createElement("div");
        legend.style.cssText = `flex:0 0 auto;display:flex;flex-wrap:wrap;align-items:center;gap:4px 14px;` +
            `padding:4px 10px;box-sizing:border-box;font-size:${fontSize}px;color:${textColor};`;
        const item = (color: string, text: string, outline?: boolean) => {
            const wrap = document.createElement("div");
            wrap.style.cssText = "display:flex;align-items:center;gap:5px;";
            const box = document.createElement("span");
            box.style.cssText = `width:12px;height:12px;border-radius:2px;background:${color};display:inline-block;flex:0 0 12px;` +
                (outline ? `box-shadow:inset 0 0 0 2px ${conflictColor};` : "border:1px solid rgba(0,0,0,.12);");
            wrap.appendChild(box);
            const t = document.createElement("span"); t.textContent = text; wrap.appendChild(t);
            legend.appendChild(wrap);
        };
        item(lowColor, `1–${lowMax}`);
        item(midColor, `${lowMax + 1}–${midMax}`);
        item(highColor, `${midMax + 1}+`);
        item(highColor, "conflict", true);
        return legend;
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        return this.fmtService.buildFormattingModel(this.settings);
    }
}
