"use strict";

import powerbi from "powerbi-visuals-api";
import { FormattingSettingsService } from "powerbi-visuals-utils-formattingmodel";
import { VisualFormattingSettingsModel } from "./settings";

import DataView = powerbi.DataView;
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;

interface ItemInfo { code: string; name: string; weeks: Set<number>; }
interface Resource { name: string; items: Map<string, ItemInfo>; }

const MS_DAY = 86400000;
const H1 = 20;   // month header row height
const H2 = 22;   // week header row height

export class Visual implements IVisual {
    private target: HTMLElement;
    private fmtService: FormattingSettingsService;
    private settings: VisualFormattingSettingsModel;

    private resources: Resource[] = [];
    private weeks: Date[] = [];
    private firstMonday: Date | null = null;
    private expanded: Set<string> = new Set<string>();
    private dataToken = "";

    constructor(options: VisualConstructorOptions) {
        this.target = options.element;
        this.target.style.overflow = "hidden";
        this.fmtService = new FormattingSettingsService();
    }

    public update(options: VisualUpdateOptions): void {
        const dv: DataView = options.dataViews && options.dataViews[0];
        this.settings = this.fmtService.populateFormattingSettingsModel(VisualFormattingSettingsModel, dv);

        const token = dv && dv.table ? `${dv.table.rows.length}|${dv.table.columns.length}` : "";
        if (token !== this.dataToken) { this.expanded.clear(); this.dataToken = token; }

        this.build(dv);
        this.render(options.viewport.width, options.viewport.height);
    }

    private static monday(d: Date): Date {
        const x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
        const wd = (x.getDay() + 6) % 7;   // 0 = Monday
        x.setDate(x.getDate() - wd);
        return x;
    }

    private build(dv: DataView): void {
        this.resources = [];
        this.weeks = [];
        this.firstMonday = null;
        if (!dv || !dv.table || !dv.table.columns || !dv.table.rows) return;

        const cols = dv.table.columns;
        const idx = (role: string) => cols.findIndex(c => c.roles && (c.roles as any)[role]);
        const iRes = idx("resource"), iDate = idx("date"), iCode = idx("issueCode"),
            iName = idx("itemName");

        const asStr = (v: any) => (v === null || v === undefined) ? "" : String(v);
        const asDate = (v: any): Date | undefined => {
            if (v === null || v === undefined || v === "") return undefined;
            if (v instanceof Date) return isNaN(v.getTime()) ? undefined : v;
            const d = new Date(v);
            return isNaN(d.getTime()) ? undefined : d;
        };

        // date range -> weeks (Monday-aligned)
        let minM: Date | null = null, maxM: Date | null = null;
        for (const r of dv.table.rows) {
            const d = iDate >= 0 ? asDate(r[iDate]) : undefined;
            if (!d) continue;
            const m = Visual.monday(d);
            if (!minM || m < minM) minM = m;
            if (!maxM || m > maxM) maxM = m;
        }
        if (!minM || !maxM) return;
        this.firstMonday = minM;
        for (let t = minM.getTime(); t <= maxM.getTime(); t += 7 * MS_DAY) this.weeks.push(new Date(t));

        const weekIndex = (d: Date): number =>
            Math.round((Visual.monday(d).getTime() - (this.firstMonday as Date).getTime()) / (7 * MS_DAY));

        const byName = new Map<string, Resource>();
        for (const r of dv.table.rows) {
            const name = iRes >= 0 ? asStr(r[iRes]) : "";
            const d = iDate >= 0 ? asDate(r[iDate]) : undefined;
            const code = iCode >= 0 ? asStr(r[iCode]) : "";
            if (name === "" || !d || code === "") continue;
            const wi = weekIndex(d);
            let res = byName.get(name);
            if (!res) { res = { name, items: new Map() }; byName.set(name, res); }
            let item = res.items.get(code);
            if (!item) {
                item = { code, name: iName >= 0 ? asStr(r[iName]) : code, weeks: new Set() };
                res.items.set(code, item);
            }
            item.weeks.add(wi);
        }
        this.resources = Array.from(byName.values())
            .sort((a, b) => a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
    }

    private weekCount(res: Resource, wi: number): number {
        let n = 0;
        res.items.forEach(it => { if (it.weeks.has(wi)) n++; });
        return n;
    }

    private render(width: number, height: number): void {
        const el = this.target;
        while (el.firstChild) el.removeChild(el.firstChild);
        if (this.resources.length === 0 || this.weeks.length === 0) {
            const msg = document.createElement("div");
            msg.style.cssText = "padding:12px;font:12px 'Segoe UI';color:#888";
            msg.textContent = "Bind Resource Name, Date and Issue Code (Item Name optional). " +
                "Use Fact Resource Day Allocation for Date, Dim Resource for the name, Dim Issue for the codes.";
            el.appendChild(msg);
            return;
        }

        const s = this.settings;
        const rowH = Math.max(16, s.appearance.rowHeight.value);
        const fontSize = Math.max(8, s.appearance.fontSize.value);
        const textColor = s.appearance.textColor.value.value;
        const headerColor = s.appearance.headerColor.value.value;
        const nameW = Math.max(90, s.appearance.nameWidth.value);
        const weekW = Math.max(40, s.appearance.weekWidth.value);
        const gridColor = s.grid.gridColor.value.value;
        const banded = s.grid.bandedRows.value;
        const bandColor = s.grid.bandColor.value.value;
        const lowMax = s.thresholds.lowMax.value;
        const midMax = s.thresholds.midMax.value;
        const zeroColor = s.thresholds.zeroColor.value.value;
        const lowColor = s.thresholds.lowColor.value.value;
        const midColor = s.thresholds.midColor.value.value;
        const highColor = s.thresholds.highColor.value.value;
        const cellText = s.thresholds.cellTextColor.value.value;

        const loadColor = (n: number): string =>
            n <= 0 ? zeroColor : (n <= lowMax ? lowColor : (n <= midMax ? midColor : highColor));

        const nowWeek = s.grid.showTodayLine.value && this.firstMonday
            ? Math.round((Visual.monday(new Date()).getTime() - this.firstMonday.getTime()) / (7 * MS_DAY))
            : -999;

        const container = document.createElement("div");
        container.style.cssText = `width:${width}px;height:${height}px;display:flex;flex-direction:column;` +
            `font-family:'Segoe UI',sans-serif;box-sizing:border-box;overflow:hidden;`;
        el.appendChild(container);

        // legend node (inserted top or bottom)
        const legend = s.legend.show.value ? this.buildLegend(fontSize, textColor,
            lowMax, midMax, lowColor, midColor, highColor) : null;
        if (legend && !s.legend.atBottom.value) container.appendChild(legend);

        const scroller = document.createElement("div");
        scroller.style.cssText = `flex:1 1 auto;overflow:auto;position:relative;`;
        container.appendChild(scroller);

        const table = document.createElement("table");
        table.style.cssText = `border-collapse:separate;border-spacing:0;table-layout:fixed;` +
            `font-size:${fontSize}px;color:${textColor};`;
        scroller.appendChild(table);

        // ---- header ----
        const thead = document.createElement("thead");
        table.appendChild(thead);

        // month row (row1): group consecutive weeks by month-year
        const monthRow = document.createElement("tr");
        const corner = document.createElement("th");
        corner.textContent = "Resource";
        corner.style.cssText = `position:sticky;left:0;top:0;z-index:5;width:${nameW}px;min-width:${nameW}px;` +
            `height:${H1}px;background:${headerColor};color:#fff;text-align:left;padding:0 8px;` +
            `box-sizing:border-box;font-weight:600;`;
        corner.rowSpan = 2;
        monthRow.appendChild(corner);

        const monthLabel = (d: Date) => d.toLocaleDateString(undefined, { month: "short", year: "numeric" });
        let i = 0;
        while (i < this.weeks.length) {
            const label = monthLabel(this.weeks[i]);
            let span = 1;
            while (i + span < this.weeks.length && monthLabel(this.weeks[i + span]) === label) span++;
            const th = document.createElement("th");
            th.textContent = label;
            th.colSpan = span;
            th.style.cssText = `position:sticky;top:0;z-index:3;height:${H1}px;background:${headerColor};` +
                `color:#fff;font-weight:600;text-align:center;border-left:1px solid rgba(255,255,255,.35);` +
                `box-sizing:border-box;`;
            monthRow.appendChild(th);
            i += span;
        }
        thead.appendChild(monthRow);

        // week row (row2): day range of each week
        const weekRow = document.createElement("tr");
        this.weeks.forEach((wk, wi) => {
            const end = new Date(wk.getTime() + 6 * MS_DAY);
            const th = document.createElement("th");
            th.textContent = `${wk.getDate()}–${end.getDate()}`;
            th.title = `${wk.toLocaleDateString()} – ${end.toLocaleDateString()}`;
            const todayBg = wi === nowWeek ? "background:#8f1714;" : `background:${headerColor};`;
            th.style.cssText = `position:sticky;top:${H1}px;z-index:3;width:${weekW}px;min-width:${weekW}px;` +
                `height:${H2}px;${todayBg}color:#fff;text-align:center;font-weight:400;` +
                `border-left:1px solid rgba(255,255,255,.25);box-sizing:border-box;`;
            weekRow.appendChild(th);
        });
        thead.appendChild(weekRow);

        // ---- body ----
        const tbody = document.createElement("tbody");
        table.appendChild(tbody);

        this.resources.forEach((res, ri) => {
            const rowBg = banded && ri % 2 === 1 ? bandColor : "#FFFFFF";
            const tr = document.createElement("tr");

            // sticky name cell with chevron
            const nameCell = document.createElement("th");
            nameCell.style.cssText = `position:sticky;left:0;z-index:1;width:${nameW}px;min-width:${nameW}px;` +
                `height:${rowH}px;background:${rowBg};text-align:left;padding:0 6px;box-sizing:border-box;` +
                `border-bottom:1px solid ${gridColor};border-right:1px solid ${gridColor};` +
                `font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
            const chev = document.createElement("span");
            const isExp = this.expanded.has(res.name);
            chev.textContent = res.items.size > 0 ? (isExp ? "▼ " : "▶ ") : "";
            chev.style.cssText = "cursor:pointer;color:#888;user-select:none;";
            chev.onclick = () => {
                if (this.expanded.has(res.name)) this.expanded.delete(res.name);
                else this.expanded.add(res.name);
                this.render(width, height);
            };
            nameCell.appendChild(chev);
            const nm = document.createElement("span");
            nm.textContent = res.name;
            nm.title = res.name;
            nameCell.appendChild(nm);
            tr.appendChild(nameCell);

            // week cells (counts)
            this.weeks.forEach((_wk, wi) => {
                const n = this.weekCount(res, wi);
                const td = document.createElement("td");
                td.textContent = n > 0 ? String(n) : "";
                td.title = n > 0 ? `${res.name}: ${n} item${n === 1 ? "" : "s"}` : "";
                const todayEdge = wi === nowWeek ? `box-shadow: inset 0 0 0 1px ${highColor};` : "";
                td.style.cssText = `width:${weekW}px;min-width:${weekW}px;height:${rowH}px;text-align:center;` +
                    `background:${loadColor(n)};color:${cellText};box-sizing:border-box;` +
                    `border-bottom:1px solid ${gridColor};border-left:1px solid ${gridColor};${todayEdge}`;
                tr.appendChild(td);
            });
            tbody.appendChild(tr);

            // expanded item rows
            if (isExp) {
                const items = Array.from(res.items.values())
                    .sort((a, b) => a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
                items.forEach(it => {
                    const itr = document.createElement("tr");
                    const ic = document.createElement("th");
                    ic.style.cssText = `position:sticky;left:0;z-index:1;width:${nameW}px;min-width:${nameW}px;` +
                        `height:${rowH}px;background:${rowBg};text-align:left;padding:0 6px 0 22px;box-sizing:border-box;` +
                        `border-bottom:1px solid ${gridColor};border-right:1px solid ${gridColor};` +
                        `font-weight:400;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:${textColor};`;
                    ic.textContent = it.name;
                    ic.title = `${it.code}: ${it.name}`;
                    itr.appendChild(ic);
                    this.weeks.forEach((_wk, wi) => {
                        const active = it.weeks.has(wi);
                        const td = document.createElement("td");
                        td.textContent = active ? it.code : "";
                        td.title = active ? `${it.code}: ${it.name}` : "";
                        td.style.cssText = `width:${weekW}px;min-width:${weekW}px;height:${rowH}px;text-align:center;` +
                            `font-size:${Math.max(7, fontSize - 2)}px;color:#666;box-sizing:border-box;` +
                            `background:${active ? lowColor : rowBg};` +
                            `border-bottom:1px solid ${gridColor};border-left:1px solid ${gridColor};` +
                            `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
                        itr.appendChild(td);
                    });
                    tbody.appendChild(itr);
                });
            }
        });

        if (legend && s.legend.atBottom.value) container.appendChild(legend);
    }

    private buildLegend(fontSize: number, textColor: string, lowMax: number, midMax: number,
        lowColor: string, midColor: string, highColor: string): HTMLElement {
        const legend = document.createElement("div");
        legend.style.cssText = `flex:0 0 auto;display:flex;flex-wrap:wrap;align-items:center;gap:4px 14px;` +
            `padding:4px 10px;box-sizing:border-box;font-size:${fontSize}px;color:${textColor};`;
        const item = (color: string, text: string) => {
            const wrap = document.createElement("div");
            wrap.style.cssText = "display:flex;align-items:center;gap:5px;";
            const box = document.createElement("span");
            box.style.cssText = `width:12px;height:12px;border-radius:2px;background:${color};` +
                `display:inline-block;flex:0 0 12px;border:1px solid rgba(0,0,0,.12);`;
            wrap.appendChild(box);
            const t = document.createElement("span");
            t.textContent = text;
            wrap.appendChild(t);
            legend.appendChild(wrap);
        };
        item(lowColor, `1–${lowMax} items`);
        item(midColor, `${lowMax + 1}–${midMax} items`);
        item(highColor, `${midMax + 1}+ items (conflict)`);
        return legend;
    }

    public getFormattingModel(): powerbi.visuals.FormattingModel {
        return this.fmtService.buildFormattingModel(this.settings);
    }
}
