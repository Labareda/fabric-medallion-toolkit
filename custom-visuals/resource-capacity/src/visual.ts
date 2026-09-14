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

interface ItemInfo { code: string; name: string; days: Set<number>; }   // days = canonical y*10000+m*100+d
interface Resource { name: string; items: Map<string, ItemInfo>; selectionIds: ISelectionId[]; }
interface Period { start: Date; label: string; top: string; }

type Gran = "year" | "quarter" | "month" | "week" | "day";
const GRANS: { key: Gran; label: string }[] = [
    { key: "year", label: "Year" }, { key: "quarter", label: "Quarter" },
    { key: "month", label: "Month" }, { key: "week", label: "Week" }, { key: "day", label: "Day" }
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
    private minDate: Date | null = null;
    private maxDate: Date | null = null;
    private expanded: Set<string> = new Set<string>();
    private granularity: Gran = "day";
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
        this.resources = []; this.minDate = null; this.maxDate = null;
        if (!dv || !dv.table || !dv.table.columns || !dv.table.rows) return;
        const cols = dv.table.columns;
        const idx = (role: string) => cols.findIndex(c => c.roles && (c.roles as any)[role]);
        const iRes = idx("resource"), iDate = idx("date"), iCode = idx("issueCode"), iName = idx("itemName");
        const asStr = (v: any) => (v === null || v === undefined) ? "" : String(v);
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
            // one selection id per source row -> selecting a person cross-filters
            // every allocation row that belongs to them (the count comes straight
            // from these fact rows, so it always matches slicer-filtered data).
            res.selectionIds.push(
                this.host.createSelectionIdBuilder().withTable(table, rowIndex).createSelectionId()
            );
            let item = res.items.get(code);
            if (!item) { item = { code, name: iName >= 0 ? asStr(r[iName]) : code, days: new Set() }; res.items.set(code, item); }
            item.days.add(Visual.ymd(dm));
        });
        this.resources = Array.from(byName.values()).sort((a, b) => a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
    }

    // Build the ordered list of periods for the current grain + an index() mapper.
    private buildPeriods(): { periods: Period[]; indexOf: (c: number) => number; nowIndex: number } {
        const periods: Period[] = [];
        const min = this.minDate as Date, max = this.maxDate as Date;
        const g = this.granularity;
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

    private render(): void {
        const el = this.target;
        this.closePopover();
        while (el.firstChild) el.removeChild(el.firstChild);
        if (this.resources.length === 0 || !this.minDate) {
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
        const colW = Math.max(34, s.appearance.weekWidth.value);
        const gridColor = s.grid.gridColor.value.value;
        const banded = s.grid.bandedRows.value;
        const bandColor = s.grid.bandColor.value.value;
        const lowMax = s.thresholds.lowMax.value, midMax = s.thresholds.midMax.value;
        const zeroColor = s.thresholds.zeroColor.value.value, lowColor = s.thresholds.lowColor.value.value;
        const midColor = s.thresholds.midColor.value.value, highColor = s.thresholds.highColor.value.value;
        const cellText = s.thresholds.cellTextColor.value.value;
        const conflictAt = s.conflicts.conflictAt.value, minConflicts = s.conflicts.minConflicts.value;
        const conflictColor = s.conflicts.conflictColor.value.value;
        const loadColor = (n: number) => n <= 0 ? zeroColor : (n <= lowMax ? lowColor : (n <= midMax ? midColor : highColor));

        const { periods, indexOf, nowIndex } = this.buildPeriods();

        // per resource: items grouped by period index (for counts + detail)
        interface RR { res: Resource; byPeriod: Map<number, ItemInfo[]>; conflicts: number; }
        const rrs: RR[] = [];
        this.resources.forEach(res => {
            const byPeriod = new Map<number, ItemInfo[]>();
            res.items.forEach(it => {
                const seen = new Set<number>();
                it.days.forEach(c => { const pi = indexOf(c); if (!seen.has(pi)) { seen.add(pi); (byPeriod.get(pi) || byPeriod.set(pi, []).get(pi))!.push(it); } });
            });
            let conflicts = 0;
            byPeriod.forEach(list => { if (list.length >= conflictAt) conflicts++; });
            rrs.push({ res, byPeriod, conflicts });
        });
        const visibleRRs = rrs.filter(rr => rr.conflicts >= minConflicts);

        const container = document.createElement("div");
        container.style.cssText = `width:${this.vw}px;height:${this.vh}px;display:flex;flex-direction:column;` +
            `font-family:'Segoe UI',sans-serif;box-sizing:border-box;overflow:hidden;`;
        el.appendChild(container);

        // ---- toolbar: grain buttons + conflict summary ----
        const toolbar = document.createElement("div");
        toolbar.style.cssText = `flex:0 0 auto;display:flex;align-items:center;gap:6px;padding:4px 8px;` +
            `box-sizing:border-box;font-size:${fontSize}px;color:${textColor};`;
        const gl = document.createElement("span");
        gl.textContent = "Zoom:"; gl.style.cssText = "color:#888;margin-right:2px;";
        toolbar.appendChild(gl);
        GRANS.forEach(gr => {
            const b = document.createElement("div");
            b.textContent = gr.label;
            const active = this.granularity === gr.key;
            b.style.cssText = `padding:2px 8px;border-radius:3px;cursor:pointer;user-select:none;` +
                `border:1px solid ${active ? headerColor : "#CCC"};` +
                `background:${active ? headerColor : "#FFF"};color:${active ? "#FFF" : "#333"};`;
            b.onclick = () => { this.granularity = gr.key; this.render(); };
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

        // Ctrl + mouse wheel zooms the date grain (day <-> week <-> month <->
        // quarter <-> year), like the timeline. Plain wheel still scrolls the grid.
        const order: Gran[] = ["year", "quarter", "month", "week", "day"];
        scroller.addEventListener("wheel", (ev: WheelEvent) => {
            if (!(ev.ctrlKey || ev.metaKey)) return;
            ev.preventDefault();
            const i = order.indexOf(this.granularity);
            const ni = ev.deltaY < 0 ? Math.min(order.length - 1, i + 1) : Math.max(0, i - 1);
            if (ni !== i) { this.granularity = order[ni]; this.render(); }
        }, { passive: false });

        const table = document.createElement("table");
        table.style.cssText = `border-collapse:separate;border-spacing:0;table-layout:fixed;font-size:${fontSize}px;color:${textColor};`;
        scroller.appendChild(table);

        const twoRow = this.granularity !== "year";
        const thead = document.createElement("thead");
        table.appendChild(thead);

        // top grouping row
        if (twoRow) {
            const topRow = document.createElement("tr");
            const corner = document.createElement("th");
            corner.textContent = "Resource";
            corner.rowSpan = 2;
            corner.style.cssText = `position:sticky;left:0;top:0;z-index:5;width:${nameW}px;min-width:${nameW}px;` +
                `height:${H1}px;background:${headerColor};color:#fff;text-align:left;padding:0 8px;box-sizing:border-box;font-weight:600;`;
            topRow.appendChild(corner);
            let i = 0;
            while (i < periods.length) {
                const label = periods[i].top; let span = 1;
                while (i + span < periods.length && periods[i + span].top === label) span++;
                const th = document.createElement("th");
                th.textContent = label; th.colSpan = span;
                th.style.cssText = `position:sticky;top:0;z-index:3;height:${H1}px;background:${headerColor};color:#fff;` +
                    `font-weight:600;text-align:center;border-left:1px solid rgba(255,255,255,.35);box-sizing:border-box;`;
                topRow.appendChild(th); i += span;
            }
            thead.appendChild(topRow);
        }

        // period label row
        const labelRow = document.createElement("tr");
        if (!twoRow) {
            const corner = document.createElement("th");
            corner.textContent = "Resource";
            corner.style.cssText = `position:sticky;left:0;top:0;z-index:5;width:${nameW}px;min-width:${nameW}px;` +
                `height:${H2}px;background:${headerColor};color:#fff;text-align:left;padding:0 8px;box-sizing:border-box;font-weight:600;`;
            labelRow.appendChild(corner);
        }
        periods.forEach((p, pi) => {
            const th = document.createElement("th");
            th.textContent = p.label;
            th.title = p.start.toLocaleDateString();
            const bg = pi === nowIndex ? "#8f1714" : headerColor;
            const top = twoRow ? `top:${H1}px;` : "top:0;";
            th.style.cssText = `position:sticky;${top}z-index:3;width:${colW}px;min-width:${colW}px;height:${H2}px;` +
                `background:${bg};color:#fff;text-align:center;font-weight:400;` +
                `border-left:1px solid rgba(255,255,255,.25);box-sizing:border-box;`;
            labelRow.appendChild(th);
        });
        thead.appendChild(labelRow);

        // ---- body ----
        const tbody = document.createElement("tbody");
        table.appendChild(tbody);

        visibleRRs.forEach((rr, ri) => {
            const res = rr.res;
            const rowBg = banded && ri % 2 === 1 ? bandColor : "#FFFFFF";
            const tr = document.createElement("tr");

            const nameCell = document.createElement("th");
            nameCell.style.cssText = `position:sticky;left:0;z-index:1;width:${nameW}px;min-width:${nameW}px;height:${rowH}px;` +
                `background:${rowBg};text-align:left;padding:0 6px;box-sizing:border-box;` +
                `border-bottom:1px solid ${gridColor};border-right:1px solid ${gridColor};font-weight:600;` +
                `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
            const chev = document.createElement("span");
            const isExp = this.expanded.has(res.name);
            chev.textContent = res.items.size > 0 ? (isExp ? "▼ " : "▶ ") : "";
            chev.style.cssText = "cursor:pointer;color:#888;user-select:none;";
            chev.onclick = () => { if (isExp) this.expanded.delete(res.name); else this.expanded.add(res.name); this.render(); };
            nameCell.appendChild(chev);
            const nm = document.createElement("span");
            nm.textContent = res.name;
            nm.title = res.name + "  —  click to cross-filter other visuals";
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
                badge.onclick = (ev) => this.showConflictDetail(res, rr.byPeriod, periods, conflictAt, ev as MouseEvent, textColor, headerColor, conflictColor, fontSize);
                nameCell.appendChild(badge);
            }
            tr.appendChild(nameCell);

            periods.forEach((_p, pi) => {
                const list = rr.byPeriod.get(pi) || [];
                const n = list.length;
                const isConf = n >= conflictAt;
                const td = document.createElement("td");
                td.textContent = n > 0 ? String(n) : "";
                td.title = n > 0 ? `${res.name}: ${n} item${n === 1 ? "" : "s"} — click for detail` : "";
                const conf = isConf ? `box-shadow:inset 0 0 0 2px ${conflictColor};font-weight:700;` : "";
                td.style.cssText = `width:${colW}px;min-width:${colW}px;height:${rowH}px;text-align:center;` +
                    `background:${loadColor(n)};color:${cellText};box-sizing:border-box;` +
                    `border-bottom:1px solid ${gridColor};border-left:1px solid ${gridColor};${conf}` +
                    (n > 0 ? "cursor:pointer;" : "");
                // click the number -> open the person's item rows (code + summary)
                if (n > 0) td.onclick = () => {
                    if (this.expanded.has(res.name)) this.expanded.delete(res.name);
                    else this.expanded.add(res.name);
                    this.render();
                };
                tr.appendChild(td);
            });
            tbody.appendChild(tr);

            if (isExp) {
                const items = Array.from(res.items.values()).sort((a, b) => a.name < b.name ? -1 : (a.name > b.name ? 1 : 0));
                items.forEach(it => {
                    const seen = new Set<number>();
                    it.days.forEach(c => seen.add(indexOf(c)));
                    const itr = document.createElement("tr");
                    const ic = document.createElement("th");
                    ic.style.cssText = `position:sticky;left:0;z-index:1;width:${nameW}px;min-width:${nameW}px;height:${rowH}px;` +
                        `background:${rowBg};text-align:left;padding:0 6px 0 22px;box-sizing:border-box;` +
                        `border-bottom:1px solid ${gridColor};border-right:1px solid ${gridColor};font-weight:400;color:${textColor};` +
                        `white-space:nowrap;overflow:hidden;text-overflow:ellipsis;`;
                    ic.textContent = `${it.code}: ${it.name}`; ic.title = `${it.code}: ${it.name}`;
                    itr.appendChild(ic);
                    periods.forEach((_p, pi) => {
                        const active = seen.has(pi);
                        const td = document.createElement("td");
                        td.textContent = active ? it.code : "";
                        td.title = active ? `${it.code}: ${it.name}` : "";
                        td.style.cssText = `width:${colW}px;min-width:${colW}px;height:${rowH}px;text-align:center;` +
                            `font-size:${Math.max(7, fontSize - 2)}px;color:#666;box-sizing:border-box;background:${active ? lowColor : rowBg};` +
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

    // ---- detail popovers ----

    private closePopover(): void {
        if (this.popover && this.popover.parentNode) this.popover.parentNode.removeChild(this.popover);
        this.popover = null;
    }

    private showConflictDetail(res: Resource, byPeriod: Map<number, ItemInfo[]>, periods: Period[], conflictAt: number,
        ev: MouseEvent, textColor: string, headerColor: string, conflictColor: string, fontSize: number): void {
        const rows: { code: string; name: string; extra: string }[] = [];
        byPeriod.forEach((list, pi) => {
            if (list.length >= conflictAt) {
                const p = periods[pi];
                list.slice().sort((a, b) => a.code < b.code ? -1 : 1)
                    .forEach(it => rows.push({ code: it.code, name: it.name, extra: p ? `${p.top ? p.top + " " : ""}${p.label}` : "" }));
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
        ht.textContent = title;
        head.appendChild(ht);
        const close = document.createElement("div");
        close.textContent = "✕"; close.style.cssText = "cursor:pointer;padding:0 2px;";
        close.onclick = () => this.closePopover();
        head.appendChild(close);
        pop.appendChild(head);

        const sub = document.createElement("div");
        sub.style.cssText = `padding:3px 8px;color:${conflictColor};border-bottom:1px solid #EEE;`;
        sub.textContent = subtitle;
        pop.appendChild(sub);

        const listWrap = document.createElement("div");
        listWrap.style.cssText = "overflow:auto;padding:2px 0;";
        rows.forEach(r => {
            const row = document.createElement("div");
            row.style.cssText = "padding:3px 8px;border-bottom:1px solid #F3F3F3;display:flex;gap:6px;";
            const code = document.createElement("span");
            code.textContent = r.code;
            code.style.cssText = "font-weight:600;flex:0 0 auto;color:#B21C1A;";
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
