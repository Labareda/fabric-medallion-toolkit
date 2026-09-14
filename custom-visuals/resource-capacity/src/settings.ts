"use strict";

import { formattingSettings } from "powerbi-visuals-utils-formattingmodel";

import FormattingSettingsCard = formattingSettings.SimpleCard;
import FormattingSettingsSlice = formattingSettings.Slice;
import FormattingSettingsModel = formattingSettings.Model;

class AppearanceCard extends FormattingSettingsCard {
    rowHeight = new formattingSettings.NumUpDown({ name: "rowHeight", displayName: "Row height", value: 26 });
    fontSize = new formattingSettings.NumUpDown({ name: "fontSize", displayName: "Text size", value: 11 });
    textColor = new formattingSettings.ColorPicker({ name: "textColor", displayName: "Text colour", value: { value: "#333333" } });
    headerColor = new formattingSettings.ColorPicker({ name: "headerColor", displayName: "Header background", value: { value: "#B21C1A" } });
    headerTextColor = new formattingSettings.ColorPicker({ name: "headerTextColor", displayName: "Header text", value: { value: "#FFFFFF" } });
    nameWidth = new formattingSettings.NumUpDown({ name: "nameWidth", displayName: "Name column width", value: 200 });
    weekWidth = new formattingSettings.NumUpDown({ name: "weekWidth", displayName: "Min column width", value: 40 });
    workingDaysOnly = new formattingSettings.ToggleSwitch({ name: "workingDaysOnly", displayName: "Working days only (hide Sat/Sun)", value: true });

    name: string = "appearance";
    displayName: string = "Appearance";
    slices: FormattingSettingsSlice[] = [
        this.rowHeight, this.fontSize, this.textColor, this.headerColor, this.headerTextColor,
        this.nameWidth, this.weekWidth, this.workingDaysOnly
    ];
}

class ThresholdsCard extends FormattingSettingsCard {
    lowMax = new formattingSettings.NumUpDown({ name: "lowMax", displayName: "Light up to (items)", value: 2 });
    midMax = new formattingSettings.NumUpDown({ name: "midMax", displayName: "Amber up to (items)", value: 4 });
    zeroColor = new formattingSettings.ColorPicker({ name: "zeroColor", displayName: "Empty cell", value: { value: "#FFFFFF" } });
    lowColor = new formattingSettings.ColorPicker({ name: "lowColor", displayName: "Light load", value: { value: "#FDE7D3" } });
    midColor = new formattingSettings.ColorPicker({ name: "midColor", displayName: "Medium load", value: { value: "#F7B267" } });
    highColor = new formattingSettings.ColorPicker({ name: "highColor", displayName: "Heavy load / conflict", value: { value: "#E8722C" } });
    cellTextColor = new formattingSettings.ColorPicker({ name: "cellTextColor", displayName: "Cell text colour", value: { value: "#5A3210" } });

    name: string = "thresholds";
    displayName: string = "Load colours";
    slices: FormattingSettingsSlice[] = [
        this.lowMax, this.midMax, this.zeroColor, this.lowColor, this.midColor, this.highColor, this.cellTextColor
    ];
}

class MetricCard extends FormattingSettingsCard {
    sumValue = new formattingSettings.ToggleSwitch({
        name: "sumValue",
        displayName: "Sum the Value field (instead of counting items)",
        value: false
    });

    name: string = "metric";
    displayName: string = "Metric";
    slices: FormattingSettingsSlice[] = [ this.sumValue ];
}

/** Item chips are driven by the Detail fields well; this only toggles the value. */
class DetailCard extends FormattingSettingsCard {
    showValue = new formattingSettings.ToggleSwitch({ name: "showValue", displayName: "Append Value to each item (when summing)", value: true });
    chipBackground = new formattingSettings.ColorPicker({ name: "chipBackground", displayName: "Item background", value: { value: "#FFFFFF" } });
    chipTextColor = new formattingSettings.ColorPicker({ name: "chipTextColor", displayName: "Item text", value: { value: "#333333" } });

    name: string = "detail";
    displayName: string = "Item detail";
    slices: FormattingSettingsSlice[] = [ this.showValue, this.chipBackground, this.chipTextColor ];
}

class ConflictsCard extends FormattingSettingsCard {
    conflictAt = new formattingSettings.NumUpDown({ name: "conflictAt", displayName: "Conflict when items in a period ≥", value: 5 });
    minConflicts = new formattingSettings.NumUpDown({ name: "minConflicts", displayName: "Only show people with ≥ N conflicts", value: 0 });
    conflictColor = new formattingSettings.ColorPicker({ name: "conflictColor", displayName: "Conflict highlight", value: { value: "#C62828" } });

    name: string = "conflicts";
    displayName: string = "Conflicts";
    slices: FormattingSettingsSlice[] = [ this.conflictAt, this.minConflicts, this.conflictColor ];
}

class GridCard extends FormattingSettingsCard {
    gridColor = new formattingSettings.ColorPicker({ name: "gridColor", displayName: "Grid line colour", value: { value: "#E4E4E4" } });
    bandedRows = new formattingSettings.ToggleSwitch({ name: "bandedRows", displayName: "Banded rows", value: false });
    bandColor = new formattingSettings.ColorPicker({ name: "bandColor", displayName: "Band colour", value: { value: "#FAFAFA" } });
    showTodayLine = new formattingSettings.ToggleSwitch({ name: "showTodayLine", displayName: "Highlight current period", value: true });
    todayColor = new formattingSettings.ColorPicker({ name: "todayColor", displayName: "Current-period colour", value: { value: "#8F1714" } });

    name: string = "grid";
    displayName: string = "Grid & rows";
    slices: FormattingSettingsSlice[] = [ this.gridColor, this.bandedRows, this.bandColor, this.showTodayLine, this.todayColor ];
}

class LegendCard extends FormattingSettingsCard {
    show = new formattingSettings.ToggleSwitch({ name: "show", displayName: "Show legend", value: true });
    atBottom = new formattingSettings.ToggleSwitch({ name: "atBottom", displayName: "Position at bottom", value: false });

    name: string = "legend";
    displayName: string = "Legend";
    slices: FormattingSettingsSlice[] = [ this.show, this.atBottom ];
}

export class VisualFormattingSettingsModel extends FormattingSettingsModel {
    metric = new MetricCard();
    detail = new DetailCard();
    appearance = new AppearanceCard();
    thresholds = new ThresholdsCard();
    conflicts = new ConflictsCard();
    grid = new GridCard();
    legend = new LegendCard();

    cards: FormattingSettingsCard[] = [ this.metric, this.detail, this.appearance, this.thresholds, this.conflicts, this.grid, this.legend ];
}
