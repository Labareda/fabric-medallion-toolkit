"use strict";

import { formattingSettings } from "powerbi-visuals-utils-formattingmodel";

import FormattingSettingsCard = formattingSettings.SimpleCard;
import FormattingSettingsSlice = formattingSettings.Slice;
import FormattingSettingsModel = formattingSettings.Model;

class AppearanceCard extends FormattingSettingsCard {
    rowHeight = new formattingSettings.NumUpDown({ name: "rowHeight", displayName: "Row height", value: 26 });
    fontSize = new formattingSettings.NumUpDown({ name: "fontSize", displayName: "Text size", value: 11 });
    textColor = new formattingSettings.ColorPicker({ name: "textColor", displayName: "Text colour", value: { value: "#333333" } });
    headerColor = new formattingSettings.ColorPicker({ name: "headerColor", displayName: "Header colour", value: { value: "#B21C1A" } });
    nameWidth = new formattingSettings.NumUpDown({ name: "nameWidth", displayName: "Name column width", value: 200 });
    weekWidth = new formattingSettings.NumUpDown({ name: "weekWidth", displayName: "Week column width", value: 96 });

    name: string = "appearance";
    displayName: string = "Appearance";
    slices: FormattingSettingsSlice[] = [
        this.rowHeight, this.fontSize, this.textColor, this.headerColor, this.nameWidth, this.weekWidth
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

class GridCard extends FormattingSettingsCard {
    gridColor = new formattingSettings.ColorPicker({ name: "gridColor", displayName: "Grid line colour", value: { value: "#E4E4E4" } });
    bandedRows = new formattingSettings.ToggleSwitch({ name: "bandedRows", displayName: "Banded rows", value: false });
    bandColor = new formattingSettings.ColorPicker({ name: "bandColor", displayName: "Band colour", value: { value: "#FAFAFA" } });
    showTodayLine = new formattingSettings.ToggleSwitch({ name: "showTodayLine", displayName: "Highlight current week", value: true });

    name: string = "grid";
    displayName: string = "Grid & rows";
    slices: FormattingSettingsSlice[] = [ this.gridColor, this.bandedRows, this.bandColor, this.showTodayLine ];
}

class LegendCard extends FormattingSettingsCard {
    show = new formattingSettings.ToggleSwitch({ name: "show", displayName: "Show legend", value: true });
    atBottom = new formattingSettings.ToggleSwitch({ name: "atBottom", displayName: "Position at bottom", value: false });

    name: string = "legend";
    displayName: string = "Legend";
    slices: FormattingSettingsSlice[] = [ this.show, this.atBottom ];
}

export class VisualFormattingSettingsModel extends FormattingSettingsModel {
    appearance = new AppearanceCard();
    thresholds = new ThresholdsCard();
    grid = new GridCard();
    legend = new LegendCard();

    cards: FormattingSettingsCard[] = [ this.appearance, this.thresholds, this.grid, this.legend ];
}
