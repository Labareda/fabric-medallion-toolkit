"use strict";

import { formattingSettings } from "powerbi-visuals-utils-formattingmodel";

import FormattingSettingsCard = formattingSettings.SimpleCard;
import FormattingSettingsSlice = formattingSettings.Slice;
import FormattingSettingsModel = formattingSettings.Model;

/** Appearance: row height, fonts, gridlines. */
class AppearanceCard extends FormattingSettingsCard {
    rowHeight = new formattingSettings.NumUpDown({
        name: "rowHeight",
        displayName: "Row height",
        value: 20
    });
    fontSize = new formattingSettings.NumUpDown({
        name: "fontSize",
        displayName: "Text size",
        value: 10
    });
    textColor = new formattingSettings.ColorPicker({
        name: "textColor",
        displayName: "Text colour",
        value: { value: "#333333" }
    });
    headerColor = new formattingSettings.ColorPicker({
        name: "headerColor",
        displayName: "Header colour",
        value: { value: "#B21C1A" }
    });
    showGridlines = new formattingSettings.ToggleSwitch({
        name: "showGridlines",
        displayName: "Show gridlines",
        value: true
    });
    gridlineColor = new formattingSettings.ColorPicker({
        name: "gridlineColor",
        displayName: "Gridline colour",
        value: { value: "#ECECEC" }
    });

    name: string = "appearance";
    displayName: string = "Appearance";
    slices: FormattingSettingsSlice[] = [
        this.rowHeight, this.fontSize, this.textColor,
        this.headerColor, this.showGridlines, this.gridlineColor
    ];
}

/** Bars: colours (by status or fixed), corners, end labels. */
class BarsCard extends FormattingSettingsCard {
    colorByStatus = new formattingSettings.ToggleSwitch({
        name: "colorByStatus",
        displayName: "Colour by status",
        value: true
    });
    defaultColor = new formattingSettings.ColorPicker({
        name: "defaultColor",
        displayName: "Default / other bar colour",
        value: { value: "#F2820D" }
    });
    doneColor = new formattingSettings.ColorPicker({
        name: "doneColor",
        displayName: "Done colour",
        value: { value: "#1F9D55" }
    });
    inProgressColor = new formattingSettings.ColorPicker({
        name: "inProgressColor",
        displayName: "In Progress colour",
        value: { value: "#F2820D" }
    });
    toDoColor = new formattingSettings.ColorPicker({
        name: "toDoColor",
        displayName: "To Do colour",
        value: { value: "#B21C1A" }
    });
    cornerRadius = new formattingSettings.NumUpDown({
        name: "cornerRadius",
        displayName: "Corner radius",
        value: 2
    });
    showBarLabels = new formattingSettings.ToggleSwitch({
        name: "showBarLabels",
        displayName: "Label at bar end",
        value: true
    });

    name: string = "bars";
    displayName: string = "Bars";
    slices: FormattingSettingsSlice[] = [
        this.colorByStatus, this.defaultColor, this.doneColor,
        this.inProgressColor, this.toDoColor, this.cornerRadius, this.showBarLabels
    ];
}

/** Milestone diamonds. */
class MilestoneCard extends FormattingSettingsCard {
    milestoneColor = new formattingSettings.ColorPicker({
        name: "milestoneColor",
        displayName: "Milestone colour",
        value: { value: "#B21C1A" }
    });
    milestoneSize = new formattingSettings.NumUpDown({
        name: "milestoneSize",
        displayName: "Milestone size",
        value: 9
    });

    name: string = "milestone";
    displayName: string = "Milestones";
    slices: FormattingSettingsSlice[] = [ this.milestoneColor, this.milestoneSize ];
}

/** Today line. */
class TodayLineCard extends FormattingSettingsCard {
    show = new formattingSettings.ToggleSwitch({
        name: "show",
        displayName: "Show today line",
        value: true
    });
    lineColor = new formattingSettings.ColorPicker({
        name: "lineColor",
        displayName: "Line colour",
        value: { value: "#B21C1A" }
    });

    name: string = "todayLine";
    displayName: string = "Today line";
    slices: FormattingSettingsSlice[] = [ this.show, this.lineColor ];
}

/** Left columns (name tree always shown; lead/resources optional). */
class ColumnsCard extends FormattingSettingsCard {
    showLead = new formattingSettings.ToggleSwitch({
        name: "showLead",
        displayName: "Show Lead column",
        value: true
    });
    showResources = new formattingSettings.ToggleSwitch({
        name: "showResources",
        displayName: "Show Resources column",
        value: true
    });
    nameWidth = new formattingSettings.NumUpDown({
        name: "nameWidth",
        displayName: "Name column width",
        value: 260
    });
    leadWidth = new formattingSettings.NumUpDown({
        name: "leadWidth",
        displayName: "Lead column width",
        value: 90
    });
    resourceWidth = new formattingSettings.NumUpDown({
        name: "resourceWidth",
        displayName: "Resources column width",
        value: 100
    });

    name: string = "columns";
    displayName: string = "Columns";
    slices: FormattingSettingsSlice[] = [
        this.showLead, this.showResources,
        this.nameWidth, this.leadWidth, this.resourceWidth
    ];
}

export class VisualFormattingSettingsModel extends FormattingSettingsModel {
    appearance = new AppearanceCard();
    bars = new BarsCard();
    milestone = new MilestoneCard();
    todayLine = new TodayLineCard();
    columns = new ColumnsCard();

    cards: FormattingSettingsCard[] = [
        this.appearance, this.bars, this.milestone, this.todayLine, this.columns
    ];
}
