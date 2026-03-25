const DEFAULT_SOURCE = "https://jamiebingo-api.jamie-lee-thompson.workers.dev/submissions";

const TEAM_COLORS = {
    0: "#f9fffe",
    1: "#f9801d",
    2: "#c74ebd",
    3: "#3ab3da",
    4: "#fed83d",
    5: "#80c71f",
    6: "#f38baa",
    7: "#474f52",
    8: "#9d9d97",
    9: "#169c9c",
    10: "#8932b8",
    11: "#3c44aa",
    12: "#835432",
    13: "#5e7c16",
    14: "#b02e26",
    15: "#1d1d21"
};

const state = {
    rows: [],
    filtered: [],
    source: DEFAULT_SOURCE,
    openRowId: null
};

const elements = {
    body: document.getElementById("leaderboard-body"),
    template: document.getElementById("row-template"),
    searchPlayer: document.getElementById("search-player"),
    sortOrder: document.getElementById("sort-order"),
    sizeFilter: document.getElementById("size-filter"),
    categoryFilter: document.getElementById("category-filter"),
    refreshButton: document.getElementById("refresh-button"),
    sourceLabel: document.getElementById("source-label"),
    resultsMeta: document.getElementById("results-meta"),
    defaultCount: document.getElementById("default-count"),
    customCount: document.getElementById("custom-count"),
    bestDefaultTime: document.getElementById("best-default-time"),
    bestCustomTime: document.getElementById("best-custom-time")
};

bootstrap();

async function bootstrap() {
    const querySource = new URLSearchParams(window.location.search).get("source");
    state.source = querySource || DEFAULT_SOURCE;
    elements.sourceLabel.textContent = `Source: ${state.source}`;
    bindEvents();
    await loadSubmissions();
}

function bindEvents() {
    elements.searchPlayer.addEventListener("input", applyFilters);
    elements.sortOrder.addEventListener("change", applyFilters);
    elements.sizeFilter.addEventListener("change", applyFilters);
    elements.categoryFilter.addEventListener("change", applyFilters);
    elements.refreshButton.addEventListener("click", loadSubmissions);
}

async function loadSubmissions() {
    renderMessage("Loading submissions...");
    try {
        const response = await fetch(state.source, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const raw = await response.json();
        state.rows = normalizeSubmissions(raw).filter((row) => row.isValid);
        populateSizeFilter(state.rows);
        applyFilters();
    } catch (error) {
        state.rows = [];
        state.filtered = [];
        renderMessage(`Could not load submissions: ${error.message}`);
        updateStats();
    }
}

function normalizeSubmissions(raw) {
    const items = Array.isArray(raw) ? raw : Array.isArray(raw?.submissions) ? raw.submissions : [];
    return items.map((row, index) => {
        const settingsLines = Array.isArray(row.settingsLines) ? row.settingsLines : [];
        const previewSize = Number(row.previewSize ?? row.boardSize ?? 0);
        const previewSlotIds = Array.isArray(row.previewSlotIds) ? row.previewSlotIds.map((value) => String(value ?? "")) : [];
        const completedSlotIds = Array.isArray(row.completedSlotIds) ? row.completedSlotIds.map((value) => String(value ?? "")) : [];
        const opponentCompletedSlotIds = Array.isArray(row.opponentCompletedSlotIds) ? row.opponentCompletedSlotIds.map((value) => String(value ?? "")) : [];
        const normalized = {
            id: `${row.playerName || row.player || "Unknown"}-${row.finishedAtEpochSeconds || 0}-${index}`,
            playerName: String(row.playerName ?? row.player ?? "Unknown"),
            cardSeed: String(row.cardSeed ?? ""),
            worldSeed: String(row.worldSeed ?? ""),
            durationSeconds: Number(row.durationSeconds ?? 0),
            finishedAtEpochSeconds: Number(row.finishedAtEpochSeconds ?? row.finishedAt ?? 0),
            completed: Boolean(row.completed),
            participantCount: Number(row.participantCount ?? 0),
            commandsUsed: Boolean(row.commandsUsed),
            rerollsUsedCount: Number(row.rerollsUsedCount ?? 0),
            fakeRerollsUsedCount: Number(row.fakeRerollsUsedCount ?? 0),
            previewSize,
            previewSlotIds,
            completedSlotIds,
            opponentCompletedSlotIds,
            teamColorId: Number(row.teamColorId ?? 10),
            settingsLines
        };
        normalized.invalidReason = computeInvalidReason(normalized);
        normalized.isValid = normalized.invalidReason === "";
        normalized.leaderboardCategory = readSettingValue(settingsLines, "Leaderboard Category", "Custom");
        normalized.leaderboardCategoryReason = readSettingValue(settingsLines, "Leaderboard Category Reason", "");
        return normalized;
    });
}

function computeInvalidReason(row) {
    if (!row.completed) return "Card was not completed successfully";
    if (row.participantCount !== 1) return "Must be 1 player only";
    if (row.commandsUsed) return "Commands were used";
    if (row.rerollsUsedCount > 0 || row.fakeRerollsUsedCount > 0) return "Rerolls were used";
    return "";
}

function populateSizeFilter(rows) {
    const current = elements.sizeFilter.value;
    const sizes = [...new Set(rows.map((row) => row.previewSize).filter((size) => size > 0))].sort((a, b) => a - b);
    elements.sizeFilter.innerHTML = `<option value="all">All sizes</option>${sizes.map((size) => `<option value="${size}">${size}x${size}</option>`).join("")}`;
    if (sizes.map(String).includes(current)) {
        elements.sizeFilter.value = current;
    }
}

function applyFilters() {
    const search = elements.searchPlayer.value.trim().toLowerCase();
    const sort = elements.sortOrder.value;
    const size = elements.sizeFilter.value;
    const category = elements.categoryFilter.value;

    const filtered = state.rows.filter((row) => {
        if (search && !row.playerName.toLowerCase().includes(search)) return false;
        if (size !== "all" && String(row.previewSize) !== size) return false;
        if (category !== "all" && row.leaderboardCategory.toLowerCase() !== category) return false;
        return true;
    });

    filtered.sort((a, b) => {
        if (sort === "recent") return b.finishedAtEpochSeconds - a.finishedAtEpochSeconds;
        if (a.durationSeconds !== b.durationSeconds) return a.durationSeconds - b.durationSeconds;
        return b.finishedAtEpochSeconds - a.finishedAtEpochSeconds;
    });

    if (state.openRowId && !filtered.some((row) => row.id === state.openRowId)) {
        state.openRowId = null;
    }

    state.filtered = filtered;
    renderTable();
    updateStats();
}

function renderTable() {
    if (!state.filtered.length) {
        renderMessage("No submissions match the current filters.");
        return;
    }

    elements.body.innerHTML = "";
    state.filtered.forEach((row, index) => {
        const fragment = elements.template.content.cloneNode(true);
        const tr = fragment.querySelector("tr");
        tr.classList.add("summary-row");
        if (state.openRowId === row.id) {
            tr.classList.add("is-open");
        }
        tr.addEventListener("click", () => {
            state.openRowId = state.openRowId === row.id ? null : row.id;
            renderTable();
        });

        fragment.querySelector('[data-col="rank"]').textContent = String(index + 1);
        fragment.querySelector('[data-col="player"]').textContent = row.playerName;
        fragment.querySelector('[data-col="time"]').textContent = formatDuration(row.durationSeconds);

        const leaderboardPill = document.createElement("span");
        leaderboardPill.className = `pill ${row.leaderboardCategory.toLowerCase() === "default" ? "valid" : "invalid"}`;
        leaderboardPill.textContent = row.leaderboardCategory;
        fragment.querySelector('[data-col="leaderboard"]').appendChild(leaderboardPill);

        fragment.querySelector('[data-col="board"]').textContent = row.previewSize > 0 ? `${row.previewSize}x${row.previewSize}` : "--";
        fragment.querySelector('[data-col="finished"]').textContent = row.finishedAtEpochSeconds > 0
            ? new Date(row.finishedAtEpochSeconds * 1000).toLocaleString()
            : "--";

        const cardCell = fragment.querySelector('[data-col="cardSeed"]');
        cardCell.textContent = row.cardSeed || "(none)";
        cardCell.classList.add("seed");
        cardCell.title = row.cardSeed;

        const worldCell = fragment.querySelector('[data-col="worldSeed"]');
        worldCell.textContent = row.worldSeed || "(none)";
        worldCell.classList.add("seed");
        worldCell.title = row.worldSeed;

        elements.body.appendChild(fragment);

        if (state.openRowId === row.id) {
            elements.body.appendChild(buildDetailRow(row));
        }
    });
}

function buildDetailRow(row) {
    const tr = document.createElement("tr");
    tr.className = "detail-row";

    const td = document.createElement("td");
    td.colSpan = 8;
    td.className = "detail-cell";

    const panel = document.createElement("div");
    panel.className = "detail-panel";

    const preview = document.createElement("section");
    preview.className = "detail-preview";
    preview.innerHTML = `<div class="detail-title">Card Preview</div>`;
    preview.appendChild(buildCardPreview(row));

    const meta = document.createElement("section");
    meta.className = "detail-meta";
    meta.innerHTML = `<div class="detail-title">Run Details</div>`;

    const sections = document.createElement("div");
    sections.className = "detail-sections";
    sections.appendChild(buildSeedGrid(row));
    sections.appendChild(buildSettingsPanel(row));
    meta.appendChild(sections);

    panel.append(preview, meta);
    td.appendChild(panel);
    tr.appendChild(td);
    return tr;
}

function buildCardPreview(row) {
    const wrap = document.createElement("div");
    if (!row.previewSize || !row.previewSlotIds.length) {
        wrap.className = "detail-empty";
        wrap.textContent = "This run does not include card preview data yet.";
        return wrap;
    }

    const completed = new Set(row.completedSlotIds);
    const opponent = new Set(row.opponentCompletedSlotIds);
    const preview = document.createElement("div");
    preview.className = "card-preview";

    const color = TEAM_COLORS[row.teamColorId] || TEAM_COLORS[10];
    for (let y = 0; y < row.previewSize; y++) {
        const rowEl = document.createElement("div");
        rowEl.className = "preview-row";
        rowEl.style.gridTemplateColumns = `repeat(${row.previewSize}, 1fr)`;
        for (let x = 0; x < row.previewSize; x++) {
            const idx = y * row.previewSize + x;
            const slotId = row.previewSlotIds[idx] || "";
            const slot = document.createElement("div");
            slot.className = "preview-slot";
            if (completed.has(slotId)) {
                slot.classList.add("is-completed");
                const fill = document.createElement("div");
                fill.className = "preview-fill";
                fill.style.background = color;
                slot.appendChild(fill);
            } else if (opponent.has(slotId)) {
                slot.classList.add("is-opponent");
            }
            const content = document.createElement("div");
            content.className = "preview-content";
            if (slotId && !isMaskedPreviewSlot(row)) {
                content.textContent = slotId;
                content.title = slotId;
            } else {
                content.classList.add("preview-mask");
                content.textContent = slotId ? "?" : "";
            }
            slot.appendChild(content);
            rowEl.appendChild(slot);
        }
        preview.appendChild(rowEl);
    }

    return preview;
}

function buildSeedGrid(row) {
    const grid = document.createElement("div");
    grid.className = "seed-grid";

    const cardSeed = buildSeedItem("Card Seed", row.cardSeed || "(none)");
    const worldSeed = buildSeedItem("Bingo World Seed", row.worldSeed || "(none)");
    const blacklistSeed = buildSeedItem("Blacklist/Whitelist Seed", findSeedLine(row.settingsLines, ["Blacklist Seed", "Blacklist/Whitelist Seed", "Whitelist Seed"]) || "(none)");
    const raritySeed = buildSeedItem("Rarity Changer Seed", findSeedLine(row.settingsLines, ["Rarity Seed", "Rarity Changer Seed"]) || "(none)");

    grid.append(cardSeed, worldSeed, blacklistSeed, raritySeed);
    return grid;
}

function buildSeedItem(label, value) {
    const item = document.createElement("div");
    item.className = "seed-item";
    item.innerHTML = `<span class="seed-label">${escapeHtml(label)}</span><span class="seed-value">${escapeHtml(value)}</span>`;
    return item;
}

function buildSettingsPanel(row) {
    const settings = document.createElement("div");
    settings.className = "settings-list";

    const lines = dedupeDetails([
        `Leaderboard: ${row.leaderboardCategory}`,
        row.leaderboardCategoryReason ? `Leaderboard Reason: ${row.leaderboardCategoryReason}` : "",
        ...row.settingsLines,
        row.cardSeed ? `Card Seed (Full): ${row.cardSeed}` : "",
        row.worldSeed ? `Bingo World Seed (Full): ${row.worldSeed}` : ""
    ].filter(Boolean));

    if (!lines.length) {
        const empty = document.createElement("div");
        empty.className = "detail-empty";
        empty.textContent = "No settings were stored for this run.";
        settings.appendChild(empty);
        return settings;
    }

    lines.forEach((line) => {
        const split = splitSettingLine(line);
        const rowEl = document.createElement("div");
        rowEl.className = "setting-line";
        rowEl.innerHTML = `<span class="setting-key">${escapeHtml(split.key)}</span><span class="setting-value">${escapeHtml(split.value)}</span>`;
        settings.appendChild(rowEl);
    });

    return settings;
}

function splitSettingLine(line) {
    const idx = String(line).indexOf(":");
    if (idx < 0) {
        return { key: line, value: "" };
    }
    return {
        key: line.slice(0, idx + 1),
        value: line.slice(idx + 1).trim()
    };
}

function dedupeDetails(lines) {
    const seen = new Set();
    const out = [];
    lines.forEach((line) => {
        if (!line || seen.has(line)) return;
        seen.add(line);
        out.push(line);
    });
    return out;
}

function findSeedLine(settingsLines, keys) {
    if (!Array.isArray(settingsLines)) return "";
    for (const key of keys) {
        const value = readSettingValue(settingsLines, key, "");
        if (value) return value;
    }
    return "";
}

function isMaskedPreviewSlot(row) {
    return readSettingValue(row.settingsLines, "Mode", "").toUpperCase() === "BLIND"
        || readSettingValue(row.settingsLines, "Mode", "").toUpperCase() === "HANGMAN";
}

function updateStats() {
    const defaultRows = state.rows.filter((row) => row.leaderboardCategory.toLowerCase() === "default");
    const customRows = state.rows.filter((row) => row.leaderboardCategory.toLowerCase() === "custom");
    elements.defaultCount.textContent = String(defaultRows.length);
    elements.customCount.textContent = String(customRows.length);
    elements.bestDefaultTime.textContent = bestTimeFor(defaultRows);
    elements.bestCustomTime.textContent = bestTimeFor(customRows);
    elements.resultsMeta.textContent = `${state.filtered.length} run${state.filtered.length === 1 ? "" : "s"}`;
}

function bestTimeFor(rows) {
    if (!rows.length) return "--:--";
    const best = rows.slice().sort((a, b) => a.durationSeconds - b.durationSeconds)[0];
    return formatDuration(best.durationSeconds);
}

function renderMessage(message) {
    elements.body.innerHTML = `<tr><td colspan="8" class="empty-state">${escapeHtml(message)}</td></tr>`;
}

function readSettingValue(settingsLines, key, fallback) {
    if (!Array.isArray(settingsLines)) return fallback;
    const prefix = `${key}:`;
    const line = settingsLines.find((value) => typeof value === "string" && value.startsWith(prefix));
    if (!line) return fallback;
    return line.slice(prefix.length).trim() || fallback;
}

function formatDuration(totalSeconds) {
    const seconds = Math.max(0, Number(totalSeconds || 0));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainder = seconds % 60;
    if (hours > 0) return `${hours}:${String(minutes).padStart(2, "0")}:${String(remainder).padStart(2, "0")}`;
    return `${String(minutes).padStart(2, "0")}:${String(remainder).padStart(2, "0")}`;
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}
