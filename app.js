const DEFAULT_SOURCE = "./data/submissions.json";

const state = {
    rows: [],
    filtered: [],
    source: DEFAULT_SOURCE
};

const elements = {
    body: document.getElementById("leaderboard-body"),
    template: document.getElementById("row-template"),
    searchPlayer: document.getElementById("search-player"),
    validityFilter: document.getElementById("validity-filter"),
    sortOrder: document.getElementById("sort-order"),
    sizeFilter: document.getElementById("size-filter"),
    categoryFilter: document.getElementById("category-filter"),
    refreshButton: document.getElementById("refresh-button"),
    sourceLabel: document.getElementById("source-label"),
    resultsMeta: document.getElementById("results-meta"),
    validCount: document.getElementById("valid-count"),
    invalidCount: document.getElementById("invalid-count"),
    bestTime: document.getElementById("best-time")
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
    elements.validityFilter.addEventListener("change", applyFilters);
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
        state.rows = normalizeSubmissions(raw);
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
            previewSize: Number(row.previewSize ?? row.boardSize ?? 0),
            settingsLines: Array.isArray(row.settingsLines) ? row.settingsLines : []
        };
        normalized.invalidReason = computeInvalidReason(normalized);
        normalized.isValid = normalized.invalidReason === "";
        normalized.leaderboardCategory = readSettingValue(normalized.settingsLines, "Leaderboard Category", normalized.isValid ? "Default" : "Custom");
        normalized.leaderboardCategoryReason = readSettingValue(normalized.settingsLines, "Leaderboard Category Reason", "");
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
    const validity = elements.validityFilter.value;
    const sort = elements.sortOrder.value;
    const size = elements.sizeFilter.value;
    const category = elements.categoryFilter.value;

    const filtered = state.rows.filter((row) => {
        if (search && !row.playerName.toLowerCase().includes(search)) return false;
        if (validity === "valid" && !row.isValid) return false;
        if (validity === "invalid" && row.isValid) return false;
        if (size !== "all" && String(row.previewSize) !== size) return false;
        if (category !== "all" && row.leaderboardCategory.toLowerCase() !== category) return false;
        return true;
    });

    filtered.sort((a, b) => {
        if (sort === "recent") return b.finishedAtEpochSeconds - a.finishedAtEpochSeconds;
        if (a.isValid !== b.isValid) return a.isValid ? -1 : 1;
        if (a.isValid && b.isValid && a.durationSeconds !== b.durationSeconds) return a.durationSeconds - b.durationSeconds;
        return b.finishedAtEpochSeconds - a.finishedAtEpochSeconds;
    });

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
        fragment.querySelector('[data-col="rank"]').textContent = String(index + 1);
        fragment.querySelector('[data-col="player"]').textContent = row.playerName;
        fragment.querySelector('[data-col="time"]').textContent = formatDuration(row.durationSeconds);

        const pill = document.createElement("span");
        pill.className = `pill ${row.isValid ? "valid" : "invalid"}`;
        pill.textContent = row.isValid ? "Valid" : "Invalid";
        fragment.querySelector('[data-col="result"]').appendChild(pill);

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

        const reasonCell = fragment.querySelector('[data-col="reason"]');
        reasonCell.textContent = row.isValid
            ? (row.leaderboardCategoryReason || "Eligible")
            : row.invalidReason;
        reasonCell.className = `reason ${row.isValid ? "" : "invalid"}`.trim();

        elements.body.appendChild(fragment);
    });
}

function updateStats() {
    const validRows = state.rows.filter((row) => row.isValid);
    const invalidRows = state.rows.filter((row) => !row.isValid);
    const best = validRows.slice().sort((a, b) => a.durationSeconds - b.durationSeconds)[0];
    elements.validCount.textContent = String(validRows.length);
    elements.invalidCount.textContent = String(invalidRows.length);
    elements.bestTime.textContent = best ? formatDuration(best.durationSeconds) : "--:--";
    elements.resultsMeta.textContent = `${state.filtered.length} result${state.filtered.length === 1 ? "" : "s"}`;
}

function renderMessage(message) {
    elements.body.innerHTML = `<tr><td colspan="10" class="empty-state">${escapeHtml(message)}</td></tr>`;
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
