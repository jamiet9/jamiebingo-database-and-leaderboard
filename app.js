const DEFAULT_SOURCE = "https://jamiebingo-api.jamie-lee-thompson.workers.dev/submissions";
const LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS = 1774396800;
const WEEKLY_RESET_ANCHOR_EPOCH_SECONDS = 1774462867;
const WEEKLY_RESET_PERIOD_SECONDS = 7 * 24 * 60 * 60;
const ITEM_TEXTURE_BASE = "https://mcasset.cloud/1.21.8/assets/minecraft/textures/item/";
const MINECRAFT_ASSET_BASE = "https://mcasset.cloud/1.21.8/assets/";
const LOCAL_ENTITY_TEXTURES = {
    "minecraft:allay": "./assets/entity/allay/allay.png",
    "minecraft:armadillo": "./assets/entity/armadillo.png",
    "minecraft:bat": "./assets/entity/bat.png",
    "minecraft:bee": "./assets/entity/bee/bee.png",
    "minecraft:blaze": "./assets/entity/blaze.png",
    "minecraft:bogged": "./assets/entity/skeleton/bogged.png",
    "minecraft:breeze": "./assets/entity/breeze/breeze.png",
    "minecraft:camel": "./assets/entity/camel/camel.png",
    "minecraft:cat": "./assets/entity/cat/tabby.png",
    "minecraft:cave_spider": "./assets/entity/spider/cave_spider.png",
    "minecraft:chicken": "./assets/entity/chicken/temperate_chicken.png",
    "minecraft:cow": "./assets/entity/cow/temperate_cow.png",
    "minecraft:creaking": "./assets/entity/creaking/creaking.png",
    "minecraft:creeper": "./assets/entity/creeper/creeper.png",
    "minecraft:dolphin": "./assets/entity/dolphin.png",
    "minecraft:donkey": "./assets/entity/horse/donkey.png",
    "minecraft:drowned": "./assets/entity/zombie/drowned.png",
    "minecraft:elder_guardian": "./assets/entity/guardian_elder.png",
    "minecraft:ender_dragon": "./assets/entity/enderdragon/dragon.png",
    "minecraft:enderman": "./assets/entity/enderman/enderman.png",
    "minecraft:endermite": "./assets/entity/endermite.png",
    "minecraft:evoker": "./assets/entity/illager/evoker.png",
    "minecraft:fox": "./assets/entity/fox/fox.png",
    "minecraft:frog": "./assets/entity/frog/temperate_frog.png",
    "minecraft:ghast": "./assets/entity/ghast/ghast.png",
    "minecraft:goat": "./assets/entity/goat/goat.png",
    "minecraft:guardian": "./assets/entity/guardian.png",
    "minecraft:hoglin": "./assets/entity/hoglin/hoglin.png",
    "minecraft:horse": "./assets/entity/horse/horse_brown.png",
    "minecraft:husk": "./assets/entity/zombie/husk.png",
    "minecraft:iron_golem": "./assets/entity/iron_golem/iron_golem.png",
    "minecraft:llama": "./assets/entity/llama/creamy.png",
    "minecraft:magma_cube": "./assets/entity/slime/magmacube.png",
    "minecraft:mooshroom": "./assets/entity/cow/red_mooshroom.png",
    "minecraft:mule": "./assets/entity/horse/mule.png",
    "minecraft:nautilus": "./assets/entity/nautilus/nautilus.png",
    "minecraft:ocelot": "./assets/entity/cat/ocelot.png",
    "minecraft:panda": "./assets/entity/panda/panda.png",
    "minecraft:parrot": "./assets/entity/parrot/parrot_red_blue.png",
    "minecraft:phantom": "./assets/entity/phantom.png",
    "minecraft:pig": "./assets/entity/pig/temperate_pig.png",
    "minecraft:piglin": "./assets/entity/piglin/piglin.png",
    "minecraft:piglin_brute": "./assets/entity/piglin/piglin_brute.png",
    "minecraft:pillager": "./assets/entity/illager/pillager.png",
    "minecraft:polar_bear": "./assets/entity/bear/polarbear.png",
    "minecraft:rabbit": "./assets/entity/rabbit/brown.png",
    "minecraft:ravager": "./assets/entity/illager/ravager.png",
    "minecraft:shulker": "./assets/entity/shulker/shulker_purple.png",
    "minecraft:silverfish": "./assets/entity/silverfish.png",
    "minecraft:skeleton": "./assets/entity/skeleton/skeleton.png",
    "minecraft:slime": "./assets/entity/slime/slime.png",
    "minecraft:snow_golem": "./assets/entity/snow_golem.png",
    "minecraft:spider": "./assets/entity/spider/spider.png",
    "minecraft:stray": "./assets/entity/skeleton/stray.png",
    "minecraft:strider": "./assets/entity/strider/strider.png",
    "minecraft:turtle": "./assets/entity/turtle/big_sea_turtle.png",
    "minecraft:vex": "./assets/entity/illager/vex.png",
    "minecraft:villager": "./assets/entity/villager/villager.png",
    "minecraft:vindicator": "./assets/entity/illager/vindicator.png",
    "minecraft:warden": "./assets/entity/warden/warden.png",
    "minecraft:witch": "./assets/entity/witch.png",
    "minecraft:wither_skeleton": "./assets/entity/skeleton/wither_skeleton.png",
    "minecraft:wolf": "./assets/entity/wolf/wolf.png",
    "minecraft:zoglin": "./assets/entity/hoglin/zoglin.png",
    "minecraft:zombie": "./assets/entity/zombie/zombie.png",
    "minecraft:zombie_villager": "./assets/entity/zombie_villager/zombie_villager.png",
    "minecraft:zombified_piglin": "./assets/entity/piglin/zombified_piglin.png"
};

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
    openRowId: null,
    nextResetEpochSeconds: fallbackNextResetEpochSeconds()
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
    bestCustomTime: document.getElementById("best-custom-time"),
    nextResetTime: document.getElementById("next-reset-time")
};

let resetTimerHandle = null;

bootstrap();

async function bootstrap() {
    const querySource = new URLSearchParams(window.location.search).get("source");
    state.source = querySource || DEFAULT_SOURCE;
    elements.sourceLabel.textContent = `Source: ${state.source}`;
    bindEvents();
    startResetTimer();
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
        state.nextResetEpochSeconds = Number(raw?.nextResetEpochSeconds ?? fallbackNextResetEpochSeconds());
        state.rows = normalizeSubmissions(raw).filter((row) => row.isValid);
        populateSizeFilter(state.rows);
        applyFilters();
    } catch (error) {
        state.nextResetEpochSeconds = fallbackNextResetEpochSeconds();
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
        const previewSlots = Array.isArray(row.previewSlots)
            ? row.previewSlots.map(normalizePreviewSlot)
            : [];
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
            voteRerollUsed: Boolean(row.voteRerollUsed),
            rerollsUsedCount: Number(row.rerollsUsedCount ?? 0),
            fakeRerollsUsedCount: Number(row.fakeRerollsUsedCount ?? 0),
            previewSize,
            previewSlots: previewSlots.length ? previewSlots : previewSlotIds.map((id) => normalizePreviewSlot({ id, name: id, category: "", rarity: "" })),
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
        normalized.mode = readSettingValue(settingsLines, "Mode", "--");
        normalized.cardDifficulty = readSettingValue(settingsLines, "Card Difficulty", "normal");
        return normalized;
    });
}

function computeInvalidReason(row) {
    if (row.finishedAtEpochSeconds < currentSeasonStartEpochSeconds()) {
        return "Run is from a previous weekly reset";
    }
    if (row.finishedAtEpochSeconds < LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS) {
        return "Run was completed before March 25, 2026";
    }
    if (!row.completed) return "Card was not completed successfully";
    if (row.commandsUsed) return "Commands or gamemode changes were used";
    if (row.voteRerollUsed) return "Vote reroll unclaimed was used";
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
        fragment.querySelector('[data-col="mode"]').textContent = row.mode || "--";
        fragment.querySelector('[data-col="difficulty"]').textContent = row.cardDifficulty || "--";
        fragment.querySelector('[data-col="finished"]').textContent = row.finishedAtEpochSeconds > 0
            ? new Date(row.finishedAtEpochSeconds * 1000).toLocaleString()
            : "--";

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
            const slotData = row.previewSlots[idx] || normalizePreviewSlot({ id: row.previewSlotIds[idx] || "" });
            const slotId = slotData.id || "";
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
                const texture = createSlotTexture(slotData);
                if (texture) {
                    content.appendChild(texture);
                } else {
                    content.textContent = slotData.name || slotId;
                }
                content.title = buildSlotTooltip(slotData);
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
    const safeValue = String(value ?? "");
    item.innerHTML = `
        <span class="seed-label">${escapeHtml(label)}</span>
        <span class="seed-value">${escapeHtml(safeValue)}</span>
        <button type="button" class="seed-copy-button">Copy</button>
    `;
    item.querySelector(".seed-copy-button").addEventListener("click", async (event) => {
        event.stopPropagation();
        await copyTextToClipboard(safeValue);
    });
    return item;
}

function buildSettingsPanel(row) {
    const settings = document.createElement("div");
    settings.className = "settings-list";

    const lines = dedupeDetails([
        `Leaderboard: ${row.leaderboardCategory}`,
        row.leaderboardCategoryReason ? `Leaderboard Reason: ${row.leaderboardCategoryReason}` : "",
        ...row.settingsLines.filter((line) => !isDuplicateLeaderboardLine(line)),
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

function isDuplicateLeaderboardLine(line) {
    if (!line) return false;
    return line.startsWith("Leaderboard Category:")
        || line.startsWith("Leaderboard Category Reason:")
        || line.startsWith("Leaderboard Reason:");
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

function startResetTimer() {
    updateResetTimer();
    if (resetTimerHandle !== null) {
        window.clearInterval(resetTimerHandle);
    }
    resetTimerHandle = window.setInterval(updateResetTimer, 1000);
}

function updateResetTimer() {
    if (!elements.nextResetTime) return;
    const now = Math.floor(Date.now() / 1000);
    const remaining = Math.max(0, state.nextResetEpochSeconds - now);
    elements.nextResetTime.textContent = formatResetCountdown(remaining);
}

function formatResetCountdown(totalSeconds) {
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (days > 0) {
        return `${days}d ${String(hours).padStart(2, "0")}h ${String(minutes).padStart(2, "0")}m`;
    }
    return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

function fallbackNextResetEpochSeconds() {
    return currentSeasonStartEpochSeconds() + WEEKLY_RESET_PERIOD_SECONDS;
}

function currentSeasonStartEpochSeconds() {
    const now = Math.floor(Date.now() / 1000);
    if (now <= WEEKLY_RESET_ANCHOR_EPOCH_SECONDS) {
        return WEEKLY_RESET_ANCHOR_EPOCH_SECONDS;
    }
    const elapsed = now - WEEKLY_RESET_ANCHOR_EPOCH_SECONDS;
    const cycles = Math.floor(elapsed / WEEKLY_RESET_PERIOD_SECONDS);
    return WEEKLY_RESET_ANCHOR_EPOCH_SECONDS + cycles * WEEKLY_RESET_PERIOD_SECONDS;
}

function renderMessage(message) {
    elements.body.innerHTML = `<tr><td colspan="8" class="empty-state">${escapeHtml(message)}</td></tr>`;
}

function normalizePreviewSlot(slot) {
    return {
        id: String(slot?.id ?? ""),
        name: String(slot?.name ?? slot?.id ?? ""),
        category: String(slot?.category ?? ""),
        rarity: String(slot?.rarity ?? ""),
        questIcon: slot?.questIcon ?? null
    };
}

function isQuestSlot(slot) {
    return !!slot && (!!slot.category || slot.id.startsWith("quest.") || slot.id.startsWith("quest_"));
}

function buildSlotTooltip(slot) {
    const parts = [slot.name || slot.id];
    if (slot.category) parts.push(`Category: ${slot.category}`);
    if (slot.rarity) parts.push(`Rarity: ${slot.rarity}`);
    return parts.join("\n");
}

function createSlotTexture(slot) {
    if (!slot || !slot.id) return null;
    if (isQuestSlot(slot)) {
        return createQuestIcon(slot);
    }

    const img = document.createElement("img");
    img.className = "preview-item-icon";
    img.alt = slot.name || slot.id;
    img.loading = "lazy";
    img.src = `${ITEM_TEXTURE_BASE}${slot.id.replace("minecraft:", "")}.png`;
    img.onerror = () => {
        const fallback = document.createElement("div");
        fallback.className = "preview-item-fallback";
        fallback.textContent = (slot.name || slot.id).trim().charAt(0).toUpperCase() || "?";
        img.replaceWith(fallback);
    };
    return img;
}

function createQuestIcon(slot) {
    const icon = slot.questIcon;
    if (!icon) {
        const badge = document.createElement("div");
        badge.className = "preview-quest-badge";
        badge.textContent = (slot.category || slot.name || "?").trim().charAt(0).toUpperCase() || "?";
        return badge;
    }

    const wrap = document.createElement("div");
    wrap.className = "preview-quest-wrap";

    const main = createQuestLayer(icon.mainItemId, icon.mainTexture, icon.mainRegion, "preview-quest-main", icon.mainEntityId, icon.mainEntityBaby);
    if (main) wrap.appendChild(main);

    const corner = createQuestLayer(icon.cornerItemId, icon.cornerTexture, icon.cornerRegion, "preview-quest-corner", icon.cornerEntityId, icon.cornerEntityBaby);
    if (corner) wrap.appendChild(corner);

    if (icon.numberText) {
        const number = document.createElement("div");
        number.className = "preview-quest-number";
        number.textContent = icon.numberText;
        wrap.appendChild(number);
    }

    if (!wrap.children.length) {
        const badge = document.createElement("div");
        badge.className = "preview-quest-badge";
        badge.textContent = (slot.category || slot.name || "?").trim().charAt(0).toUpperCase() || "?";
        return badge;
    }

    return wrap;
}

function createQuestLayer(itemId, textureId, region, className, entityId = "", isBaby = false) {
    if (itemId) {
        const img = document.createElement("img");
        img.className = `${className} preview-item-icon`;
        img.alt = itemId;
        img.loading = "lazy";
        img.src = `${ITEM_TEXTURE_BASE}${itemId.replace("minecraft:", "")}.png`;
        return img;
    }

    const entityTexture = resolveEntityTexture(entityId, isBaby);
    if (entityTexture) {
        const img = document.createElement("img");
        img.className = className;
        img.alt = entityId || "";
        img.loading = "lazy";
        img.src = entityTexture;
        return img;
    }

    const textureUrl = resolveTextureUrl(textureId || region?.texture || "");
    if (!textureUrl) return null;

    if (region && region.width > 0 && region.height > 0 && region.textureWidth > 0 && region.textureHeight > 0) {
        const div = document.createElement("div");
        div.className = `${className} preview-quest-region`;
        div.style.backgroundImage = `url("${textureUrl}")`;
        div.style.backgroundSize = `${(region.textureWidth / region.width) * 100}% ${(region.textureHeight / region.height) * 100}%`;
        div.style.backgroundPosition = `${(-region.u / Math.max(1, region.width)) * 100}% ${(-region.v / Math.max(1, region.height)) * 100}%`;
        return div;
    }

    const img = document.createElement("img");
    img.className = className;
    img.alt = textureId || region?.texture || "";
    img.loading = "lazy";
    img.src = textureUrl;
    return img;
}

function resolveEntityTexture(entityId, isBaby) {
    const value = String(entityId || "");
    if (!value) return "";
    if (isBaby) {
        const babyCandidate = LOCAL_ENTITY_TEXTURES[`${value}#baby`];
        if (babyCandidate) return babyCandidate;
    }
    return LOCAL_ENTITY_TEXTURES[value] || "";
}

function resolveTextureUrl(textureId) {
    const value = String(textureId || "");
    if (!value) return "";
    if (value.startsWith("minecraft:")) {
        const parts = value.split(":");
        return `${MINECRAFT_ASSET_BASE}${parts[0]}/${parts[1]}`;
    }
    if (value.startsWith("jamiebingo:")) {
        const path = value.split(":")[1];
        if (path === "textures/gui/advancement_icon.png") {
            return "./assets/quest_icons/advancement_icon.png";
        }
        if (path.startsWith("textures/gui/quest_icons/")) {
            return `./assets/quest_icons/${path.split("/").pop()}`;
        }
    }
    return "";
}

async function copyTextToClipboard(value) {
    const text = String(value ?? "");
    if (!text || text === "(none)") return;
    try {
        await navigator.clipboard.writeText(text);
    } catch {
        const textArea = document.createElement("textarea");
        textArea.value = text;
        textArea.style.position = "fixed";
        textArea.style.opacity = "0";
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand("copy");
        textArea.remove();
    }
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
