const LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS = 1774396800;
const WEEKLY_RESET_ANCHOR_EPOCH_SECONDS = 1774537841;
const WEEKLY_RESET_PERIOD_SECONDS = 7 * 24 * 60 * 60;
const ONLINE_QUEUE_STALE_SECONDS = 15 * 60;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: corsHeaders()
      });
    }

    if (request.method === "GET" && url.pathname === "/submissions") {
      const seasonStart = currentSeasonStart();
      const results = await loadSubmissionRows(env, seasonStart);
      const weekly = await loadCurrentWeeklyState(env, seasonStart);

      const submissions = results.map((row) => decorateSubmissionWithWeekly(row, weekly));

      return Response.json({
        submissions,
        seasonStartEpochSeconds: seasonStart,
        nextResetEpochSeconds: seasonStart + WEEKLY_RESET_PERIOD_SECONDS
      }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "GET" && url.pathname === "/weekly-challenge") {
      const baseSeed = currentSeasonStart();
      let stored = null;
      try {
        stored = await env.DB.prepare(`
          SELECT
            base_seed AS baseSeed,
            challenge_id AS challengeId,
            next_reset_epoch_seconds AS nextResetEpochSeconds,
            settings_seed AS settingsSeed,
            world_seed AS worldSeed,
            card_seed AS cardSeed,
            preview_size AS previewSize,
            preview_slots_json AS previewSlotsJson,
            settings_json AS settingsJson
          FROM weekly_challenge_state
          WHERE challenge_id = ?
          LIMIT 1
        `).bind(`weekly-${baseSeed}`).first();
      } catch {
        stored = null;
      }
      return Response.json({
        baseSeed,
        challengeId: stored?.challengeId || `weekly-${baseSeed}`,
        nextResetEpochSeconds: stored?.nextResetEpochSeconds || (baseSeed + WEEKLY_RESET_PERIOD_SECONDS),
        settingsSeed: stored?.settingsSeed || "",
        worldSeed: stored?.worldSeed || "",
        cardSeed: stored?.cardSeed || "",
        previewSize: Number(stored?.previewSize || 0),
        previewSlots: parseJsonArray(stored?.previewSlotsJson),
        settingsLines: parseJsonArray(stored?.settingsJson)
      }, {
        headers: corsHeaders()
      });
    }

    if (url.pathname.startsWith("/online/")) {
      await ensureOnlineQueueTable(env);
      await cleanupStaleOnlineQueueEntries(env, Math.floor(Date.now() / 1000));
    }

    if (request.method === "GET" && url.pathname === "/online/queues") {
      const playerName = normalizePlayerName(url.searchParams.get("playerName"));
      const snapshot = await buildOnlineQueueSnapshot(env, playerName);
      return Response.json(snapshot, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/queue/join") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const queueMode = normalizeQueueMode(body?.queueMode);
      if (!playerName) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          status: "error",
          message: "Player name is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      if (!queueMode) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          status: "error",
          message: "Queue mode is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      const existingMatch = playerName ? await loadActiveMatchForPlayer(env, playerName) : null;
      if (existingMatch?.matchId) {
        await maybeAdvanceMatchState(env, existingMatch.matchId, now);
      }
      let refreshedMatch = playerName ? await loadActiveMatchForPlayer(env, playerName) : null;
      if (refreshedMatch?.matchId && refreshedMatch.state === "ready_to_start") {
        await clearActiveMatch(env, refreshedMatch.matchId);
        refreshedMatch = null;
      }
      if (refreshedMatch) {
        return Response.json(await buildOnlineQueueSnapshot(env, playerName, "matched", "Match already found"), {
          headers: corsHeaders()
        });
      }
      await env.DB.prepare(`
        INSERT INTO online_queue_entries (
          player_name,
          queue_mode,
          controller_preferences_json,
          world_preferences_json,
          queued_at_epoch_seconds,
          updated_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_name) DO UPDATE SET
          queue_mode = excluded.queue_mode,
          controller_preferences_json = excluded.controller_preferences_json,
          world_preferences_json = excluded.world_preferences_json,
          updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
      `).bind(
        playerName,
        queueMode,
        JSON.stringify(body?.controllerPreferences || {}),
        JSON.stringify(body?.worldPreferences || {}),
        now,
        now
      ).run();
      await tryCreateMatchesForQueueMode(env, queueMode, now);

      const snapshot = await buildOnlineQueueSnapshot(
        env,
        playerName,
        "queued",
        (await loadActiveMatchForPlayer(env, playerName))
          ? `Match found for ${queueModeLabel(queueMode)}`
          : `Queued for ${queueModeLabel(queueMode)}`
      );
      return Response.json(snapshot, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/queue/leave") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      if (!playerName) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          status: "error",
          message: "Player name is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }

      await env.DB.prepare(`
        DELETE FROM online_queue_entries
        WHERE player_name = ?
      `).bind(playerName).run();

      const snapshot = await buildOnlineQueueSnapshot(env, playerName, "idle", "Queue left");
      return Response.json(snapshot, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/ready") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      if (!playerName) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          activeMatch: null,
          status: "error",
          message: "Player name is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const activeMatch = await loadActiveMatchForPlayer(env, playerName);
      if (!activeMatch) {
        return Response.json(await buildOnlineQueueSnapshot(env, playerName, "idle", "No active match"), {
          headers: corsHeaders()
        });
      }

      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        UPDATE online_match_players
        SET ready = 1
        WHERE match_id = ? AND player_name = ?
      `).bind(activeMatch.matchId, playerName).run();

      await maybeAdvanceMatchState(env, activeMatch.matchId, now);

      const snapshot = await buildOnlineQueueSnapshot(env, playerName, "matched", "Ready status updated");
      return Response.json(snapshot, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/leave") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      if (!playerName) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          activeMatch: null,
          status: "error",
          message: "Player name is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const activeMatch = await loadActiveMatchForPlayer(env, playerName);
      if (!activeMatch) {
        return Response.json(await buildOnlineQueueSnapshot(env, playerName, "idle", "No active match"), {
          headers: corsHeaders()
        });
      }

      const now = Math.floor(Date.now() / 1000);
      await dissolvePendingMatch(env, activeMatch.matchId, playerName, now);
      const snapshot = await buildOnlineQueueSnapshot(env, playerName, "idle", "Left match");
      return Response.json(snapshot, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/consume") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      if (!playerName) {
        return Response.json({
          counts: await loadOnlineQueueCounts(env),
          activeQueue: null,
          activeMatch: null,
          status: "error",
          message: "Player name is required"
        }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const activeMatch = await loadActiveMatchForPlayer(env, playerName);
      if (activeMatch?.matchId) {
        await clearActiveMatch(env, activeMatch.matchId);
      }
      return Response.json(await buildOnlineQueueSnapshot(env, playerName, "idle", "Match consumed"), {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/weekly-challenge-publish") {
      const authError = requireApiKey(request, env);
      if (authError) return authError;

      const body = await request.json();
      await upsertWeeklyChallengeState(env, body);

      return Response.json({ ok: true }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/submit") {
      const body = await request.json();
      const weekly = await loadCurrentWeeklyState(env, currentSeasonStart());
      const weeklyMatch = matchesWeeklySubmission(body, weekly);

      await env.DB.prepare(`
        INSERT INTO submissions (
          player_name,
          card_seed,
          world_seed,
          settings_seed,
          duration_seconds,
          finished_at_epoch_seconds,
          completed,
          participant_count,
          commands_used,
          vote_reroll_used,
          rerolls_used_count,
          fake_rerolls_used_count,
          preview_size,
          team_color_id,
          preview_slots_json,
          preview_slot_ids_json,
          completed_slot_ids_json,
          opponent_completed_slot_ids_json,
          settings_json,
          weekly_challenge,
          weekly_challenge_id,
          leaderboard_category,
          leaderboard_category_reason,
          submitted_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
        body.playerName || "Unknown",
        body.cardSeed || "",
        body.worldSeed || "",
        body.settingsSeed || "",
        Number(body.durationSeconds || 0),
        Number(body.finishedAtEpochSeconds || 0),
        body.completed ? 1 : 0,
        Number(body.participantCount || 0),
        body.commandsUsed ? 1 : 0,
        body.voteRerollUsed ? 1 : 0,
        Number(body.rerollsUsedCount || 0),
        Number(body.fakeRerollsUsedCount || 0),
        Number(body.previewSize || 0),
        Number(body.teamColorId || 0),
        JSON.stringify(asArray(body.previewSlots)),
        JSON.stringify(asArray(body.previewSlotIds)),
        JSON.stringify(asArray(body.completedSlotIds)),
        JSON.stringify(asArray(body.opponentCompletedSlotIds)),
        JSON.stringify(asArray(body.settingsLines)),
        weeklyMatch ? 1 : (body.weeklyChallenge ? 1 : 0),
        weeklyMatch ? (weekly.challengeId || "") : (body.weeklyChallengeId || ""),
        weeklyMatch ? "Weekly" : (body.leaderboardCategory || "Custom"),
        weeklyMatch ? `Matches ${weekly.challengeId || "the current weekly challenge"}` : (body.leaderboardCategoryReason || ""),
        Number(body.submittedAtEpochSeconds || Math.floor(Date.now() / 1000))
      ).run();

      return Response.json({ ok: true }, {
        headers: corsHeaders()
      });
    }

    return new Response("Not found", { status: 404 });
  }
};

async function loadSubmissionRows(env, seasonStart) {
  try {
    const { results } = await env.DB.prepare(`
      SELECT
        player_name AS playerName,
        card_seed AS cardSeed,
        world_seed AS worldSeed,
        settings_seed AS settingsSeed,
        duration_seconds AS durationSeconds,
        finished_at_epoch_seconds AS finishedAtEpochSeconds,
        completed,
        participant_count AS participantCount,
        commands_used AS commandsUsed,
        vote_reroll_used AS voteRerollUsed,
        rerolls_used_count AS rerollsUsedCount,
        fake_rerolls_used_count AS fakeRerollsUsedCount,
        preview_size AS previewSize,
        team_color_id AS teamColorId,
        preview_slots_json AS previewSlotsJson,
        preview_slot_ids_json AS previewSlotIdsJson,
        completed_slot_ids_json AS completedSlotIdsJson,
        opponent_completed_slot_ids_json AS opponentCompletedSlotIdsJson,
        settings_json AS settingsJson,
        weekly_challenge AS weeklyChallenge,
        weekly_challenge_id AS weeklyChallengeId,
        leaderboard_category AS leaderboardCategory,
        leaderboard_category_reason AS leaderboardCategoryReason
      FROM submissions
      WHERE completed = 1
        AND commands_used = 0
        AND vote_reroll_used = 0
        AND finished_at_epoch_seconds >= ?
        AND finished_at_epoch_seconds >= ?
      ORDER BY submitted_at_epoch_seconds DESC
    `).bind(LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS, seasonStart).all();
    return results || [];
  } catch {
    const { results } = await env.DB.prepare(`
      SELECT
        player_name AS playerName,
        card_seed AS cardSeed,
        world_seed AS worldSeed,
        '' AS settingsSeed,
        duration_seconds AS durationSeconds,
        finished_at_epoch_seconds AS finishedAtEpochSeconds,
        completed,
        participant_count AS participantCount,
        commands_used AS commandsUsed,
        0 AS voteRerollUsed,
        rerolls_used_count AS rerollsUsedCount,
        fake_rerolls_used_count AS fakeRerollsUsedCount,
        preview_size AS previewSize,
        team_color_id AS teamColorId,
        '[]' AS previewSlotsJson,
        '[]' AS previewSlotIdsJson,
        '[]' AS completedSlotIdsJson,
        '[]' AS opponentCompletedSlotIdsJson,
        settings_json AS settingsJson,
        0 AS weeklyChallenge,
        '' AS weeklyChallengeId,
        'Custom' AS leaderboardCategory,
        '' AS leaderboardCategoryReason
      FROM submissions
      WHERE completed = 1
        AND commands_used = 0
        AND finished_at_epoch_seconds >= ?
        AND finished_at_epoch_seconds >= ?
      ORDER BY submitted_at_epoch_seconds DESC
    `).bind(LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS, seasonStart).all();
    return results || [];
  }
}

async function loadCurrentWeeklyState(env, baseSeed) {
  try {
    const row = await env.DB.prepare(`
      SELECT
        challenge_id AS challengeId,
        base_seed AS baseSeed,
        next_reset_epoch_seconds AS nextResetEpochSeconds,
        settings_seed AS settingsSeed,
        world_seed AS worldSeed,
        card_seed AS cardSeed
      FROM weekly_challenge_state
      WHERE challenge_id = ?
      LIMIT 1
    `).bind(`weekly-${baseSeed}`).first();
    return row || null;
  } catch {
    return null;
  }
}

async function ensureOnlineQueueTable(env) {
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS online_queue_entries (
      player_name TEXT PRIMARY KEY,
      queue_mode TEXT NOT NULL,
      controller_preferences_json TEXT NOT NULL,
      world_preferences_json TEXT NOT NULL,
      queued_at_epoch_seconds INTEGER NOT NULL,
      updated_at_epoch_seconds INTEGER NOT NULL
    )
  `).run();
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS online_matches (
      match_id TEXT PRIMARY KEY,
      queue_mode TEXT NOT NULL,
      state TEXT NOT NULL,
      target_player_count INTEGER NOT NULL,
      created_at_epoch_seconds INTEGER NOT NULL,
      updated_at_epoch_seconds INTEGER NOT NULL
    )
  `).run();
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS online_match_players (
      match_id TEXT NOT NULL,
      player_name TEXT NOT NULL UNIQUE,
      ready INTEGER NOT NULL DEFAULT 0,
      joined_at_epoch_seconds INTEGER NOT NULL,
      PRIMARY KEY (match_id, player_name)
    )
  `).run();
  try {
    await env.DB.prepare(`
      ALTER TABLE online_match_players
      ADD COLUMN ready INTEGER NOT NULL DEFAULT 0
    `).run();
  } catch {
  }
  try {
    await env.DB.prepare(`
      ALTER TABLE online_matches
      ADD COLUMN start_after_epoch_seconds INTEGER NOT NULL DEFAULT 0
    `).run();
  } catch {
  }
  try {
    await env.DB.prepare(`
      ALTER TABLE online_matches
      ADD COLUMN match_payload_json TEXT NOT NULL DEFAULT '{}'
    `).run();
  } catch {
  }
}

async function cleanupStaleOnlineQueueEntries(env, nowEpochSeconds) {
  await env.DB.prepare(`
    DELETE FROM online_queue_entries
    WHERE updated_at_epoch_seconds < ?
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
  await env.DB.prepare(`
    DELETE FROM online_match_players
    WHERE match_id IN (
      SELECT match_id
      FROM online_matches
      WHERE updated_at_epoch_seconds < ?
    )
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
  await env.DB.prepare(`
    DELETE FROM online_matches
    WHERE updated_at_epoch_seconds < ?
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
}

async function buildOnlineQueueSnapshot(env, playerName, status = "idle", message = "") {
  const counts = await loadOnlineQueueCounts(env);
  let activeQueue = null;
  const now = Math.floor(Date.now() / 1000);
  const activeMatch = playerName ? await loadActiveMatchForPlayer(env, playerName) : null;
  if (activeMatch?.matchId) {
    await maybeAdvanceMatchState(env, activeMatch.matchId, now);
  }
  const refreshedActiveMatch = playerName ? await loadActiveMatchForPlayer(env, playerName) : null;
  if (playerName) {
    const row = await env.DB.prepare(`
      SELECT queue_mode AS queueMode
      FROM online_queue_entries
      WHERE player_name = ?
      LIMIT 1
    `).bind(playerName).first();
    activeQueue = normalizeQueueMode(row?.queueMode);
  }
  return {
    counts,
    activeQueue,
    activeMatch: refreshedActiveMatch,
    status,
    message
  };
}

async function loadActiveMatchForPlayer(env, playerName) {
  const row = await env.DB.prepare(`
    SELECT
      m.match_id AS matchId,
      m.queue_mode AS queueMode,
      m.state AS state,
      m.target_player_count AS targetPlayerCount,
      m.created_at_epoch_seconds AS createdAtEpochSeconds,
      m.start_after_epoch_seconds AS startAfterEpochSeconds,
      m.match_payload_json AS matchPayloadJson
    FROM online_match_players p
    JOIN online_matches m ON m.match_id = p.match_id
    WHERE p.player_name = ?
    LIMIT 1
  `).bind(playerName).first();
  if (!row) return null;
  const { results } = await env.DB.prepare(`
    SELECT player_name AS playerName, ready AS ready
    FROM online_match_players
    WHERE match_id = ?
    ORDER BY joined_at_epoch_seconds ASC, player_name ASC
  `).bind(row.matchId).all();
  const readyPlayerNames = [];
  const playerNames = [];
  for (const entry of results || []) {
    const playerEntryName = String(entry.playerName || "");
    playerNames.push(playerEntryName);
    if (Number(entry.ready || 0) != 0) {
      readyPlayerNames.push(playerEntryName);
    }
  }
  return {
    matchId: row.matchId,
    queueMode: normalizeQueueMode(row.queueMode),
    state: String(row.state || "pending"),
    targetPlayerCount: Number(row.targetPlayerCount || 0),
    createdAtEpochSeconds: Number(row.createdAtEpochSeconds || 0),
    startAfterEpochSeconds: Number(row.startAfterEpochSeconds || 0),
    settingsLines: parseJsonArray(tryParseMatchPayload(row.matchPayloadJson)?.settingsLines),
    definitionJson: String(row.matchPayloadJson || "{}"),
    playerNames,
    readyPlayerNames
  };
}

async function maybeAdvanceMatchState(env, matchId, nowEpochSeconds) {
  const match = await env.DB.prepare(`
    SELECT
      match_id AS matchId,
      state AS state,
      target_player_count AS targetPlayerCount,
      created_at_epoch_seconds AS createdAtEpochSeconds,
      start_after_epoch_seconds AS startAfterEpochSeconds
    FROM online_matches
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  if (!match) return;

  const { results } = await env.DB.prepare(`
    SELECT ready AS ready
    FROM online_match_players
    WHERE match_id = ?
  `).bind(matchId).all();
  const readyCount = (results || []).filter((entry) => Number(entry.ready || 0) != 0).length;
  const targetPlayerCount = Number(match.targetPlayerCount || 0);
  const now = Number(nowEpochSeconds || 0);
  const currentState = String(match.state || "pending_ready");
  const forceStartAt = Number(match.createdAtEpochSeconds || 0) + 5 * 60;

  if (currentState === "pending_ready") {
    if (readyCount >= targetPlayerCount || now >= forceStartAt) {
      const revealDuration = await computeRevealDurationSeconds(env, matchId);
      const revealAt = now + revealDuration;
      await env.DB.prepare(`
        UPDATE online_matches
        SET state = ?, start_after_epoch_seconds = ?, updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind("revealing", revealAt, now, matchId).run();
    }
    return;
  }

  if (currentState === "revealing" && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0)) {
    await env.DB.prepare(`
      UPDATE online_matches
      SET state = ?, updated_at_epoch_seconds = ?
      WHERE match_id = ?
    `).bind("ready_to_start", now, matchId).run();
    return;
  }

  if (currentState === "ready_to_start" && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0) + 5 * 60) {
    await env.DB.prepare(`
      DELETE FROM online_match_players
      WHERE match_id = ?
    `).bind(matchId).run();
    await env.DB.prepare(`
      DELETE FROM online_matches
      WHERE match_id = ?
    `).bind(matchId).run();
  }
}

async function clearActiveMatch(env, matchId) {
  if (!matchId) return;
  await env.DB.prepare(`
    DELETE FROM online_match_players
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_matches
    WHERE match_id = ?
  `).bind(matchId).run();
}

async function computeRevealDurationSeconds(env, matchId) {
  try {
    const row = await env.DB.prepare(`
      SELECT match_payload_json AS matchPayloadJson
      FROM online_matches
      WHERE match_id = ?
      LIMIT 1
    `).bind(matchId).first();
    const payload = tryParseMatchPayload(row?.matchPayloadJson);
    const count = Array.isArray(payload?.settingsLines) ? payload.settingsLines.length : 0;
    return Math.max(10, Math.min(30, count * 2));
  } catch {
    return 16;
  }
}

async function dissolvePendingMatch(env, matchId, leavingPlayerName, nowEpochSeconds) {
  const match = await env.DB.prepare(`
    SELECT queue_mode AS queueMode
    FROM online_matches
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  if (!match) return;

  const { results } = await env.DB.prepare(`
    SELECT player_name AS playerName
    FROM online_match_players
    WHERE match_id = ?
  `).bind(matchId).all();

  const remainingPlayers = (results || [])
    .map((row) => normalizePlayerName(row?.playerName))
    .filter((name) => name && name !== leavingPlayerName);

  await env.DB.prepare(`
    DELETE FROM online_match_players
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_matches
    WHERE match_id = ?
  `).bind(matchId).run();

  const queueMode = normalizeQueueMode(match.queueMode);
  if (!queueMode) return;

  for (const playerName of remainingPlayers) {
    await env.DB.prepare(`
      INSERT INTO online_queue_entries (
        player_name,
        queue_mode,
        controller_preferences_json,
        world_preferences_json,
        queued_at_epoch_seconds,
        updated_at_epoch_seconds
      ) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(player_name) DO UPDATE SET
        queue_mode = excluded.queue_mode,
        updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
    `).bind(
      playerName,
      queueMode,
      "{}",
      "{}",
      nowEpochSeconds,
      nowEpochSeconds
    ).run();
  }
}

async function loadOnlineQueueCounts(env) {
  const counts = {
    RANKED_1V1: 0,
    CASUAL_1V1: 0,
    CASUAL_FFA: 0,
    CASUAL_2S: 0,
    CASUAL_3S: 0,
    CASUAL_4S: 0
  };
  const { results } = await env.DB.prepare(`
    SELECT queue_mode AS queueMode, COUNT(*) AS entryCount
    FROM online_queue_entries
    GROUP BY queue_mode
  `).all();
  for (const row of results || []) {
    const queueMode = normalizeQueueMode(row?.queueMode);
    if (!queueMode) continue;
    counts[queueMode] = Number(row?.entryCount || 0);
  }
  return counts;
}

async function tryCreateMatchesForQueueMode(env, queueMode, nowEpochSeconds) {
  const targetPlayerCount = requiredPlayersForQueueMode(queueMode);
  if (!targetPlayerCount) return;
  while (true) {
    const { results } = await env.DB.prepare(`
      SELECT
        player_name AS playerName,
        controller_preferences_json AS controllerPreferencesJson,
        world_preferences_json AS worldPreferencesJson
      FROM online_queue_entries
      WHERE queue_mode = ?
      ORDER BY queued_at_epoch_seconds ASC, player_name ASC
      LIMIT ?
    `).bind(queueMode, targetPlayerCount).all();
    const queueRows = (results || []).map((row) => ({
      playerName: normalizePlayerName(row?.playerName),
      controllerPreferences: parseJsonObject(row?.controllerPreferencesJson),
      worldPreferences: parseJsonObject(row?.worldPreferencesJson)
    })).filter((row) => row.playerName);
    const players = queueRows.map((row) => row.playerName);
    if (queueRows.length < targetPlayerCount) {
      return;
    }

    const matchId = `match-${queueMode.toLowerCase()}-${nowEpochSeconds}-${Math.random().toString(36).slice(2, 8)}`;
    const matchPayload = buildMatchPayload(queueMode, queueRows, nowEpochSeconds);
    await env.DB.prepare(`
      INSERT INTO online_matches (
        match_id,
        queue_mode,
        state,
        target_player_count,
        created_at_epoch_seconds,
        updated_at_epoch_seconds,
        start_after_epoch_seconds,
        match_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
        matchId,
        queueMode,
        "pending_ready",
        targetPlayerCount,
        nowEpochSeconds,
        nowEpochSeconds,
        0,
        JSON.stringify(matchPayload)
      ).run();

      for (const playerName of players) {
        await env.DB.prepare(`
          INSERT INTO online_match_players (
            match_id,
            player_name,
            ready,
            joined_at_epoch_seconds
        ) VALUES (?, ?, ?, ?)
      `).bind(matchId, playerName, 0, nowEpochSeconds).run();
      }

    await env.DB.prepare(`
      DELETE FROM online_queue_entries
      WHERE player_name IN (${players.map(() => "?").join(", ")})
    `).bind(...players).run();
  }
}

function parseJsonObject(value) {
  try {
    const parsed = JSON.parse(String(value || "{}"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function tryParseMatchPayload(value) {
  try {
    const parsed = JSON.parse(String(value || "{}"));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function buildMatchPayload(queueMode, queueRows, nowEpochSeconds) {
  const controllerPreferences = queueRows.map((row) => row.controllerPreferences || {});
  const worldPreferences = queueRows.map((row) => row.worldPreferences || {});
  const matchSeed = (nowEpochSeconds * 1000003) ^ Math.floor(Math.random() * 2147483647);
  const win = resolveEnumPreference(controllerPreferences, "win", ["FULL", "LINE", "LOCKOUT", "RARITY", "BLIND", "HANGMAN", "GUNGAME", "GAMEGUN"], "FULL");
  const cardSize = resolveCardSizeValue(controllerPreferences);
  const cardDifficulty = resolveEnumPreference(controllerPreferences, "cardDifficulty", ["easy", "normal", "hard", "extreme"], "normal").toLowerCase();
  const gameDifficulty = resolveEnumPreference(controllerPreferences, "gameDifficulty", ["easy", "normal", "hard"], "normal").toLowerCase();
  const effectsEnabled = resolveTogglePreference(controllerPreferences, "effects", false);
  const rtpEnabled = resolveTogglePreference(controllerPreferences, "rtp", false);
  const hostileMobsEnabled = resolveTogglePreference(controllerPreferences, "hostileMobs", true);
  const hungerEnabled = resolveTogglePreference(controllerPreferences, "hunger", true);
  const naturalRegenEnabled = resolveTogglePreference(controllerPreferences, "naturalRegen", true);
  const keepInventoryEnabled = resolveTogglePreference(controllerPreferences, "keepInventory", false);
  const hardcoreEnabled = resolveTogglePreference(controllerPreferences, "hardcore", false);
  const teamChestEnabled = resolveTogglePreference(controllerPreferences, "teamChest", true);
  const minesEnabled = resolveTogglePreference(controllerPreferences, "mines", false);
  const powerSlotEnabled = resolveTogglePreference(controllerPreferences, "powerSlot", false);
  const draftEnabled = resolveTogglePreference(controllerPreferences, "draft", false);
  const rerollsEnabled = resolveTogglePreference(controllerPreferences, "rerolls", false);
  const fakeRerollsEnabled = resolveTogglePreference(controllerPreferences, "fakeRerolls", false);
  const worldTypeMode = resolveWorldType(worldPreferences);
  const surfaceCaveBiomes = resolveTogglePreference(worldPreferences, "surfaceCaveBiomes", false);
  const prelitPortalsMode = resolvePrelitPortals(worldPreferences);
  const settingsLines = [
    `Mode: ${win}`,
    `Card Size: ${cardSize}x${cardSize}`,
    `Card Difficulty: ${cardDifficulty}`,
    `Game Difficulty: ${gameDifficulty}`,
    `Effects: ${effectsEnabled ? "Enabled" : "Disabled"}`,
    `RTP: ${rtpEnabled ? "Enabled" : "Disabled"}`,
    `Hostile Mobs: ${hostileMobsEnabled ? "Enabled" : "Disabled"}`,
    `Hunger: ${hungerEnabled ? "Enabled" : "Disabled"}`,
    `Natural Regen: ${naturalRegenEnabled ? "On" : "Off"}`,
    `Keep Inventory: ${keepInventoryEnabled ? "Enabled" : "Disabled"}`,
    `Hardcore: ${hardcoreEnabled ? "Enabled" : "Disabled"}`,
    `Team Chest: ${teamChestEnabled ? "Enabled" : "Disabled"}`,
    `Mines: ${minesEnabled ? "Enabled" : "Disabled"}`,
    `Power Slot: ${powerSlotEnabled ? "Enabled" : "Disabled"}`,
    `Draft: ${draftEnabled ? "Enabled" : "Disabled"}`,
    `Rerolls: ${rerollsEnabled ? "Enabled" : "Disabled"}`,
    `Fake Rerolls: ${fakeRerollsEnabled ? "Enabled" : "Disabled"}`,
    "PVP: Disabled",
    "Adventure: Disabled",
    "Late Join: Disabled",
    "Team Sync: Enabled",
    "Delay: 60s",
    "New Seed Every Game: Enabled",
    `World Type: ${resolveWorldTypeLabel(worldTypeMode)}`,
    `World Surface Cave Biomes: ${surfaceCaveBiomes ? "Enabled" : "Disabled"}`,
    `Prelit Portals: ${resolvePrelitLabel(prelitPortalsMode)}`
  ];
  return {
    generatedAtEpochSeconds: nowEpochSeconds,
    matchSeed,
    queueMode,
    win,
    cardSize,
    cardDifficulty,
    gameDifficulty,
    effectsEnabled,
    rtpEnabled,
    hostileMobsEnabled,
    hungerEnabled,
    naturalRegenEnabled,
    keepInventoryEnabled,
    hardcoreEnabled,
    teamChestEnabled,
    minesEnabled,
    powerSlotEnabled,
    draftEnabled,
    rerollsEnabled,
    fakeRerollsEnabled,
    worldTypeMode,
    surfaceCaveBiomes,
    prelitPortalsMode,
    settingsLines
  };
}

function resolveTogglePreference(preferenceObjects, key, defaultValue) {
  let onWeight = 1;
  let offWeight = 1;
  for (const preferences of preferenceObjects || []) {
    const value = String(preferences?.[key] || "").trim().toUpperCase();
    if (value === "ON") onWeight += 1;
    if (value === "OFF") offWeight += 1;
  }
  if (onWeight === offWeight) return defaultValue;
  return onWeight > offWeight;
}

function resolveEnumPreference(preferenceObjects, key, options, defaultValue) {
  const weights = new Map();
  for (const option of options) {
    weights.set(String(option).toUpperCase(), 1);
  }
  for (const preferences of preferenceObjects || []) {
    const value = String(preferences?.[key] || "").trim().toUpperCase();
    if (value === "" || value === "RANDOM") continue;
    if (weights.has(value)) {
      weights.set(value, weights.get(value) + 1);
    }
  }
  let best = String(defaultValue || options[0] || "").toUpperCase();
  let bestWeight = -1;
  for (const option of options) {
    const normalized = String(option).toUpperCase();
    const weight = Number(weights.get(normalized) || 0);
    if (weight > bestWeight) {
      best = normalized;
      bestWeight = weight;
    }
  }
  return best;
}

function resolveCardSize(preferenceObjects) {
  const sizeWeights = new Map([[2, 1], [3, 1], [4, 1], [5, 1]]);
  for (const preferences of preferenceObjects || []) {
    if (preferences?.randomCardSize) continue;
    const size = Number(preferences?.cardSize || 0);
    if (sizeWeights.has(size)) {
      sizeWeights.set(size, sizeWeights.get(size) + 1);
    }
  }
  let bestSize = 5;
  let bestWeight = -1;
  for (const [size, weight] of sizeWeights.entries()) {
    if (weight > bestWeight) {
      bestSize = size;
      bestWeight = weight;
    }
  }
  return `${bestSize}x${bestSize}`;
}

function resolveCardSizeValue(preferenceObjects) {
  const sizeText = resolveCardSize(preferenceObjects);
  const parsed = Number(String(sizeText).split("x")[0] || 5);
  return Number.isFinite(parsed) ? parsed : 5;
}

function resolveWorldType(preferenceObjects) {
  return Number(resolveEnumPreference(preferenceObjects, "worldTypeMode", ["0", "1", "2", "3", "4"], "0") || 0);
}

function resolveWorldTypeLabel(worldTypeMode) {
  return ({
    0: "Normal",
    1: "Amplified",
    2: "Superflat",
    3: "Single Biome",
    4: "Custom Biome Size"
  })[Number(worldTypeMode || 0)] || "Normal";
}

function resolvePrelitPortals(preferenceObjects) {
  return Number(resolveEnumPreference(preferenceObjects, "prelitPortalsMode", ["0", "1", "2", "3"], "0") || 0);
}

function resolvePrelitLabel(mode) {
  return ({
    0: "Off",
    1: "Nether",
    2: "End",
    3: "Both"
  })[Number(mode || 0)] || "Off";
}

function normalizePlayerName(value) {
  const text = String(value || "").trim();
  return text.slice(0, 64);
}

function normalizeQueueMode(value) {
  const text = String(value || "").trim().toUpperCase();
  return [
    "RANKED_1V1",
    "CASUAL_1V1",
    "CASUAL_FFA",
    "CASUAL_2S",
    "CASUAL_3S",
    "CASUAL_4S"
  ].includes(text) ? text : null;
}

function queueModeLabel(queueMode) {
  return ({
    RANKED_1V1: "Ranked 1v1",
    CASUAL_1V1: "Casual 1v1",
    CASUAL_FFA: "Casual FFA",
    CASUAL_2S: "Casual 2s",
    CASUAL_3S: "Casual 3s",
    CASUAL_4S: "Casual 4s"
  })[queueMode] || queueMode || "queue";
}

function requiredPlayersForQueueMode(queueMode) {
  return ({
    RANKED_1V1: 2,
    CASUAL_1V1: 2,
    CASUAL_FFA: 4,
    CASUAL_2S: 4,
    CASUAL_3S: 6,
    CASUAL_4S: 8
  })[queueMode] || 0;
}

function decorateSubmissionWithWeekly(row, weekly) {
  const submission = {
    playerName: row.playerName,
    cardSeed: row.cardSeed,
    worldSeed: row.worldSeed,
    settingsSeed: row.settingsSeed,
    durationSeconds: Number(row.durationSeconds || 0),
    finishedAtEpochSeconds: Number(row.finishedAtEpochSeconds || 0),
    completed: Boolean(row.completed),
    participantCount: Number(row.participantCount || 0),
    commandsUsed: Boolean(row.commandsUsed),
    voteRerollUsed: Boolean(row.voteRerollUsed),
    rerollsUsedCount: Number(row.rerollsUsedCount || 0),
    fakeRerollsUsedCount: Number(row.fakeRerollsUsedCount || 0),
    previewSize: Number(row.previewSize || 0),
    teamColorId: Number(row.teamColorId || 0),
    previewSlots: parseJsonArray(row.previewSlotsJson),
    previewSlotIds: parseJsonArray(row.previewSlotIdsJson),
    completedSlotIds: parseJsonArray(row.completedSlotIdsJson),
    opponentCompletedSlotIds: parseJsonArray(row.opponentCompletedSlotIdsJson),
    settingsLines: parseJsonArray(row.settingsJson),
    weeklyChallenge: Boolean(row.weeklyChallenge),
    weeklyChallengeId: row.weeklyChallengeId || "",
    leaderboardCategory: row.leaderboardCategory || "Custom",
    leaderboardCategoryReason: row.leaderboardCategoryReason || ""
  };

  if (matchesWeeklySubmission(submission, weekly)) {
    submission.weeklyChallenge = true;
    submission.weeklyChallengeId = weekly.challengeId || "";
    submission.leaderboardCategory = "Weekly";
    submission.leaderboardCategoryReason = `Matches ${weekly.challengeId || "the current weekly challenge"}`;
  }

  return submission;
}

function matchesWeeklySubmission(row, weekly) {
  if (!weekly) return false;
  const settingsSeed = String(row?.settingsSeed || "").trim();
  const worldSeed = String(row?.worldSeed || "").trim();
  const cardSeed = String(row?.cardSeed || "").trim();
  return Boolean(settingsSeed && worldSeed && cardSeed)
    && settingsSeed === String(weekly.settingsSeed || "").trim()
    && worldSeed === String(weekly.worldSeed || "").trim()
    && cardSeed === String(weekly.cardSeed || "").trim();
}
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key"
  };
}

function requireApiKey(request, env) {
  const expected = String(env.API_KEY || "").trim();
  if (!expected) return null;
  const provided = String(request.headers.get("X-API-Key") || "").trim();
  if (provided && provided === expected) return null;
  return Response.json({ ok: false, error: "Unauthorized" }, {
    status: 401,
    headers: corsHeaders()
  });
}

function parseJsonArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  try {
    const parsed = JSON.parse(value || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function currentSeasonStart() {
  const now = Math.floor(Date.now() / 1000);
  if (now <= WEEKLY_RESET_ANCHOR_EPOCH_SECONDS) {
    return WEEKLY_RESET_ANCHOR_EPOCH_SECONDS;
  }
  const elapsed = now - WEEKLY_RESET_ANCHOR_EPOCH_SECONDS;
  const cycles = Math.floor(elapsed / WEEKLY_RESET_PERIOD_SECONDS);
  return WEEKLY_RESET_ANCHOR_EPOCH_SECONDS + cycles * WEEKLY_RESET_PERIOD_SECONDS;
}

async function upsertWeeklyChallengeState(env, body) {
  try {
    await env.DB.prepare(`
      INSERT INTO weekly_challenge_state (
        challenge_id,
        base_seed,
        next_reset_epoch_seconds,
        settings_seed,
        world_seed,
        card_seed,
        preview_size,
        preview_slots_json,
        settings_json,
        updated_at_epoch_seconds
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(challenge_id) DO UPDATE SET
        base_seed = excluded.base_seed,
        next_reset_epoch_seconds = excluded.next_reset_epoch_seconds,
        settings_seed = excluded.settings_seed,
        world_seed = excluded.world_seed,
        card_seed = excluded.card_seed,
        preview_size = excluded.preview_size,
        preview_slots_json = excluded.preview_slots_json,
        settings_json = excluded.settings_json,
        updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
    `).bind(
      body.challengeId || "",
      Number(body.baseSeed || 0),
      Number(body.nextResetEpochSeconds || 0),
      body.settingsSeed || "",
      body.worldSeed || "",
      body.cardSeed || "",
      Number(body.previewSize || 0),
      JSON.stringify(asArray(body.previewSlots)),
      JSON.stringify(asArray(body.settingsLines)),
      Math.floor(Date.now() / 1000)
    ).run();
  } catch {
    await env.DB.prepare(`
      INSERT INTO weekly_challenge_state (
        challenge_id,
        base_seed,
        next_reset_epoch_seconds,
        settings_seed,
        world_seed,
        card_seed,
        preview_size,
        preview_slots_json,
        settings_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(challenge_id) DO UPDATE SET
        base_seed = excluded.base_seed,
        next_reset_epoch_seconds = excluded.next_reset_epoch_seconds,
        settings_seed = excluded.settings_seed,
        world_seed = excluded.world_seed,
        card_seed = excluded.card_seed,
        preview_size = excluded.preview_size,
        preview_slots_json = excluded.preview_slots_json,
        settings_json = excluded.settings_json
    `).bind(
      body.challengeId || "",
      Number(body.baseSeed || 0),
      Number(body.nextResetEpochSeconds || 0),
      body.settingsSeed || "",
      body.worldSeed || "",
      body.cardSeed || "",
      Number(body.previewSize || 0),
      JSON.stringify(asArray(body.previewSlots)),
      JSON.stringify(asArray(body.settingsLines))
    ).run();
  }
}







