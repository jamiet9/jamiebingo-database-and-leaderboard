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
      if (await shouldClearPreStartMatch(env, refreshedMatch, now)) {
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

    if (request.method === "POST" && url.pathname === "/online/match/sync") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      if (!playerName || !matchId) {
        return Response.json({ status: "error", message: "Player name and match id are required" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      const sender = await loadOnlineMatchPlayerRow(env, matchId, playerName);
      if (!sender) {
        return Response.json({ status: "error", message: "Match player not found" }, {
          status: 404,
          headers: corsHeaders()
        });
      }
      await upsertOnlineRuntimeState(env, {
        matchId,
        playerName,
        score: Number(body?.score || 0),
        completedLines: Number(body?.completedLines || 0),
        preferredColorId: Number(body?.preferredColorId ?? -1),
        completedSlotIdsJson: JSON.stringify(Array.isArray(body?.completedSlotIds) ? body.completedSlotIds : []),
        spawnPublished: Boolean(body?.spawnPublished),
        spawnX: Number(body?.spawnX || 0),
        spawnY: Number(body?.spawnY || 0),
        spawnZ: Number(body?.spawnZ || 0),
        updatedAtEpochSeconds: now
      });
      await env.DB.prepare(`
        UPDATE online_match_players
        SET disconnect_notice_epoch_seconds = 0
        WHERE match_id = ? AND player_name = ?
      `).bind(matchId, playerName).run();
      await upsertOnlineSlotClaims(env, {
        matchId,
        playerName,
        teamIndex: Number(sender.teamIndex || 0),
        completedSlotIds: Array.isArray(body?.completedSlotIds) ? body.completedSlotIds : [],
        claimedAtEpochSeconds: now
      });
      await upsertOnlineTeamMineState(env, {
        matchId,
        teamIndex: 0,
        active: Boolean(body?.mineSnapshot?.active),
        sourceQuestIdsJson: JSON.stringify(Array.isArray(body?.mineSnapshot?.sourceQuestIds) ? body.mineSnapshot.sourceQuestIds : []),
        displayNamesJson: JSON.stringify(Array.isArray(body?.mineSnapshot?.displayNames) ? body.mineSnapshot.displayNames : []),
        triggeredQuestId: normalizeText(body?.mineSnapshot?.triggeredQuestId || ""),
        deadlineEpochSeconds: Number(body?.mineSnapshot?.remainingSeconds) >= 0 ? now + Math.max(0, Number(body?.mineSnapshot?.remainingSeconds || 0)) : 0,
        progressQuestId: normalizeText(body?.mineSnapshot?.progressQuestId || ""),
        progressValue: Math.max(0, Number(body?.mineSnapshot?.progress || 0)),
        progressMax: Math.max(0, Number(body?.mineSnapshot?.progressMax || 0)),
        defuseQuestId: normalizeText(body?.mineSnapshot?.defuseQuestId || ""),
        defuseDisplayName: normalizeText(body?.mineSnapshot?.defuseDisplayName || ""),
        updatedAtEpochSeconds: now
      });
      await upsertOnlinePowerState(env, {
        matchId,
        active: Boolean(body?.powerSlotSnapshot?.active),
        slotId: normalizeText(body?.powerSlotSnapshot?.slotId || ""),
        displayName: normalizeText(body?.powerSlotSnapshot?.displayName || ""),
        deadlineEpochSeconds: Number(body?.powerSlotSnapshot?.remainingSeconds) >= 0
          ? now + Math.max(0, Number(body?.powerSlotSnapshot?.remainingSeconds || 0))
          : 0,
        claimed: Boolean(body?.powerSlotSnapshot?.claimed),
        resolutionNonce: Math.max(0, Number(body?.powerSlotSnapshot?.resolutionNonce || 0)),
        buffResult: Boolean(body?.powerSlotSnapshot?.buffResult),
        resolvedByPlayerName: normalizePlayerName(body?.powerSlotSnapshot?.resolvedByPlayerName || ""),
        updatedAtEpochSeconds: now
      });
      if (Array.isArray(body?.teamChestSlots)) {
        await upsertOnlineTeamChestState(env, {
          matchId,
          teamIndex: Number(sender.teamIndex || 0),
          chestSlotsJson: JSON.stringify(body.teamChestSlots),
          updatedAtEpochSeconds: now
        });
      }
      await env.DB.prepare(`
        UPDATE online_matches
        SET state = ?, updated_at_epoch_seconds = ?
        WHERE match_id = ? AND state IN ('ready_to_start', 'active')
      `).bind("active", now, matchId).run();
      return Response.json(await buildOnlineRuntimeSnapshot(env, matchId, Number(body?.chatCursor || 0), playerName), {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/publish-start") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const settingsSeed = normalizeText(body?.settingsSeed);
      const worldSeed = normalizeText(body?.worldSeed);
      const cardSeed = normalizeText(body?.cardSeed);
      if (!playerName || !matchId || !settingsSeed || !worldSeed || !cardSeed) {
        return Response.json({ status: "error", message: "Start payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_start_payloads (
          match_id,
          settings_seed,
          world_seed,
          card_seed,
          published_by_player_name,
          published_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id) DO NOTHING
      `).bind(matchId, settingsSeed, worldSeed, cardSeed, playerName, now).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET state = ?, updated_at_epoch_seconds = ?
        WHERE match_id = ? AND state IN ('revealing', 'ready_to_start', 'launching')
      `).bind("launching", now, matchId).run();
      return Response.json(await buildOnlineQueueSnapshot(env, playerName, "matched", "Start payload ready"), {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/draft/publish") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const draftState = normalizeDraftState(body?.draftState);
      if (!playerName || !matchId || !draftState) {
        return Response.json({ status: "error", message: "Draft publish payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const hostPlayerName = await loadOnlineMatchHostPlayerName(env, matchId);
      const existingRow = await env.DB.prepare(`
        SELECT state_json AS stateJson
        FROM online_match_draft_states
        WHERE match_id = ?
        LIMIT 1
      `).bind(matchId).first();
      const existingDraftState = normalizeDraftState(parseJsonObject(existingRow?.stateJson));
      const existingTurnOrder = Array.isArray(existingDraftState?.turnOrder)
        ? existingDraftState.turnOrder.map(normalizePlayerName).filter(Boolean)
        : [];
      const existingCurrentTurnPlayer = existingTurnOrder.length > 0
        ? existingTurnOrder[Math.max(0, Number(existingDraftState?.turnIndex || 0)) % existingTurnOrder.length]
        : "";
        const normalizedHost = normalizePlayerName(hostPlayerName).toLowerCase();
        const normalizedPlayer = normalizePlayerName(playerName).toLowerCase();
        const canPublishInitial = !existingDraftState && normalizedHost && normalizedHost === normalizedPlayer;
        const canPublishTurnAdvance = !!existingDraftState
          && !!existingCurrentTurnPlayer
          && normalizePlayerName(existingCurrentTurnPlayer).toLowerCase() === normalizedPlayer;
      if (!canPublishInitial && !canPublishTurnAdvance) {
        return Response.json({ status: "error", message: "Only the host can publish draft state" }, {
          status: 403,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_draft_states (
          match_id,
          state_json,
          updated_at_epoch_seconds
        ) VALUES (?, ?, ?)
        ON CONFLICT(match_id) DO UPDATE SET
          state_json = excluded.state_json,
          updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
      `).bind(matchId, JSON.stringify(draftState), now).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/draft/pick") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const choiceIndex = Math.max(0, Number(body?.choiceIndex || 0));
      const x = Math.max(0, Number(body?.x || 0));
      const y = Math.max(0, Number(body?.y || 0));
      if (!playerName || !matchId) {
        return Response.json({ status: "error", message: "Draft pick payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const row = await env.DB.prepare(`
        SELECT state_json AS stateJson
        FROM online_match_draft_states
        WHERE match_id = ?
        LIMIT 1
      `).bind(matchId).first();
      const draftState = normalizeDraftState(parseJsonObject(row?.stateJson));
      if (!draftState || !draftState.active || draftState.finished) {
        return Response.json({ status: "error", message: "Draft is not active" }, {
          status: 409,
          headers: corsHeaders()
        });
      }
      const turnOrder = Array.isArray(draftState.turnOrder) ? draftState.turnOrder.map(normalizePlayerName).filter(Boolean) : [];
      const currentTurnPlayer = turnOrder.length > 0
        ? turnOrder[Math.max(0, Number(draftState.turnIndex || 0)) % turnOrder.length]
        : "";
      if (!currentTurnPlayer || currentTurnPlayer !== playerName) {
        return Response.json({ status: "error", message: "It is not your draft turn" }, {
          status: 409,
          headers: corsHeaders()
        });
      }
      if (draftState.pendingPick && normalizePlayerName(draftState.pendingPick.playerName)) {
        return Response.json({ status: "error", message: "Draft pick already pending" }, {
          status: 409,
          headers: corsHeaders()
        });
      }
      const occupied = new Set((Array.isArray(draftState.slots) ? draftState.slots : []).map((slot) => `${Number(slot?.x || -1)},${Number(slot?.y || -1)}`));
      if (occupied.has(`${x},${y}`)) {
        return Response.json({ status: "error", message: "Draft slot already taken" }, {
          status: 409,
          headers: corsHeaders()
        });
      }
      const choices = Array.isArray(draftState.choices) ? draftState.choices : [];
      if (choiceIndex < 0 || choiceIndex >= choices.length) {
        return Response.json({ status: "error", message: "Draft choice is invalid" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      draftState.pendingPick = {
        playerName,
        choiceIndex,
        x,
        y
      };
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
          UPDATE online_match_draft_states
          SET state_json = ?, updated_at_epoch_seconds = ?
          WHERE match_id = ?
      `).bind(JSON.stringify(draftState), now, matchId).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/reroll/publish") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const rerollState = normalizeRerollState(body?.rerollState);
      if (!playerName || !matchId || !rerollState) {
        return Response.json({ status: "error", message: "Reroll publish payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const hostPlayerName = await loadOnlineMatchHostPlayerName(env, matchId);
      const existingRow = await env.DB.prepare(`
        SELECT state_json AS stateJson
        FROM online_match_reroll_states
        WHERE match_id = ?
        LIMIT 1
      `).bind(matchId).first();
      const existingRerollState = normalizeRerollState(parseJsonObject(existingRow?.stateJson));
      const existingTurnOrder = Array.isArray(existingRerollState?.turnOrder)
        ? existingRerollState.turnOrder.map(normalizePlayerName).filter(Boolean)
        : [];
      const existingCurrentTurnPlayer = existingTurnOrder.length > 0
        ? existingTurnOrder[Math.max(0, Number(existingRerollState?.turnIndex || 0)) % existingTurnOrder.length]
        : "";
      const normalizedHost = normalizePlayerName(hostPlayerName).toLowerCase();
      const normalizedPlayer = normalizePlayerName(playerName).toLowerCase();
      const canPublishInitial = !existingRerollState && normalizedHost && normalizedHost === normalizedPlayer;
      const canPublishTurnAdvance = !!existingRerollState
        && !!existingCurrentTurnPlayer
        && normalizePlayerName(existingCurrentTurnPlayer).toLowerCase() === normalizedPlayer;
      if (!canPublishInitial && !canPublishTurnAdvance) {
        return Response.json({ status: "error", message: "Only the host or active player can publish reroll state" }, {
          status: 403,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_reroll_states (
          match_id,
          state_json,
          updated_at_epoch_seconds
        ) VALUES (?, ?, ?)
        ON CONFLICT(match_id) DO UPDATE SET
          state_json = excluded.state_json,
          updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
      `).bind(matchId, JSON.stringify(rerollState), now).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/chat") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const channel = normalizeOnlineChatChannel(body?.channel);
      const message = normalizeChatMessage(body?.message);
      if (!playerName || !matchId || !message) {
        return Response.json({ status: "error", message: "Chat payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const sender = await loadOnlineMatchPlayerRow(env, matchId, playerName);
      if (!sender) {
        return Response.json({ status: "error", message: "Match player not found" }, {
          status: 404,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_chat_messages (
          match_id,
          player_name,
          channel,
          team_index,
          message,
          created_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?)
      `).bind(
        matchId,
        playerName,
        channel,
        Number(sender.teamIndex || 0),
        message,
        now
      ).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/vote-draw") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      if (!playerName || !matchId) {
        return Response.json({ status: "error", message: "Vote draw payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const sender = await loadOnlineMatchPlayerRow(env, matchId, playerName);
      if (!sender) {
        return Response.json({ status: "error", message: "Match player not found" }, {
          status: 404,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_draw_votes (
          match_id,
          player_name,
          voted_at_epoch_seconds
        ) VALUES (?, ?, ?)
        ON CONFLICT(match_id, player_name) DO UPDATE SET
          voted_at_epoch_seconds = excluded.voted_at_epoch_seconds
      `).bind(matchId, playerName, now).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      await appendSystemMatchChat(env, matchId, `${playerName} voted for a draw.`);
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/forfeit") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const reasonMessage = normalizeText(body?.reasonMessage);
      const systemMessage = normalizeText(body?.systemMessage);
      if (!playerName || !matchId) {
        return Response.json({ status: "error", message: "Forfeit payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const sender = await loadOnlineMatchPlayerRow(env, matchId, playerName);
      if (!sender) {
        return Response.json({ status: "error", message: "Match player not found" }, {
          status: 404,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_forfeits (
          match_id,
          player_name,
          forfeited_at_epoch_seconds,
          reason_text
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(match_id, player_name) DO UPDATE SET
          forfeited_at_epoch_seconds = excluded.forfeited_at_epoch_seconds,
          reason_text = excluded.reason_text
      `).bind(matchId, playerName, now, reasonMessage || "Player forfeited the game.").run();
      await env.DB.prepare(`
        DELETE FROM online_match_draw_votes
        WHERE match_id = ? AND player_name = ?
      `).bind(matchId, playerName).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      await appendSystemMatchChat(env, matchId, systemMessage || `${playerName} forfeited the online match.`);
      return Response.json({ status: "ok" }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/online/match/eliminate") {
      const body = await request.json();
      const playerName = normalizePlayerName(body?.playerName);
      const matchId = normalizeText(body?.matchId);
      const reasonMessage = normalizeText(body?.reasonMessage);
      const systemMessage = normalizeText(body?.systemMessage);
      if (!playerName || !matchId || !reasonMessage) {
        return Response.json({ status: "error", message: "Elimination payload incomplete" }, {
          status: 400,
          headers: corsHeaders()
        });
      }
      const sender = await loadOnlineMatchPlayerRow(env, matchId, playerName);
      if (!sender) {
        return Response.json({ status: "error", message: "Match player not found" }, {
          status: 404,
          headers: corsHeaders()
        });
      }
      const now = Math.floor(Date.now() / 1000);
      await env.DB.prepare(`
        INSERT INTO online_match_forfeits (
          match_id,
          player_name,
          forfeited_at_epoch_seconds,
          reason_text
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(match_id, player_name) DO UPDATE SET
          forfeited_at_epoch_seconds = excluded.forfeited_at_epoch_seconds,
          reason_text = excluded.reason_text
      `).bind(matchId, playerName, now, reasonMessage).run();
      await env.DB.prepare(`
        DELETE FROM online_match_draw_votes
        WHERE match_id = ? AND player_name = ?
      `).bind(matchId, playerName).run();
      await env.DB.prepare(`
        UPDATE online_matches
        SET updated_at_epoch_seconds = ?
        WHERE match_id = ?
      `).bind(now, matchId).run();
      await appendSystemMatchChat(env, matchId, systemMessage || `${playerName} was eliminated.`);
      return Response.json({ status: "ok" }, {
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
        team_index INTEGER NOT NULL DEFAULT 0,
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
        ALTER TABLE online_match_players
        ADD COLUMN team_index INTEGER NOT NULL DEFAULT 0
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
    try {
      await env.DB.prepare(`
        ALTER TABLE online_match_players
        ADD COLUMN disconnect_notice_epoch_seconds INTEGER NOT NULL DEFAULT 0
      `).run();
    } catch {
    }
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_runtime_states (
          match_id TEXT NOT NULL,
          player_name TEXT NOT NULL,
          score INTEGER NOT NULL DEFAULT 0,
          completed_lines INTEGER NOT NULL DEFAULT 0,
          preferred_color_id INTEGER NOT NULL DEFAULT -1,
          completed_slot_ids_json TEXT NOT NULL DEFAULT '[]',
          updated_at_epoch_seconds INTEGER NOT NULL,
          PRIMARY KEY (match_id, player_name)
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_shared_spawns (
          match_id TEXT PRIMARY KEY,
          x INTEGER NOT NULL,
          y INTEGER NOT NULL,
          z INTEGER NOT NULL,
          published_by_player_name TEXT NOT NULL DEFAULT '',
          updated_at_epoch_seconds INTEGER NOT NULL
        )
      `).run();
    await env.DB.prepare(`
      CREATE TABLE IF NOT EXISTS online_match_chat_messages (
        message_id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id TEXT NOT NULL,
        player_name TEXT NOT NULL,
        channel TEXT NOT NULL,
        team_index INTEGER NOT NULL DEFAULT 0,
        message TEXT NOT NULL,
        created_at_epoch_seconds INTEGER NOT NULL
      )
    `).run();
    await env.DB.prepare(`
      CREATE TABLE IF NOT EXISTS online_match_start_payloads (
        match_id TEXT PRIMARY KEY,
        settings_seed TEXT NOT NULL,
        world_seed TEXT NOT NULL,
        card_seed TEXT NOT NULL,
        published_by_player_name TEXT NOT NULL,
        published_at_epoch_seconds INTEGER NOT NULL
      )
    `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_team_chests (
          match_id TEXT NOT NULL,
          team_index INTEGER NOT NULL,
          chest_slots_json TEXT NOT NULL DEFAULT '[]',
          updated_at_epoch_seconds INTEGER NOT NULL,
          PRIMARY KEY (match_id, team_index)
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_slot_claims (
          match_id TEXT NOT NULL,
          slot_id TEXT NOT NULL,
          team_index INTEGER NOT NULL DEFAULT 0,
          claimed_by_player_name TEXT NOT NULL,
          claimed_at_epoch_seconds INTEGER NOT NULL,
          PRIMARY KEY (match_id, slot_id)
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_team_mines (
          match_id TEXT NOT NULL,
          team_index INTEGER NOT NULL,
          active INTEGER NOT NULL DEFAULT 0,
          source_quest_ids_json TEXT NOT NULL DEFAULT '[]',
          display_names_json TEXT NOT NULL DEFAULT '[]',
          triggered_quest_id TEXT NOT NULL DEFAULT '',
          deadline_epoch_seconds INTEGER NOT NULL DEFAULT 0,
          progress_quest_id TEXT NOT NULL DEFAULT '',
          progress_value INTEGER NOT NULL DEFAULT 0,
          progress_max INTEGER NOT NULL DEFAULT 0,
          defuse_quest_id TEXT NOT NULL DEFAULT '',
          defuse_display_name TEXT NOT NULL DEFAULT '',
          updated_at_epoch_seconds INTEGER NOT NULL,
          PRIMARY KEY (match_id, team_index)
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_power_state (
          match_id TEXT PRIMARY KEY,
          active INTEGER NOT NULL DEFAULT 0,
          slot_id TEXT NOT NULL DEFAULT '',
          display_name TEXT NOT NULL DEFAULT '',
          deadline_epoch_seconds INTEGER NOT NULL DEFAULT 0,
          claimed INTEGER NOT NULL DEFAULT 0,
          resolution_nonce INTEGER NOT NULL DEFAULT 0,
          buff_result INTEGER NOT NULL DEFAULT 0,
          resolved_by_player_name TEXT NOT NULL DEFAULT '',
          updated_at_epoch_seconds INTEGER NOT NULL
        )
      `).run();
      try {
        await env.DB.prepare(`
          ALTER TABLE online_match_power_state
          ADD COLUMN resolution_nonce INTEGER NOT NULL DEFAULT 0
        `).run();
      } catch {
      }
      try {
        await env.DB.prepare(`
          ALTER TABLE online_match_power_state
          ADD COLUMN buff_result INTEGER NOT NULL DEFAULT 0
        `).run();
      } catch {
      }
      try {
        await env.DB.prepare(`
          ALTER TABLE online_match_power_state
          ADD COLUMN resolved_by_player_name TEXT NOT NULL DEFAULT ''
        `).run();
      } catch {
      }
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_draft_states (
          match_id TEXT PRIMARY KEY,
          state_json TEXT NOT NULL DEFAULT '{}',
          updated_at_epoch_seconds INTEGER NOT NULL
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_reroll_states (
          match_id TEXT PRIMARY KEY,
          state_json TEXT NOT NULL DEFAULT '{}',
          updated_at_epoch_seconds INTEGER NOT NULL
        )
      `).run();
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS online_match_draw_votes (
        match_id TEXT NOT NULL,
        player_name TEXT NOT NULL,
        voted_at_epoch_seconds INTEGER NOT NULL,
        PRIMARY KEY (match_id, player_name)
      )
    `).run();
    await env.DB.prepare(`
      CREATE TABLE IF NOT EXISTS online_match_forfeits (
        match_id TEXT NOT NULL,
        player_name TEXT NOT NULL,
        forfeited_at_epoch_seconds INTEGER NOT NULL,
        reason_text TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (match_id, player_name)
      )
    `).run();
    try {
      await env.DB.prepare(`
        ALTER TABLE online_match_forfeits
        ADD COLUMN reason_text TEXT NOT NULL DEFAULT ''
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
    DELETE FROM online_match_start_payloads
    WHERE match_id IN (
      SELECT match_id
      FROM online_matches
      WHERE updated_at_epoch_seconds < ?
    )
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
  await env.DB.prepare(`
    DELETE FROM online_match_runtime_states
    WHERE updated_at_epoch_seconds < ?
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
  await env.DB.prepare(`
    DELETE FROM online_match_chat_messages
    WHERE created_at_epoch_seconds < ?
  `).bind(Number(nowEpochSeconds || 0) - (ONLINE_QUEUE_STALE_SECONDS * 2)).run();
  await env.DB.prepare(`
    DELETE FROM online_match_draw_votes
    WHERE match_id IN (
      SELECT match_id
      FROM online_matches
      WHERE updated_at_epoch_seconds < ?
    )
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
  await env.DB.prepare(`
    DELETE FROM online_match_forfeits
    WHERE match_id IN (
      SELECT match_id
      FROM online_matches
      WHERE updated_at_epoch_seconds < ?
    )
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
  await env.DB.prepare(`
    DELETE FROM online_match_power_state
    WHERE match_id IN (
      SELECT match_id
      FROM online_matches
      WHERE updated_at_epoch_seconds < ?
    )
  `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
    await env.DB.prepare(`
      DELETE FROM online_match_team_chests
      WHERE updated_at_epoch_seconds < ?
    `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
    await env.DB.prepare(`
      DELETE FROM online_match_slot_claims
      WHERE match_id IN (
        SELECT match_id
        FROM online_matches
        WHERE updated_at_epoch_seconds < ?
      )
    `).bind(Number(nowEpochSeconds || 0) - ONLINE_QUEUE_STALE_SECONDS).run();
    await env.DB.prepare(`
      DELETE FROM online_match_team_mines
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
  let refreshedActiveMatch = playerName ? await loadActiveMatchForPlayer(env, playerName) : null;
  if (await shouldClearPreStartMatch(env, refreshedActiveMatch, now)) {
    await clearActiveMatch(env, refreshedActiveMatch.matchId);
    refreshedActiveMatch = null;
  }
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

function isTerminalMatchState(state) {
  const normalized = String(state || "").trim().toLowerCase();
  return normalized === "finished" || normalized === "drawn";
}

function isPreStartState(state) {
  const normalized = String(state || "").trim().toLowerCase();
  return normalized === "pending_ready"
    || normalized === "revealing"
    || normalized === "ready_to_start"
    || normalized === "launching";
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
      m.updated_at_epoch_seconds AS updatedAtEpochSeconds,
      m.match_payload_json AS matchPayloadJson
    FROM online_match_players p
    JOIN online_matches m ON m.match_id = p.match_id
    WHERE p.player_name = ?
    LIMIT 1
  `).bind(playerName).first();
  if (!row) return null;
  const now = Math.floor(Date.now() / 1000);
  const state = String(row.state || "pending").trim().toLowerCase();
  const updatedAt = Number(row.updatedAtEpochSeconds || 0);
  const createdAt = Number(row.createdAtEpochSeconds || 0);
  const staleAnchor = Math.max(updatedAt, createdAt);
  if (isTerminalMatchState(state)) {
    await clearActiveMatch(env, row.matchId);
    return null;
  }
  if (isPreStartState(state) && staleAnchor > 0 && now >= staleAnchor + 180) {
    await clearActiveMatch(env, row.matchId);
    return null;
  }
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
  const startPayloadRow = await env.DB.prepare(`
    SELECT
      settings_seed AS settingsSeed,
      world_seed AS worldSeed,
      card_seed AS cardSeed,
      published_by_player_name AS publishedByPlayerName,
      published_at_epoch_seconds AS publishedAtEpochSeconds
    FROM online_match_start_payloads
    WHERE match_id = ?
    LIMIT 1
  `).bind(row.matchId).first();
  return {
    matchId: row.matchId,
    queueMode: normalizeQueueMode(row.queueMode),
    state: String(row.state || "pending"),
    targetPlayerCount: Number(row.targetPlayerCount || 0),
    createdAtEpochSeconds: Number(row.createdAtEpochSeconds || 0),
    startAfterEpochSeconds: Number(row.startAfterEpochSeconds || 0),
    settingsLines: parseJsonArray(tryParseMatchPayload(row.matchPayloadJson)?.settingsLines),
    definitionJson: String(row.matchPayloadJson || "{}"),
    startPayload: startPayloadRow ? {
      settingsSeed: String(startPayloadRow.settingsSeed || ""),
      worldSeed: String(startPayloadRow.worldSeed || ""),
      cardSeed: String(startPayloadRow.cardSeed || ""),
      publishedByPlayerName: normalizePlayerName(startPayloadRow.publishedByPlayerName),
      publishedAtEpochSeconds: Number(startPayloadRow.publishedAtEpochSeconds || 0)
    } : null,
    playerNames,
    readyPlayerNames
  };
}

async function loadOnlineMatchPlayerRow(env, matchId, playerName) {
  return await env.DB.prepare(`
    SELECT team_index AS teamIndex
    FROM online_match_players
    WHERE match_id = ? AND player_name = ?
    LIMIT 1
  `).bind(matchId, playerName).first();
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
  const hasStartPayload = !!(await env.DB.prepare(`
    SELECT match_id AS matchId
    FROM online_match_start_payloads
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first());

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

  if (currentState === "revealing" && !hasStartPayload && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0) + 30) {
    await clearActiveMatch(env, matchId);
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
    return;
  }

  if (currentState === "ready_to_start" && !hasStartPayload && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0) + 30) {
    await clearActiveMatch(env, matchId);
    return;
  }

  if (currentState === "launching" && !hasStartPayload && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0) + 30) {
    await clearActiveMatch(env, matchId);
    return;
  }

  if (currentState === "launching" && Number(match.startAfterEpochSeconds || 0) > 0 && now >= Number(match.startAfterEpochSeconds || 0) + 5 * 60) {
    const runtimeRow = await env.DB.prepare(`
      SELECT match_id AS matchId
      FROM online_match_runtime_states
      WHERE match_id = ?
      LIMIT 1
    `).bind(matchId).first();
    if (!runtimeRow) {
      await clearActiveMatch(env, matchId);
    }
  }
}

async function clearActiveMatch(env, matchId) {
  if (!matchId) return;
  await env.DB.prepare(`
    DELETE FROM online_match_start_payloads
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_runtime_states
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_shared_spawns
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_chat_messages
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_draw_votes
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_slot_claims
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_team_mines
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_power_state
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_draft_states
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_reroll_states
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_forfeits
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_match_players
    WHERE match_id = ?
  `).bind(matchId).run();
  await env.DB.prepare(`
    DELETE FROM online_matches
    WHERE match_id = ?
  `).bind(matchId).run();
}

async function shouldClearPreStartMatch(env, activeMatch, nowEpochSeconds) {
  if (!activeMatch?.matchId) return false;
  const state = String(activeMatch.state || "").trim().toLowerCase();
  if (!["pending_ready", "revealing", "ready_to_start", "launching"].includes(state)) {
    return false;
  }

  const now = Number(nowEpochSeconds || 0);
  const createdAt = Number(activeMatch.createdAtEpochSeconds || 0);
  const startAfter = Number(activeMatch.startAfterEpochSeconds || 0);
  const startPayloadPublishedAt = Number(activeMatch.startPayload?.publishedAtEpochSeconds || 0);
  const staleAnchor = Math.max(createdAt, startAfter, startPayloadPublishedAt);
  const staleDelay = state === "pending_ready" ? 30 : 10;
  if (staleAnchor <= 0 || now < staleAnchor + staleDelay) {
    return false;
  }

  const runtimeRow = await env.DB.prepare(`
    SELECT match_id AS matchId
    FROM online_match_runtime_states
    WHERE match_id = ?
    LIMIT 1
  `).bind(activeMatch.matchId).first();
  return !runtimeRow;
}

async function upsertOnlineRuntimeState(env, state) {
  await env.DB.prepare(`
    INSERT INTO online_match_runtime_states (
      match_id,
      player_name,
      score,
      completed_lines,
      preferred_color_id,
      completed_slot_ids_json,
      updated_at_epoch_seconds
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(match_id, player_name) DO UPDATE SET
      score = excluded.score,
      completed_lines = excluded.completed_lines,
      preferred_color_id = excluded.preferred_color_id,
      completed_slot_ids_json = excluded.completed_slot_ids_json,
      updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
  `).bind(
    state.matchId,
    state.playerName,
    Math.max(0, Number(state.score || 0)),
    Math.max(0, Number(state.completedLines || 0)),
      Number.isFinite(Number(state.preferredColorId)) ? Number(state.preferredColorId) : -1,
      String(state.completedSlotIdsJson || "[]"),
      Number(state.updatedAtEpochSeconds || 0)
    ).run();
  if (Number(state.spawnPublished || 0) !== 0) {
    const x = Math.floor(Number(state.spawnX || 0));
    const y = Math.floor(Number(state.spawnY || 0));
    const z = Math.floor(Number(state.spawnZ || 0));
    if (Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z) && y > 0) {
      await env.DB.prepare(`
        INSERT INTO online_match_shared_spawns (
          match_id,
          x,
          y,
          z,
          published_by_player_name,
          updated_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id) DO NOTHING
      `).bind(
        state.matchId,
        x,
        y,
        z,
        state.playerName || "",
        Number(state.updatedAtEpochSeconds || 0)
      ).run();
    }
  }
}

async function loadOnlineMatchHostPlayerName(env, matchId) {
  const row = await env.DB.prepare(`
    SELECT player_name AS playerName
    FROM online_match_players
    WHERE match_id = ?
    ORDER BY joined_at_epoch_seconds ASC, player_name ASC
    LIMIT 1
  `).bind(matchId).first();
  return normalizePlayerName(row?.playerName);
}

function normalizeDraftChoice(choice) {
  if (!choice || typeof choice !== "object") return null;
  const id = normalizeText(choice.id || "");
  if (!id) return null;
  return {
    id,
    name: normalizeText(choice.name || ""),
    category: normalizeText(choice.category || ""),
    rarity: normalizeText(choice.rarity || ""),
    isQuest: Boolean(choice.isQuest)
  };
}

function normalizeDraftSlot(slot) {
  if (!slot || typeof slot !== "object") return null;
  const choice = normalizeDraftChoice(slot);
  if (!choice) return null;
  return {
    ...choice,
    x: Math.max(0, Number(slot.x || 0)),
    y: Math.max(0, Number(slot.y || 0))
  };
}

function normalizeDraftState(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const turnOrder = Array.isArray(value.turnOrder)
    ? value.turnOrder.map(normalizePlayerName).filter(Boolean)
    : [];
  const choices = Array.isArray(value.choices)
    ? value.choices.map(normalizeDraftChoice).filter(Boolean)
    : [];
  const slots = Array.isArray(value.slots)
    ? value.slots.map(normalizeDraftSlot).filter(Boolean)
    : [];
  const pending = value.pendingPick && typeof value.pendingPick === "object"
    ? {
        playerName: normalizePlayerName(value.pendingPick.playerName),
        choiceIndex: Math.max(0, Number(value.pendingPick.choiceIndex || 0)),
        x: Math.max(0, Number(value.pendingPick.x || 0)),
        y: Math.max(0, Number(value.pendingPick.y || 0))
      }
    : null;
  const active = Boolean(value.active);
  const finished = Boolean(value.finished);
  if (!active && !finished && turnOrder.length === 0 && choices.length === 0 && slots.length === 0 && !pending?.playerName) {
    return null;
  }
  return {
    active,
    finished,
    hostPlayerName: normalizePlayerName(value.hostPlayerName),
    turnOrder,
    turnIndex: Math.max(0, Number(value.turnIndex || 0)),
    choices,
    slots,
    pendingPick: pending && pending.playerName ? pending : null
  };
}

function normalizeRerollSlot(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const id = normalizeText(value.id);
  const name = normalizeText(value.name);
  if (!id || !name) return null;
  return {
    x: Math.max(0, Number(value.x || 0)),
    y: Math.max(0, Number(value.y || 0)),
    id,
    name,
    category: normalizeText(value.category),
    rarity: normalizeText(value.rarity),
    isQuest: Boolean(value.isQuest)
  };
}

function normalizeRerollPlayer(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const playerName = normalizePlayerName(value.playerName);
  if (!playerName) return null;
  return {
    playerName,
    remaining: Math.max(0, Number(value.remaining || 0))
  };
}

function normalizeRerollEvent(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const slot = normalizeRerollSlot(value);
  if (!slot) return null;
  return {
    nonce: Math.max(0, Number(value.nonce || 0)),
    x: slot.x,
    y: slot.y,
    id: slot.id,
    name: slot.name,
    category: slot.category,
    rarity: slot.rarity,
    isQuest: slot.isQuest
  };
}

function normalizeRerollState(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const turnOrder = Array.isArray(value.turnOrder)
    ? value.turnOrder.map(normalizePlayerName).filter(Boolean)
    : [];
  const players = Array.isArray(value.players)
    ? value.players.map(normalizeRerollPlayer).filter(Boolean)
    : [];
  const slots = Array.isArray(value.slots)
    ? value.slots.map(normalizeRerollSlot).filter(Boolean)
    : [];
  const lastEvent = normalizeRerollEvent(value.lastEvent);
  const active = Boolean(value.active);
  const finished = Boolean(value.finished);
  if (!active && !finished && turnOrder.length === 0 && players.length === 0 && slots.length === 0 && !lastEvent) {
    return null;
  }
  return {
    active,
    finished,
    turnOrder,
    turnIndex: Math.max(0, Number(value.turnIndex || 0)),
    players,
    slots,
    lastEvent
  };
}

async function upsertOnlineTeamChestState(env, state) {
  await env.DB.prepare(`
    INSERT INTO online_match_team_chests (
      match_id,
      team_index,
      chest_slots_json,
      updated_at_epoch_seconds
    ) VALUES (?, ?, ?, ?)
    ON CONFLICT(match_id, team_index) DO UPDATE SET
      chest_slots_json = excluded.chest_slots_json,
      updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
  `).bind(
    state.matchId,
    Number(state.teamIndex || 0),
      String(state.chestSlotsJson || "[]"),
      Number(state.updatedAtEpochSeconds || 0)
    ).run();
  }

async function upsertOnlineSlotClaims(env, state) {
  const matchId = normalizeText(state?.matchId);
  const playerName = normalizePlayerName(state?.playerName);
  const teamIndex = Math.max(0, Number(state?.teamIndex || 0));
  const claimedAtEpochSeconds = Number(state?.claimedAtEpochSeconds || 0);
  const completedSlotIds = Array.isArray(state?.completedSlotIds) ? state.completedSlotIds : [];
  for (const rawSlotId of completedSlotIds) {
    const slotId = normalizeChatMessage(rawSlotId);
    if (!matchId || !playerName || !slotId) continue;
    await env.DB.prepare(`
      INSERT INTO online_match_slot_claims (
        match_id,
        slot_id,
        team_index,
        claimed_by_player_name,
        claimed_at_epoch_seconds
      ) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(match_id, slot_id) DO NOTHING
    `).bind(
      matchId,
      slotId,
      teamIndex,
      playerName,
      claimedAtEpochSeconds
    ).run();
    }
  }

async function upsertOnlineTeamMineState(env, state) {
  const existing = await env.DB.prepare(`
    SELECT
      active AS active,
      source_quest_ids_json AS sourceQuestIdsJson,
      display_names_json AS displayNamesJson,
      triggered_quest_id AS triggeredQuestId,
      deadline_epoch_seconds AS deadlineEpochSeconds,
      progress_quest_id AS progressQuestId,
      progress_value AS progressValue,
      progress_max AS progressMax,
      defuse_quest_id AS defuseQuestId,
      defuse_display_name AS defuseDisplayName
    FROM online_match_team_mines
    WHERE match_id = ? AND team_index = ?
    LIMIT 1
  `).bind(state.matchId, Number(state.teamIndex || 0)).first();
  const existingSourceQuestIdsJson = String(existing?.sourceQuestIdsJson || "[]");
  const existingDisplayNamesJson = String(existing?.displayNamesJson || "[]");
  const incomingSourceQuestIds = parseJsonArray(state.sourceQuestIdsJson);
  const incomingDisplayNames = parseJsonArray(state.displayNamesJson);
  const sourceQuestIdsJson = parseJsonArray(existingSourceQuestIdsJson).length > 0
    ? existingSourceQuestIdsJson
    : JSON.stringify(incomingSourceQuestIds);
  const displayNamesJson = parseJsonArray(existingDisplayNamesJson).length > 0
    ? existingDisplayNamesJson
    : JSON.stringify(incomingDisplayNames);
  const triggeredQuestId = normalizeText(state.triggeredQuestId || "") || normalizeText(existing?.triggeredQuestId || "");
  const deadlineEpochSeconds = Number(state.deadlineEpochSeconds || 0) > 0
    ? Number(state.deadlineEpochSeconds || 0)
    : Number(existing?.deadlineEpochSeconds || 0);
  const progressQuestId = normalizeText(state.progressQuestId || "") || normalizeText(existing?.progressQuestId || "");
  const progressValue = progressQuestId ? Math.max(
    0,
    Number(state.progressValue || 0),
    Number(existing?.progressValue || 0)
  ) : 0;
  const progressMax = progressQuestId ? Math.max(
    0,
    Number(state.progressMax || 0),
    Number(existing?.progressMax || 0)
  ) : 0;
  const defuseQuestId = normalizeText(state.defuseQuestId || "") || normalizeText(existing?.defuseQuestId || "");
  const defuseDisplayName = normalizeText(state.defuseDisplayName || "") || normalizeText(existing?.defuseDisplayName || "");
  await env.DB.prepare(`
    INSERT INTO online_match_team_mines (
      match_id,
      team_index,
      active,
      source_quest_ids_json,
      display_names_json,
      triggered_quest_id,
      deadline_epoch_seconds,
      progress_quest_id,
      progress_value,
      progress_max,
      defuse_quest_id,
      defuse_display_name,
      updated_at_epoch_seconds
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(match_id, team_index) DO UPDATE SET
      active = excluded.active,
      source_quest_ids_json = excluded.source_quest_ids_json,
      display_names_json = excluded.display_names_json,
      triggered_quest_id = excluded.triggered_quest_id,
      deadline_epoch_seconds = excluded.deadline_epoch_seconds,
      progress_quest_id = excluded.progress_quest_id,
      progress_value = excluded.progress_value,
      progress_max = excluded.progress_max,
      defuse_quest_id = excluded.defuse_quest_id,
      defuse_display_name = excluded.defuse_display_name,
      updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
  `).bind(
    state.matchId,
    Number(state.teamIndex || 0),
    (state.active || Number(existing?.active || 0) !== 0) ? 1 : 0,
    sourceQuestIdsJson,
    displayNamesJson,
    triggeredQuestId,
    deadlineEpochSeconds,
    progressQuestId,
    progressValue,
    progressMax,
    defuseQuestId,
    defuseDisplayName,
    Number(state.updatedAtEpochSeconds || 0)
  ).run();
}

async function upsertOnlinePowerState(env, state) {
  const existing = await env.DB.prepare(`
    SELECT
      active AS active,
      slot_id AS slotId,
      display_name AS displayName,
      deadline_epoch_seconds AS deadlineEpochSeconds,
      claimed AS claimed,
      resolution_nonce AS resolutionNonce,
      buff_result AS buffResult,
      resolved_by_player_name AS resolvedByPlayerName
    FROM online_match_power_state
    WHERE match_id = ?
    LIMIT 1
  `).bind(state.matchId).first();
  const existingSlotId = normalizeText(existing?.slotId || "");
  const incomingSlotId = normalizeText(state.slotId || "");
  const resolvedSlotId = existingSlotId || incomingSlotId;
  const resolvedDisplayName = normalizeText(existing?.displayName || "") || normalizeText(state.displayName || "");
  const wasClaimed = Number(existing?.claimed || 0) !== 0;
  const isNewClaim = !wasClaimed && Boolean(state.claimed) && !!resolvedSlotId && (!existingSlotId || existingSlotId === incomingSlotId);
  const resolutionNonce = isNewClaim
    ? Math.max(Number(existing?.resolutionNonce || 0) + 1, Number(state.resolutionNonce || 0), 1)
    : Math.max(0, Number(existing?.resolutionNonce || 0), Number(state.resolutionNonce || 0));
  const claimed = wasClaimed || Boolean(state.claimed);
  const deadlineEpochSeconds = claimed
    ? Math.max(0, Number(state.deadlineEpochSeconds || 0), Number(existing?.deadlineEpochSeconds || 0))
    : Math.max(
        0,
        existingSlotId && incomingSlotId && existingSlotId !== incomingSlotId
          ? Number(existing?.deadlineEpochSeconds || 0)
          : Math.max(Number(state.deadlineEpochSeconds || 0), Number(existing?.deadlineEpochSeconds || 0))
      );
  const buffResult = isNewClaim
    ? (state.buffResult ? 1 : 0)
    : Math.max(0, Number(existing?.buffResult || 0));
  const resolvedByPlayerName = isNewClaim
    ? normalizePlayerName(state.resolvedByPlayerName || "")
    : normalizePlayerName(existing?.resolvedByPlayerName || "");
  await env.DB.prepare(`
    INSERT INTO online_match_power_state (
      match_id,
      active,
      slot_id,
      display_name,
      deadline_epoch_seconds,
      claimed,
      resolution_nonce,
      buff_result,
      resolved_by_player_name,
      updated_at_epoch_seconds
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(match_id) DO UPDATE SET
      active = excluded.active,
      slot_id = excluded.slot_id,
      display_name = excluded.display_name,
      deadline_epoch_seconds = excluded.deadline_epoch_seconds,
      claimed = excluded.claimed,
      resolution_nonce = excluded.resolution_nonce,
      buff_result = excluded.buff_result,
      resolved_by_player_name = excluded.resolved_by_player_name,
      updated_at_epoch_seconds = excluded.updated_at_epoch_seconds
  `).bind(
    state.matchId,
    (state.active || Number(existing?.active || 0) !== 0) ? 1 : 0,
    resolvedSlotId,
    resolvedDisplayName,
    deadlineEpochSeconds,
    claimed ? 1 : 0,
    resolutionNonce,
    buffResult,
    resolvedByPlayerName,
    Number(state.updatedAtEpochSeconds || 0)
  ).run();
}

async function appendSystemMatchChat(env, matchId, message) {
  const text = normalizeChatMessage(message);
  if (!matchId || !text) return;
  await env.DB.prepare(`
    INSERT INTO online_match_chat_messages (
      match_id,
      player_name,
      channel,
      team_index,
      message,
      created_at_epoch_seconds
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).bind(
    matchId,
    "System",
    "GLOBAL",
    -1,
    text,
    Math.floor(Date.now() / 1000)
  ).run();
}

async function autoForfeitStalePlayers(env, matchId, nowEpochSeconds) {
  const matchRow = await env.DB.prepare(`
    SELECT state AS state
    FROM online_matches
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const matchState = normalizeText(matchRow?.state || "").toLowerCase();
  if (matchState !== "active") {
    return;
  }
  const { results } = await env.DB.prepare(`
    SELECT
      p.player_name AS playerName,
      COALESCE(r.updated_at_epoch_seconds, 0) AS updatedAtEpochSeconds,
      COALESCE(p.joined_at_epoch_seconds, 0) AS joinedAtEpochSeconds,
      COALESCE(p.disconnect_notice_epoch_seconds, 0) AS disconnectNoticeEpochSeconds
    FROM online_match_players p
    LEFT JOIN online_match_runtime_states r
      ON r.match_id = p.match_id AND r.player_name = p.player_name
    LEFT JOIN online_match_forfeits f
      ON f.match_id = p.match_id AND f.player_name = p.player_name
    WHERE p.match_id = ?
      AND f.player_name IS NULL
  `).bind(matchId).all();
  for (const row of results || []) {
    const playerName = normalizePlayerName(row?.playerName);
    const updatedAt = Number(row?.updatedAtEpochSeconds || 0);
    const disconnectNoticeAt = Number(row?.disconnectNoticeEpochSeconds || 0);
    const lastSeenAt = updatedAt;
    if (!playerName || lastSeenAt <= 0) continue;
    if (disconnectNoticeAt <= 0 && nowEpochSeconds >= lastSeenAt + 10) {
      await env.DB.prepare(`
        UPDATE online_match_players
        SET disconnect_notice_epoch_seconds = ?
        WHERE match_id = ? AND player_name = ?
      `).bind(nowEpochSeconds, matchId, playerName).run();
      await appendSystemMatchChat(env, matchId, `${playerName} disconnected. Reconnect within 2 minutes or forfeit.`);
    }
    if (nowEpochSeconds < lastSeenAt + 120) continue;
    await env.DB.prepare(`
      INSERT INTO online_match_forfeits (
        match_id,
        player_name,
        forfeited_at_epoch_seconds,
        reason_text
      ) VALUES (?, ?, ?, ?)
      ON CONFLICT(match_id, player_name) DO UPDATE SET
        forfeited_at_epoch_seconds = excluded.forfeited_at_epoch_seconds,
        reason_text = excluded.reason_text
    `).bind(matchId, playerName, nowEpochSeconds, "Player disconnected.").run();
    await env.DB.prepare(`
      DELETE FROM online_match_draw_votes
      WHERE match_id = ? AND player_name = ?
    `).bind(matchId, playerName).run();
    await appendSystemMatchChat(env, matchId, `${playerName} disconnected and was eliminated.`);
  }
}

async function buildOnlineRuntimeSnapshot(env, matchId, chatCursor, playerName) {
  await autoForfeitStalePlayers(env, matchId, Math.floor(Date.now() / 1000));
  const viewer = playerName ? await loadOnlineMatchPlayerRow(env, matchId, playerName) : null;
  const viewerTeamIndex = Number(viewer?.teamIndex || 0);
  const matchRow = await env.DB.prepare(`
    SELECT match_payload_json AS matchPayloadJson
    FROM online_matches
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const forfeitsResult = await env.DB.prepare(`
    SELECT player_name AS playerName, reason_text AS reasonText
    FROM online_match_forfeits
    WHERE match_id = ?
  `).bind(matchId).all();
  const drawVotesResult = await env.DB.prepare(`
    SELECT player_name AS playerName
    FROM online_match_draw_votes
    WHERE match_id = ?
  `).bind(matchId).all();
  const playersResult = await env.DB.prepare(`
    SELECT
      p.player_name AS playerName,
      p.team_index AS teamIndex,
      p.ready AS ready,
      COALESCE(r.score, 0) AS score,
      COALESCE(r.completed_lines, 0) AS completedLines,
      COALESCE(r.preferred_color_id, -1) AS preferredColorId,
      COALESCE(r.completed_slot_ids_json, '[]') AS completedSlotIdsJson,
      COALESCE(r.updated_at_epoch_seconds, 0) AS updatedAtEpochSeconds
    FROM online_match_players p
    LEFT JOIN online_match_runtime_states r
      ON r.match_id = p.match_id AND r.player_name = p.player_name
    WHERE p.match_id = ?
    ORDER BY p.joined_at_epoch_seconds ASC, p.player_name ASC
  `).bind(matchId).all();

  const chatResult = await env.DB.prepare(`
    SELECT
      message_id AS messageId,
      player_name AS playerName,
      channel AS channel,
      team_index AS teamIndex,
      message AS message,
      created_at_epoch_seconds AS createdAtEpochSeconds
    FROM online_match_chat_messages
    WHERE match_id = ? AND message_id > ?
    ORDER BY message_id ASC
    LIMIT 100
  `).bind(matchId, Math.max(0, Number(chatCursor || 0))).all();
  const slotClaimsResult = await env.DB.prepare(`
    SELECT
      slot_id AS slotId,
      team_index AS teamIndex
    FROM online_match_slot_claims
    WHERE match_id = ?
  `).bind(matchId).all();
  const forfeitedPlayers = new Set((forfeitsResult.results || []).map((row) => normalizePlayerName(row?.playerName)).filter(Boolean));
  const drawVoters = new Set((drawVotesResult.results || []).map((row) => normalizePlayerName(row?.playerName)).filter(Boolean));
  const players = (playersResult.results || []).map((row) => ({
    playerName: normalizePlayerName(row.playerName),
    teamIndex: Number(row.teamIndex || 0),
    ready: Number(row.ready || 0) !== 0,
    score: Number(row.score || 0),
    completedLines: Number(row.completedLines || 0),
    preferredColorId: Number(row.preferredColorId ?? -1),
    completedSlotIds: parseJsonArray(row.completedSlotIdsJson),
    synced: Number(row.updatedAtEpochSeconds || 0) > 0,
    forfeited: forfeitedPlayers.has(normalizePlayerName(row.playerName))
  }));
  const syncedPlayerCount = players.filter((player) => player.synced && !player.forfeited).length;
  const teamCompletedSlotIds = Array.from(new Set(
    players
      .filter((player) => Number(player.teamIndex || 0) === viewerTeamIndex)
      .flatMap((player) => Array.isArray(player.completedSlotIds) ? player.completedSlotIds : [])
      .filter((slotId) => typeof slotId === "string" && slotId.trim() !== "")
  ));
  const teamChestRow = await env.DB.prepare(`
    SELECT chest_slots_json AS chestSlotsJson
    FROM online_match_team_chests
    WHERE match_id = ? AND team_index = ?
    LIMIT 1
  `).bind(matchId, viewerTeamIndex).first();
  const teamMineRow = await env.DB.prepare(`
    SELECT
      active AS active,
      source_quest_ids_json AS sourceQuestIdsJson,
      display_names_json AS displayNamesJson,
      triggered_quest_id AS triggeredQuestId,
      deadline_epoch_seconds AS deadlineEpochSeconds,
      progress_quest_id AS progressQuestId,
      progress_value AS progressValue,
      progress_max AS progressMax,
      defuse_quest_id AS defuseQuestId,
      defuse_display_name AS defuseDisplayName
    FROM online_match_team_mines
    WHERE match_id = ? AND team_index = 0
    LIMIT 1
  `).bind(matchId).first();
  const powerStateRow = await env.DB.prepare(`
    SELECT
      active AS active,
      slot_id AS slotId,
      display_name AS displayName,
      deadline_epoch_seconds AS deadlineEpochSeconds,
      claimed AS claimed,
      resolution_nonce AS resolutionNonce,
      buff_result AS buffResult,
      resolved_by_player_name AS resolvedByPlayerName
    FROM online_match_power_state
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const draftStateRow = await env.DB.prepare(`
    SELECT state_json AS stateJson
    FROM online_match_draft_states
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const rerollStateRow = await env.DB.prepare(`
    SELECT state_json AS stateJson
    FROM online_match_reroll_states
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const sharedSpawnRow = await env.DB.prepare(`
    SELECT
      x AS x,
      y AS y,
      z AS z,
      published_by_player_name AS publishedByPlayerName
    FROM online_match_shared_spawns
    WHERE match_id = ?
    LIMIT 1
  `).bind(matchId).first();
  const slotOwnershipTeamIndices = {};
  for (const row of slotClaimsResult.results || []) {
    const slotId = normalizeChatMessage(row?.slotId);
    if (!slotId) continue;
    slotOwnershipTeamIndices[slotId] = Math.max(0, Number(row?.teamIndex || 0));
  }
  const forfeitReasons = new Map((forfeitsResult.results || [])
    .map((row) => [normalizePlayerName(row?.playerName), normalizeText(row?.reasonText)]));
  const outcome = computeOnlineMatchOutcome(tryParseMatchPayload(matchRow?.matchPayloadJson), players, drawVoters, forfeitReasons);
  if (outcome.resultState !== "active") {
    await env.DB.prepare(`
      UPDATE online_matches
      SET state = ?, updated_at_epoch_seconds = ?
      WHERE match_id = ?
    `).bind(outcome.resultState === "draw" ? "drawn" : "finished", Math.floor(Date.now() / 1000), matchId).run();
  }

  return {
    status: "ok",
    players,
    drawVotes: drawVoters.size,
    activePlayers: players.filter((player) => !player.forfeited).length,
    syncedPlayerCount,
    sharedSpawnX: Number(sharedSpawnRow?.x || 0),
    sharedSpawnY: Number(sharedSpawnRow?.y || 0),
    sharedSpawnZ: Number(sharedSpawnRow?.z || 0),
    sharedSpawnPublishedByPlayerName: normalizePlayerName(sharedSpawnRow?.publishedByPlayerName),
    localDrawVoted: playerName ? drawVoters.has(playerName) : false,
    localForfeited: playerName ? forfeitedPlayers.has(playerName) : false,
    resultState: outcome.resultState,
    resultMessage: outcome.resultMessage,
    winnerPlayerNames: outcome.winnerPlayerNames,
    teamCompletedSlotIds,
    slotOwnershipTeamIndices,
    teamChestSlots: parseJsonArray(teamChestRow?.chestSlotsJson),
    mineState: {
      active: Number(teamMineRow?.active || 0) !== 0,
      sourceQuestIds: parseJsonArray(teamMineRow?.sourceQuestIdsJson),
      displayNames: parseJsonArray(teamMineRow?.displayNamesJson),
      triggeredQuestId: normalizeText(teamMineRow?.triggeredQuestId || ""),
      remainingSeconds: Number(teamMineRow?.deadlineEpochSeconds || 0) > 0
        ? Math.max(0, Number(teamMineRow?.deadlineEpochSeconds || 0) - Math.floor(Date.now() / 1000))
        : -1,
      progressQuestId: normalizeText(teamMineRow?.progressQuestId || ""),
      progress: Math.max(0, Number(teamMineRow?.progressValue || 0)),
      progressMax: Math.max(0, Number(teamMineRow?.progressMax || 0)),
      defuseQuestId: normalizeText(teamMineRow?.defuseQuestId || ""),
      defuseDisplayName: normalizeText(teamMineRow?.defuseDisplayName || "")
    },
    powerSlotState: {
      active: Number(powerStateRow?.active || 0) !== 0,
      slotId: normalizeText(powerStateRow?.slotId || ""),
      displayName: normalizeText(powerStateRow?.displayName || ""),
      remainingSeconds: Number(powerStateRow?.deadlineEpochSeconds || 0) > 0
        ? Math.max(0, Number(powerStateRow?.deadlineEpochSeconds || 0) - Math.floor(Date.now() / 1000))
        : -1,
      claimed: Number(powerStateRow?.claimed || 0) !== 0,
      resolutionNonce: Math.max(0, Number(powerStateRow?.resolutionNonce || 0)),
      buffResult: Number(powerStateRow?.buffResult || 0) !== 0,
      resolvedByPlayerName: normalizePlayerName(powerStateRow?.resolvedByPlayerName || "")
    },
    draftState: draftStateRow?.stateJson ? normalizeDraftState(parseJsonObject(draftStateRow.stateJson)) : null,
    rerollState: rerollStateRow?.stateJson ? normalizeRerollState(parseJsonObject(rerollStateRow.stateJson)) : null,
    chatMessages: (chatResult.results || [])
      .filter((row) => {
        const channel = normalizeOnlineChatChannel(row.channel);
        if (channel !== "TEAM") return true;
        return Number(row.teamIndex || 0) === viewerTeamIndex;
      })
      .map((row) => ({
        messageId: Number(row.messageId || 0),
        playerName: normalizePlayerName(row.playerName),
        channel: normalizeOnlineChatChannel(row.channel),
        teamIndex: Number(row.teamIndex || 0),
        message: normalizeChatMessage(row.message),
        createdAtEpochSeconds: Number(row.createdAtEpochSeconds || 0)
      }))
  };
}

function computeOnlineMatchOutcome(matchPayload, players, drawVoters, forfeitReasons) {
  const normalizedPlayers = Array.isArray(players) ? players : [];
  const activePlayers = normalizedPlayers.filter((player) => !player.forfeited);
  if (activePlayers.length === 0) {
    return { resultState: "draw", resultMessage: "Everyone was eliminated.", winnerPlayerNames: [] };
  }
  if (activePlayers.length > 0 && drawVoters instanceof Set && drawVoters.size >= activePlayers.length) {
    return { resultState: "draw", resultMessage: "Draw vote passed.", winnerPlayerNames: [] };
  }

  const activeTeams = new Map();
  for (const player of activePlayers) {
    const existing = activeTeams.get(player.teamIndex) || {
      players: [],
      completedLines: 0,
      completedSlotCount: 0,
      score: 0
    };
    existing.players.push(player.playerName);
    existing.completedLines = Math.max(existing.completedLines, Number(player.completedLines || 0));
    existing.completedSlotCount = Math.max(existing.completedSlotCount, Array.isArray(player.completedSlotIds) ? player.completedSlotIds.length : 0);
    existing.score = Math.max(existing.score, Number(player.score || 0));
    activeTeams.set(player.teamIndex, existing);
  }
  if (activeTeams.size === 1 && normalizedPlayers.length > 1) {
    const winner = Array.from(activeTeams.values())[0];
    const eliminatedPlayers = normalizedPlayers.filter((player) => player.forfeited);
    const firstReason = eliminatedPlayers
      .map((player) => normalizeText(forfeitReasons instanceof Map ? forfeitReasons.get(normalizePlayerName(player.playerName)) : ""))
      .find(Boolean);
    return {
      resultState: "winner",
      resultMessage: firstReason || "Won by elimination.",
      winnerPlayerNames: winner.players
    };
  }

  const win = String(matchPayload?.win || "").toUpperCase();
  const cardSize = Math.max(1, Number(matchPayload?.cardSize || 5));
  if (win === "LINE") {
    for (const team of activeTeams.values()) {
      if (team.completedLines > 0) {
        return { resultState: "winner", resultMessage: "Triggered the line win condition.", winnerPlayerNames: team.players };
      }
    }
  }
  if (win === "FULL" || win === "BLIND") {
    const targetSlots = cardSize * cardSize;
    for (const team of activeTeams.values()) {
      if (team.completedSlotCount >= targetSlots) {
        return { resultState: "winner", resultMessage: "Triggered the full-card win condition.", winnerPlayerNames: team.players };
      }
    }
  }

  return { resultState: "active", resultMessage: "", winnerPlayerNames: [] };
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
          const teamIndex = Math.floor(players.indexOf(playerName) / teamSizeForQueueMode(queueMode));
          await env.DB.prepare(`
            INSERT INTO online_match_players (
              match_id,
              player_name,
              team_index,
              ready,
              joined_at_epoch_seconds
          ) VALUES (?, ?, ?, ?, ?)
        `).bind(matchId, playerName, teamIndex, 0, nowEpochSeconds).run();
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
  const rng = createSeededRandom(matchSeed);
  const win = resolveHeadToHeadEnumPreference(rng, controllerPreferences, "win", ["FULL", "LINE", "BLIND"], "FULL");
  const cardSize = resolveHeadToHeadCardSizeValue(rng, controllerPreferences);
  const cardDifficulty = resolveHeadToHeadEnumPreference(rng, controllerPreferences, "cardDifficulty", ["easy", "normal", "hard"], "normal").toLowerCase();
  const gameDifficulty = resolveHeadToHeadEnumPreference(rng, controllerPreferences, "gameDifficulty", ["easy", "normal", "hard"], "normal").toLowerCase();
  const effectsEnabled = false;
  const rtpEnabled = false;
  const hostileMobsEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "hostileMobs", true);
  const hungerEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "hunger", true);
  const naturalRegenEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "naturalRegen", true);
  const keepInventoryEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "keepInventory", false);
  const hardcoreEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "hardcore", false);
  const teamChestEnabled = false;
  const minesEnabled = false;
  const powerSlotEnabled = false;
  const draftEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "draft", false);
  const rerollsEnabled = resolveHeadToHeadTogglePreference(rng, controllerPreferences, "rerolls", false);
  const fakeRerollsEnabled = false;
  const worldTypeMode = resolveHeadToHeadWorldType(rng, worldPreferences);
  const surfaceCaveBiomes = resolveHeadToHeadTogglePreference(rng, worldPreferences, "surfaceCaveBiomes", false);
  const prelitPortalsMode = resolveHeadToHeadPrelitPortals(rng, worldPreferences);
  const settingsLines = [
    `Mode: ${win}`,
    `Card Size: ${cardSize}x${cardSize}`,
    `Card Difficulty: ${cardDifficulty}`,
    `Game Difficulty: ${gameDifficulty}`,
    `Hostile Mobs: ${hostileMobsEnabled ? "Enabled" : "Disabled"}`,
    `Hunger: ${hungerEnabled ? "Enabled" : "Disabled"}`,
    `Natural Regen: ${naturalRegenEnabled ? "On" : "Off"}`,
    `Keep Inventory: ${keepInventoryEnabled ? "Enabled" : "Disabled"}`,
    `Hardcore: ${hardcoreEnabled ? "Enabled" : "Disabled"}`,
    `Hide Goal Details: ${win === "BLIND" ? "On" : "Off"}`,
    "Team Chest: Disabled",
    `Draft: ${draftEnabled ? "Enabled" : "Disabled"}`,
    `Rerolls: ${rerollsEnabled ? "Enabled" : "Disabled"}`,
    "PVP: Disabled",
    "Adventure: Disabled",
    "Late Join: Disabled",
    "Team Sync: Disabled",
    "Delay: 60s",
    "New Seed Every Game: Enabled",
    `World Type: ${resolveWorldTypeLabel(worldTypeMode)}`,
    `World Surface Cave Biomes: ${surfaceCaveBiomes ? "Enabled" : "Disabled"}`,
    `Prelit Portals: ${resolvePrelitLabel(prelitPortalsMode)}`
  ];
  return {
    generatedAtEpochSeconds: nowEpochSeconds,
    matchSeed,
    playerCount: Array.isArray(queueRows) ? queueRows.length : 1,
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

function chooseHeadToHeadPreferenceObject(rng, preferenceObjects) {
  const list = Array.isArray(preferenceObjects) ? preferenceObjects.filter((entry) => entry && typeof entry === "object") : [];
  if (list.length <= 0) return {};
  if (list.length === 1) return list[0];
  return (rng ? rng() : Math.random()) < 0.5 ? list[0] : list[1];
}

function randomChoice(rng, options, fallback) {
  const normalizedOptions = Array.isArray(options) ? options : [];
  if (!normalizedOptions.length) return fallback;
  const index = Math.max(0, Math.min(normalizedOptions.length - 1, Math.floor((rng ? rng() : Math.random()) * normalizedOptions.length)));
  return normalizedOptions[index];
}

function resolveHeadToHeadTogglePreference(rng, preferenceObjects, key, defaultValue) {
  const chosen = chooseHeadToHeadPreferenceObject(rng, preferenceObjects);
  const value = String(chosen?.[key] || "").trim().toUpperCase();
  if (value === "ON") return true;
  if (value === "OFF") return false;
  return randomChoice(rng, [true, false], Boolean(defaultValue));
}

function resolveHeadToHeadEnumPreference(rng, preferenceObjects, key, options, defaultValue) {
  const normalizedOptions = (Array.isArray(options) ? options : []).map((option) => String(option).toUpperCase());
  const chosen = chooseHeadToHeadPreferenceObject(rng, preferenceObjects);
  const value = String(chosen?.[key] || "").trim().toUpperCase();
  if (value !== "" && value !== "RANDOM" && value !== "RAND" && value !== "-1" && normalizedOptions.includes(value)) {
    return value;
  }
  return randomChoice(rng, normalizedOptions, String(defaultValue || normalizedOptions[0] || "").toUpperCase());
}

function resolveHeadToHeadCardSizeValue(rng, preferenceObjects) {
  const chosen = chooseHeadToHeadPreferenceObject(rng, preferenceObjects);
  const allowedSizes = [2, 3, 4, 5];
  if (chosen && !chosen.randomCardSize) {
    const size = Number(chosen.cardSize || 0);
    if (allowedSizes.includes(size)) {
      return size;
    }
  }
  return Number(randomChoice(rng, allowedSizes, 5) || 5);
}

function resolveHeadToHeadWorldType(rng, preferenceObjects) {
  return Number(resolveHeadToHeadEnumPreference(rng, preferenceObjects, "worldTypeMode", ["0", "1", "3"], "0") || 0);
}

function resolveHeadToHeadPrelitPortals(rng, preferenceObjects) {
  return Number(resolveHeadToHeadEnumPreference(rng, preferenceObjects, "prelitPortalsMode", ["0", "1", "2", "3"], "0") || 0);
}

function resolveTogglePreference(rng, preferenceObjects, key, defaultValue) {
  const choices = [];
  let onWeight = 1;
  let offWeight = 1;
  for (const preferences of preferenceObjects || []) {
    const value = String(preferences?.[key] || "").trim().toUpperCase();
    if (value === "ON") {
      onWeight += 1;
      choices.push("ON");
    }
    if (value === "OFF") {
      offWeight += 1;
      choices.push("OFF");
    }
  }
  if (choices.length > 0 && choices.every((choice) => choice === choices[0])) {
    return choices[0] === "ON";
  }
  if (onWeight <= 0 && offWeight <= 0) return defaultValue;
  return weightedBoolean(rng, onWeight, offWeight, defaultValue);
}

function resolveEnumPreference(rng, preferenceObjects, key, options, defaultValue) {
  const explicitChoices = [];
  const weights = new Map();
  for (const option of options) {
    weights.set(String(option).toUpperCase(), 1);
  }
  for (const preferences of preferenceObjects || []) {
    const value = String(preferences?.[key] || "").trim().toUpperCase();
    if (value === "" || value === "RANDOM") continue;
    if (weights.has(value)) {
      weights.set(value, weights.get(value) + 1);
      explicitChoices.push(value);
    }
  }
  if (explicitChoices.length > 0 && explicitChoices.every((choice) => choice === explicitChoices[0])) {
    return explicitChoices[0];
  }
  return weightedChoice(
    rng,
    options.map((option) => String(option).toUpperCase()),
    options.map((option) => Number(weights.get(String(option).toUpperCase()) || 0)),
    String(defaultValue || options[0] || "").toUpperCase()
  );
}

function resolveCardSize(rng, preferenceObjects) {
  const explicitChoices = [];
  const sizeWeights = new Map([[2, 1], [3, 1], [4, 1], [5, 1]]);
  for (const preferences of preferenceObjects || []) {
    if (preferences?.randomCardSize) continue;
    const size = Number(preferences?.cardSize || 0);
    if (sizeWeights.has(size)) {
      sizeWeights.set(size, sizeWeights.get(size) + 1);
      explicitChoices.push(size);
    }
  }
  if (explicitChoices.length > 0 && explicitChoices.every((choice) => choice === explicitChoices[0])) {
    const unanimous = Number(explicitChoices[0] || 5);
    return `${unanimous}x${unanimous}`;
  }
  const sizes = Array.from(sizeWeights.keys());
  const weights = sizes.map((size) => Number(sizeWeights.get(size) || 0));
  const chosen = Number(weightedChoice(rng, sizes, weights, 5) || 5);
  return `${chosen}x${chosen}`;
}

function resolveCardSizeValue(rng, preferenceObjects) {
  const sizeText = resolveCardSize(rng, preferenceObjects);
  const parsed = Number(String(sizeText).split("x")[0] || 5);
  return Number.isFinite(parsed) ? parsed : 5;
}

  function resolveWorldType(rng, preferenceObjects) {
  return Number(resolveEnumPreference(rng, preferenceObjects, "worldTypeMode", ["0", "1", "3"], "0") || 0);
  }

  function resolveWorldTypeLabel(worldTypeMode) {
    return ({
      0: "Normal",
      1: "Amplified",
      3: "Custom Biome Size"
    })[Number(worldTypeMode || 0)] || "Normal";
  }

function resolvePrelitPortals(rng, preferenceObjects) {
  return Number(resolveEnumPreference(rng, preferenceObjects, "prelitPortalsMode", ["0", "1", "2", "3"], "0") || 0);
}

function createSeededRandom(seed) {
  let state = Number(seed || 1) >>> 0;
  if (state === 0) state = 1;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

function weightedBoolean(rng, trueWeight, falseWeight, fallback) {
  const total = Math.max(0, Number(trueWeight || 0)) + Math.max(0, Number(falseWeight || 0));
  if (total <= 0) return Boolean(fallback);
  return (rng ? rng() : Math.random()) * total < Math.max(0, Number(trueWeight || 0));
}

function weightedChoice(rng, options, weights, fallback) {
  const normalizedOptions = Array.isArray(options) ? options : [];
  const normalizedWeights = Array.isArray(weights) ? weights : [];
  const total = normalizedWeights.reduce((sum, weight) => sum + Math.max(0, Number(weight || 0)), 0);
  if (!normalizedOptions.length || total <= 0) return fallback;
  let roll = (rng ? rng() : Math.random()) * total;
  for (let i = 0; i < normalizedOptions.length; i++) {
    roll -= Math.max(0, Number(normalizedWeights[i] || 0));
    if (roll < 0) {
      return normalizedOptions[i];
    }
  }
  return normalizedOptions[normalizedOptions.length - 1] || fallback;
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

function normalizeText(value) {
  const text = String(value || "").trim();
  return text.length > 200000 ? text.slice(0, 200000) : text;
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
    RANKED_1V1: "1v1",
    CASUAL_1V1: "1v1",
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

function teamSizeForQueueMode(queueMode) {
  switch (queueMode) {
    case "CASUAL_2S":
      return 2;
    case "CASUAL_3S":
      return 3;
    case "CASUAL_4S":
      return 4;
    case "CASUAL_FFA":
    case "CASUAL_1V1":
    case "RANKED_1V1":
    default:
      return 1;
  }
}

function normalizeOnlineChatChannel(value) {
  const text = normalizeText(value).toUpperCase();
  return text === "TEAM" ? "TEAM" : "GLOBAL";
}

function normalizeChatMessage(value) {
  const text = normalizeText(value);
  if (!text) return "";
  return text.length > 256 ? text.slice(0, 256) : text;
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







