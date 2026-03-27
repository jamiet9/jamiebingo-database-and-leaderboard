const LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS = 1774396800;
const WEEKLY_RESET_ANCHOR_EPOCH_SECONDS = 1774537841;
const WEEKLY_RESET_PERIOD_SECONDS = 7 * 24 * 60 * 60;

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







