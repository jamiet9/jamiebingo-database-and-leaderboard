const LEADERBOARD_MIN_FINISHED_AT_EPOCH_SECONDS = 1774396800;
const WEEKLY_RESET_ANCHOR_EPOCH_SECONDS = 1774479299;
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

      const submissions = results.map((row) => ({
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
      }));

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
      return Response.json({
        baseSeed,
        challengeId: `weekly-${baseSeed}`,
        nextResetEpochSeconds: baseSeed + WEEKLY_RESET_PERIOD_SECONDS
      }, {
        headers: corsHeaders()
      });
    }

    if (request.method === "POST" && url.pathname === "/submit") {
      const body = await request.json();

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
        body.weeklyChallenge ? 1 : 0,
        body.weeklyChallengeId || "",
        body.leaderboardCategory || "Custom",
        body.leaderboardCategoryReason || "",
        Number(body.submittedAtEpochSeconds || Math.floor(Date.now() / 1000))
      ).run();

      return Response.json({ ok: true }, {
        headers: corsHeaders()
      });
    }

    return new Response("Not found", { status: 404 });
  }
};

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Key"
  };
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
