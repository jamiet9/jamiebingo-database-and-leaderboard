export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: corsHeaders()
      });
    }

    if (request.method === "GET" && url.pathname === "/submissions") {
      const { results } = await env.DB.prepare(`
        SELECT
          player_name AS playerName,
          card_seed AS cardSeed,
          world_seed AS worldSeed,
          duration_seconds AS durationSeconds,
          finished_at_epoch_seconds AS finishedAtEpochSeconds,
          completed,
          participant_count AS participantCount,
          commands_used AS commandsUsed,
          rerolls_used_count AS rerollsUsedCount,
          fake_rerolls_used_count AS fakeRerollsUsedCount,
          preview_size AS previewSize,
          team_color_id AS teamColorId,
          preview_slot_ids_json AS previewSlotIdsJson,
          completed_slot_ids_json AS completedSlotIdsJson,
          opponent_completed_slot_ids_json AS opponentCompletedSlotIdsJson,
          settings_json AS settingsJson,
          leaderboard_category AS leaderboardCategory,
          leaderboard_category_reason AS leaderboardCategoryReason
        FROM submissions
        WHERE completed = 1
          AND participant_count = 1
          AND commands_used = 0
          AND rerolls_used_count = 0
          AND fake_rerolls_used_count = 0
        ORDER BY submitted_at_epoch_seconds DESC
      `).all();

      const submissions = results.map((row) => ({
        playerName: row.playerName,
        cardSeed: row.cardSeed,
        worldSeed: row.worldSeed,
        durationSeconds: Number(row.durationSeconds || 0),
        finishedAtEpochSeconds: Number(row.finishedAtEpochSeconds || 0),
        completed: Boolean(row.completed),
        participantCount: Number(row.participantCount || 0),
        commandsUsed: Boolean(row.commandsUsed),
        rerollsUsedCount: Number(row.rerollsUsedCount || 0),
        fakeRerollsUsedCount: Number(row.fakeRerollsUsedCount || 0),
        previewSize: Number(row.previewSize || 0),
        teamColorId: Number(row.teamColorId || 0),
        previewSlotIds: parseJsonArray(row.previewSlotIdsJson),
        completedSlotIds: parseJsonArray(row.completedSlotIdsJson),
        opponentCompletedSlotIds: parseJsonArray(row.opponentCompletedSlotIdsJson),
        settingsLines: parseJsonArray(row.settingsJson),
        leaderboardCategory: row.leaderboardCategory || "Custom",
        leaderboardCategoryReason: row.leaderboardCategoryReason || ""
      }));

      return Response.json({ submissions }, {
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
          duration_seconds,
          finished_at_epoch_seconds,
          completed,
          participant_count,
          commands_used,
          rerolls_used_count,
          fake_rerolls_used_count,
          preview_size,
          team_color_id,
          preview_slot_ids_json,
          completed_slot_ids_json,
          opponent_completed_slot_ids_json,
          settings_json,
          leaderboard_category,
          leaderboard_category_reason,
          submitted_at_epoch_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
        body.playerName || "Unknown",
        body.cardSeed || "",
        body.worldSeed || "",
        Number(body.durationSeconds || 0),
        Number(body.finishedAtEpochSeconds || 0),
        body.completed ? 1 : 0,
        Number(body.participantCount || 0),
        body.commandsUsed ? 1 : 0,
        Number(body.rerollsUsedCount || 0),
        Number(body.fakeRerollsUsedCount || 0),
        Number(body.previewSize || 0),
        Number(body.teamColorId || 0),
        JSON.stringify(asArray(body.previewSlotIds)),
        JSON.stringify(asArray(body.completedSlotIds)),
        JSON.stringify(asArray(body.opponentCompletedSlotIds)),
        JSON.stringify(asArray(body.settingsLines)),
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
