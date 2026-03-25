ALTER TABLE submissions ADD COLUMN preview_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN completed_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN opponent_completed_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN leaderboard_category TEXT NOT NULL DEFAULT 'Custom';
ALTER TABLE submissions ADD COLUMN leaderboard_category_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE submissions ADD COLUMN vote_reroll_used INTEGER NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN preview_slots_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN settings_seed TEXT NOT NULL DEFAULT '';
ALTER TABLE submissions ADD COLUMN weekly_challenge INTEGER NOT NULL DEFAULT 0;
ALTER TABLE submissions ADD COLUMN weekly_challenge_id TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS weekly_challenge_state (
  challenge_id TEXT PRIMARY KEY,
  base_seed INTEGER NOT NULL,
  next_reset_epoch_seconds INTEGER NOT NULL,
  settings_seed TEXT NOT NULL DEFAULT '',
  world_seed TEXT NOT NULL DEFAULT '',
  card_seed TEXT NOT NULL DEFAULT '',
  preview_size INTEGER NOT NULL DEFAULT 0,
  preview_slots_json TEXT NOT NULL DEFAULT '[]',
  settings_json TEXT NOT NULL DEFAULT '[]',
  updated_at_epoch_seconds INTEGER NOT NULL DEFAULT 0
);
