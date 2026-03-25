ALTER TABLE submissions ADD COLUMN preview_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN completed_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN opponent_completed_slot_ids_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE submissions ADD COLUMN leaderboard_category TEXT NOT NULL DEFAULT 'Custom';
ALTER TABLE submissions ADD COLUMN leaderboard_category_reason TEXT NOT NULL DEFAULT '';
