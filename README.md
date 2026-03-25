# JamieBingo Database

This repo is prepared to work as a GitHub Pages leaderboard site for JamieBingo.

## What is here

- `index.html`
- `styles.css`
- `app.js`
- `data/submissions.json`

If you publish this repo with GitHub Pages from the `main` branch, the page can display the community leaderboard.

## Important limitation

GitHub Pages is static only. It cannot receive uploads from the mod.

That means:

- the mod's `leaderboardUrl` can point to this GitHub Pages site
- the mod's `submitUrl` must point to a real backend endpoint

## Expected submission JSON

The leaderboard page reads either:

- a raw array of submissions
- or `{ "submissions": [...] }`

Each submission should contain:

```json
{
  "playerName": "Runner",
  "cardSeed": "seed",
  "worldSeed": "seed",
  "durationSeconds": 1234,
  "finishedAtEpochSeconds": 1774406400,
  "completed": true,
  "participantCount": 1,
  "commandsUsed": false,
  "rerollsUsedCount": 0,
  "fakeRerollsUsedCount": 0,
  "previewSize": 5
}
```

## Next steps

1. Add a GitHub remote for this repo.
2. Push `main`.
3. Enable GitHub Pages for the repo root.
4. Point the mod's `leaderboardUrl` to the published site.
5. Add a real backend and point the mod's `submitUrl` to it.
