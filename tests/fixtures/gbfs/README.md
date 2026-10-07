# GBFS development fixture

Offline fallback used when the live feed is unreachable (`GBFS_FIXTURE_DIR=tests/fixtures/gbfs`).

- Origin: trimmed copy (12 stations) of the Naolib / Nantes Métropole feed
  (`https://api.cyclocity.fr/contracts/nantes/gbfs/gbfs.json`, GBFS 2.3), captured on 2026-10-06.
- Modification: station `16` was edited by hand to be out of service
  (`is_renting=false`, `is_returning=false`, `0` bikes) so that non-operational stations are covered.
  Station `11` is genuinely empty in the capture.
- It is **not** a live feed: timestamps are frozen, so repeated polls yield identical measures.
