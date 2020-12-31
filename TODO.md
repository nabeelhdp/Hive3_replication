1. Add pid file to ensure only one instance is running
2. In case of full dumps, ensure the existing data folders are also removed.
3. Current transaction count (target - source) is wrong because it doesn't match with actual numbers seen in the REPL DEBUG output (numEvents in REPL INFO line). Identify way to display this correctly.
