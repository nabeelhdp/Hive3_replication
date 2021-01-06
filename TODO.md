1. Add pid file to ensure only one instance is running - DONE
2. In case of failed full dump runs, clean target before restarting.
3. Current transaction count (target - source) is wrong because it doesn't match with actual numbers seen in the REPL DEBUG output (numEvents in REPL INFO line). Identify way to display this correctly - DONE
