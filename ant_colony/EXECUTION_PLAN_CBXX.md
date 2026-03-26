# CB-XX - ANT COLONY EXECUTION PLAN

## Current principle
Slow is smooth. Smooth is fast.

We do not inject unvalidated external strategy claims directly into live workers.
We first extract architecture, then research modules, then controlled integration.

---

## What has now been imported from the new source zips

### Structural imports
- queen / worker / risk / adapter separation
- asset-class specific risk logic
- separate research layer for new edges
- separate backtest harness

### Colony-safe implementation
Added modules:
- ant_colony/core/asset_profiles.py
- ant_colony/core/colony_risk_engine.py
- ant_colony/research/enhanced_edges_catalog.py
- ant_colony/research/backtest_engine.py

These are NON-LIVE modules.
No running worker logic is changed yet.

---

## Updated roadmap

### Phase 1 - Stabilize current crypto colony
1. Verify supervisor restart path
2. Finish alloc_targets -> worker wrapper v1 proof
3. Snapshot source + ANT_OUT

### Phase 2 - Colony risk layer
1. Add colony_risk_engine output file
2. Let queen combine:
   - cb20/cb21 allocation
   - colony risk overlays
3. Write new output:
   - colony_risk_targets.json

### Phase 3 - Research lane
1. Run enhanced edges only in research mode
2. Backtest on our own BTC/ETH/SOL/XRP/ADA/BNB data
3. Score by:
   - trade count
   - profit factor
   - drawdown
   - stability by year/regime
4. Reject anything unstable

### Phase 4 - Controlled live integration
1. Add one new edge at a time
2. First as shadow signal only
3. Then as capped size overlay
4. Then as standalone worker only if proven

### Phase 5 - Multi-asset colony
1. Add ETF asset class
2. Add commodity asset class
3. Keep one queen schema
4. Use asset_profiles per asset class

---

## Hard rules
- No direct live deployment from uploaded third-party zip logic
- No trusting claimed returns without our own validation
- No mixing research and live execution paths
- Every important step gets a snapshot zip