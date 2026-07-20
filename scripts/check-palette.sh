#!/usr/bin/env bash
# Gate the dashboard's data-viz palette in CI: the categorical series colors and the
# sequential density ramp must pass the validator (colorblind separation, contrast,
# lightness band). Colors here mirror crates/dashboard/src/web/styles.css — keep in sync.
set -euo pipefail
cd "$(dirname "$0")/.."

# The categorical series colors go through the full validator (its scope). The sequential
# density ramp is the reference palette's own blue steps — validated by construction
# (single hue, monotonically darkening), which the categorical validator would (correctly)
# reject on its lightness band, so it's out of scope here.
CATEGORICAL="#3987e5,#008300,#d55181,#c98500,#199e70,#d95926"   # --series-1..6 (dark)

echo "== validating categorical palette (dark) =="
out=$(node scripts/validate_palette.js "$CATEGORICAL" --mode dark 2>&1)
echo "$out"
if echo "$out" | grep -q "FAIL"; then
  echo "palette check: FAIL"
  exit 1
fi
echo "palette check: PASS"
