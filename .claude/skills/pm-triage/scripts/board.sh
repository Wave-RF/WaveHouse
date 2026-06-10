#!/usr/bin/env bash
# WaveHouse Task Board (GitHub Projects v2, project #7) helper for /pm-triage.
#
# Field/option IDs are verified (see memory reference_wavehouse_board.md). They are
# stable once created; if the board changes, re-discover and override via env:
#   gh project field-list 7 --owner Wave-RF --format json
#
# Subcommands:
#   file <P0|P1|P2|P3> <Backlog|Ready|InProgress|InReview|Done> "<labels>" "<title>"   (body on stdin) -> echoes issue #
#   set-priority <issue#> <P0|P1|P2|P3>
#   set-status   <issue#> <Backlog|Ready|InProgress|InReview|Done>
#   item-id      <issue#>
#   open-by-priority         # open issues grouped by board Priority
#
# Race note: a repo workflow auto-adds new issues to the board, so `file` creates the
# issue, polls the board for the item id, and only then sets fields (a direct
# `gh project item-add` right after create fails with "Content already exists").
set -uo pipefail

REPO="${WH_REPO:-Wave-RF/WaveHouse}"
OWNER="${WH_OWNER:-Wave-RF}"
PNUM="${WH_PROJECT_NUM:-7}"
PROJ="${WH_PROJECT_ID:-PVT_kwDOCdKSOc4BUEKD}"
PRIO_FIELD="${WH_PRIORITY_FIELD:-PVTSSF_lADOCdKSOc4BUEKDzhBO0vI}"
STATUS_FIELD="${WH_STATUS_FIELD:-PVTSSF_lADOCdKSOc4BUEKDzhBO0l8}"
# Priority/Status option-id maps as `case` lookups, not `declare -A`: associative
# arrays need bash ≥4, but the routine invokes plain `bash` (= /bin/bash 3.2 on
# macOS), which would die with "P0: unbound variable". `case` works under 3.2.
# A bad key returns non-zero so callers can reject it.
_prio_id() {
  case "$1" in
    P0) echo 79628723 ;; P1) echo 0a877460 ;;
    P2) echo da944a9c ;; P3) echo e141a9e0 ;;
    *) return 1 ;;
  esac
}
_status_id() {
  case "$1" in
    Backlog) echo f75ad846 ;; Ready) echo 61e4505c ;;
    InProgress) echo 47fc9ee4 ;; InReview) echo df73e18b ;;
    Done) echo 98236657 ;; *) return 1 ;;
  esac
}

_item_id() {
  gh project item-list "$PNUM" --owner "$OWNER" --format json --limit 400 2>/dev/null \
    | jq -r --arg n "$1" '.items[]|select(.content.number==($n|tonumber))|.id'
}
_set() { gh project item-edit --id "$1" --field-id "$2" --single-select-option-id "$3" --project-id "$PROJ" >/dev/null; }

cmd="${1:-}"; shift || true
case "$cmd" in
  file)
    prio="$1"; status="$2"; labels="$3"; title="$4"; body="$(cat)"
    prio_id=$(_prio_id "$prio")     || { echo "bad priority: $prio"  >&2; exit 2; }
    status_id=$(_status_id "$status") || { echo "bad status: $status" >&2; exit 2; }
    url=$(gh issue create --repo "$REPO" --title "$title" --label "$labels" --body "$body") || { echo "create failed" >&2; exit 1; }
    num=$(echo "$url" | grep -oE '[0-9]+$')
    item=""; for _ in 1 2 3 4 5 6; do sleep 3; item=$(_item_id "$num"); [[ -n "$item" ]] && break; done
    [[ -z "$item" ]] && item=$(gh project item-add "$PNUM" --owner "$OWNER" --url "$url" --format json | jq -r '.id')
    _set "$item" "$PRIO_FIELD" "$prio_id"
    _set "$item" "$STATUS_FIELD" "$status_id"
    echo "$num"
    ;;
  set-priority) pid=$(_prio_id "$2")   || { echo "bad priority: $2" >&2; exit 2; }; item=$(_item_id "$1"); [[ -z "$item" ]] && { echo "#$1 not on board" >&2; exit 1; }; _set "$item" "$PRIO_FIELD" "$pid"; echo "#$1 -> $2" ;;
  set-status)   sid=$(_status_id "$2") || { echo "bad status: $2"   >&2; exit 2; }; item=$(_item_id "$1"); [[ -z "$item" ]] && { echo "#$1 not on board" >&2; exit 1; }; _set "$item" "$STATUS_FIELD" "$sid"; echo "#$1 -> $2" ;;
  item-id)      _item_id "$1" ;;
  open-by-priority)
    tmp=$(mktemp)
    gh issue list --repo "$REPO" --state open --limit 300 --json number --jq '.[].number' | sort -n > "$tmp"
    gh project item-list "$PNUM" --owner "$OWNER" --format json --limit 400 \
      | jq -r '.items[]|select(.content.type=="Issue")|"\(.priority // "—")\t\(.content.number)\t\(.content.title)"' \
      | while IFS=$'\t' read -r p n t; do grep -qx "$n" "$tmp" && printf '%s\t#%s\t%s\n' "$p" "$n" "${t:0:70}"; done \
      | sort
    rm -f "$tmp"
    ;;
  *) echo "usage: board.sh {file|set-priority|set-status|item-id|open-by-priority} ..." >&2; exit 2 ;;
esac
