#!/usr/bin/env bash
# Fetch a project-board item's id + Status field for a given PR/Issue.
#
# Two call sites in the workflows ran near-identical GraphQL queries
# with copy-pasted error handling: project-orchestrator.yml and the
# board-upsert-status composite. This script is the single source of
# truth for that lookup. (A third caller, board-state-sync.yml, used
# the `id` mode but was removed when projects_v2_item delivery turned
# out to be broken from GitHub's side; the mode is kept here in case
# the workflow ever comes back.)
#
# Usage:
#   board-fetch-item.sh url <pr-or-issue-url> <project-number>
#   board-fetch-item.sh id  <node-id>          <project-number>
#
# Output (tab-separated, single line on stdout):
#   <item-id>\t<status-option-id>\t<status-option-name>
#
# All three fields may be empty when the resource is on the project's
# graph but doesn't have a project item for the requested project, OR
# when its Status field isn't set. Callers distinguish "not on board"
# from "on board, status unset" by checking item-id.
#
# Exit codes:
#   0  query succeeded (output may have empty fields — see above)
#   1  GraphQL response contained .errors
#   2  GraphQL returned a null resource/node (deleted, bad URL, missing scope)
#   64 wrong CLI usage
#
# Requires: gh (with GITHUB_TOKEN or PROJECT_BOARD_TOKEN exported as
# GH_TOKEN), jq.

set -euo pipefail

if [ "$#" -ne 3 ]; then
	echo "usage: $0 (url|id) <target> <project-number>" >&2
	exit 64
fi

mode=$1
target=$2
project_num=$3

# Single shared selection — used by both the URL and ID queries.
# Inlined into the heredocs below because GraphQL doesn't support
# fragments via gh-api's interpolation in the way we'd want.
case "$mode" in
url)
	# `resource(url:)` is a typed reference, returning PullRequest |
	# Issue | other. We query both fragments; only one matches.
	# shellcheck disable=SC2016  # $url is a GraphQL variable, not shell
	resp=$(gh api graphql -f query='query($url: URI!) {
	    resource(url: $url) {
	      ... on PullRequest {
	        projectItems(first: 20) {
	          nodes {
	            id
	            project { number }
	            fieldValueByName(name: "Status") {
	              ... on ProjectV2ItemFieldSingleSelectValue { optionId name }
	            }
	          }
	        }
	      }
	      ... on Issue {
	        projectItems(first: 20) {
	          nodes {
	            id
	            project { number }
	            fieldValueByName(name: "Status") {
	              ... on ProjectV2ItemFieldSingleSelectValue { optionId name }
	            }
	          }
	        }
	      }
	    }
	  }' -F url="$target")
	resource=$(echo "$resp" | jq -c '.data.resource')
	root_label='resource'
	;;
id)
	# `node(id:)` — same selection, looked up by GraphQL node ID
	# instead of URL. Currently no live caller (was used by
	# board-state-sync.yml which is removed); kept for symmetry and
	# in case board-state-sync is ever resurrected.
	# shellcheck disable=SC2016  # $id is a GraphQL variable, not shell
	resp=$(gh api graphql -f query='query($id: ID!) {
	    node(id: $id) {
	      ... on PullRequest {
	        projectItems(first: 20) {
	          nodes {
	            id
	            project { number }
	            fieldValueByName(name: "Status") {
	              ... on ProjectV2ItemFieldSingleSelectValue { optionId name }
	            }
	          }
	        }
	      }
	      ... on Issue {
	        projectItems(first: 20) {
	          nodes {
	            id
	            project { number }
	            fieldValueByName(name: "Status") {
	              ... on ProjectV2ItemFieldSingleSelectValue { optionId name }
	            }
	          }
	        }
	      }
	    }
	  }' -F id="$target")
	resource=$(echo "$resp" | jq -c '.data.node')
	root_label='node'
	;;
*)
	echo "usage: $0 (url|id) <target> <project-number>" >&2
	exit 64
	;;
esac

# Surface GraphQL-level errors loudly. A successful HTTP response with
# .errors set indicates a token-scope or schema problem; without this
# check, jq below would silently produce empty output and downstream
# logic would misread it as "item not on board" (Copilot R9 flagged
# the silent-skip behaviour at the original call sites).
graphql_errors=$(echo "$resp" | jq -c '.errors // empty')
if [ -n "$graphql_errors" ]; then
	echo "GraphQL error: $graphql_errors" >&2
	exit 1
fi
if [ "$resource" = "null" ]; then
	echo "GraphQL returned null $root_label for $target — check URL/ID validity and token permissions." >&2
	exit 2
fi

# Filter projectItems to the requested project number, take the first
# matching item, emit (id, optionId, name) as one tab-separated line.
# Empty fields are emitted as empty strings, matching what every
# caller's existing `// ""` defaults produced.
echo "$resource" | jq -r --arg n "$project_num" '
	[.projectItems.nodes[]? | select(.project.number == ($n | tonumber))] | .[0]
	| [
	    (.id // ""),
	    (.fieldValueByName.optionId // ""),
	    (.fieldValueByName.name // "")
	  ] | @tsv
'
