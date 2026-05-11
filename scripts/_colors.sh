#!/usr/bin/env bash
# shellcheck disable=SC2034  # vars are read by the sourcing script
# Color setup. Sourced by scripts/*.sh — not directly executable.
#
# Honors the NO_COLOR convention (https://no-color.org/). When NO_COLOR is
# set to anything non-empty, all variables resolve to empty strings so
# `printf "$RED..."` becomes `printf "..."` with no escapes.

if [ -n "${NO_COLOR:-}" ]; then
	CYAN=
	GREEN=
	YELLOW=
	RED=
	BOLD=
	RESET=
else
	# Bake the literal ESC byte (0x1B) into each variable. Same approach as
	# the Makefile so output looks identical regardless of which path renders.
	CYAN=$'\033[36m'
	GREEN=$'\033[32m'
	YELLOW=$'\033[33m'
	RED=$'\033[31m'
	BOLD=$'\033[1m'
	RESET=$'\033[0m'
fi
