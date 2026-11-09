# Engine guard.
# A job must fail at once, not cell by cell, when the engine it is meant to time is not the one it
# would load: the built extension $REPO/gnomon/_engine*.so must exist and, when EXTCMP_ENGINE_SO_SHA256
# is set, its sha256 must equal it. Checked once per process tree
# (EXTCMP_ENGINE_SO_VERIFIED caches path:size:mtime for child scripts).
engine_guard() {
  local so; so=$(ls "$REPO"/gnomon/_engine*.so 2>/dev/null | head -n 1)
  if [ -z "$so" ]; then
    echo "ENGINE GUARD: no built engine in $REPO/gnomon (_engine*.so missing): refusing to run" >&2; return 1
  fi
  local id="$so:$(stat -c %s:%Y "$so")"
  [ "${EXTCMP_ENGINE_SO_VERIFIED:-}" = "$id" ] && return 0
  if [ -n "${EXTCMP_ENGINE_SO_SHA256:-}" ]; then
    local got; got=$(sha256sum "$so" | cut -d' ' -f1)
    if [ "$got" != "$EXTCMP_ENGINE_SO_SHA256" ]; then
      echo "ENGINE GUARD: $so sha256 $got is not the pinned $EXTCMP_ENGINE_SO_SHA256: refusing to run" >&2; return 1
    fi
  fi
  export EXTCMP_ENGINE_SO_VERIFIED="$id"
}
if [ "${EXTCMP_ENGINE_GUARD:-1}" = 1 ]; then engine_guard || exit 3; fi
