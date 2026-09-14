#!/usr/bin/env bash
# Permission boundary for worktree PostgreSQL issuance and provisioner material.
wrt_set_postgres_pki_permissions() {
  local root="$1"
  chmod 0755 "$root"
  chmod 0700 "$root/root" "$root/server" "$root/node-a"
  chmod 0600 "$root/root/ca.key" "$root/server/key.pem" "$root/node-a/key.pem"
  chmod 0644 "$root/node-a-public.pem"
}
