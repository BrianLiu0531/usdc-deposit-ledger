#!/usr/bin/env bash
# Run Terraform in a Linux container with short-lived credentials from your current AWS CLI login.
#
# Useful when Terraform can't run on the host, e.g. antivirus HTTPS scanning (Avast, Kaspersky...)
# breaks the local gRPC connection between Terraform and its provider plugins on Windows.
#
# usage:  scripts/terraform-in-docker.sh <stack dir> <terraform args...>
# e.g.:   scripts/terraform-in-docker.sh terraform/app plan
#
# Env vars:
#   AWS_PROFILE   CLI profile to export credentials from (default: default)
#   AWS_REGION    region (default: ap-southeast-2)
#   CA_BUNDLE     PEM bundle to trust inside the container, e.g. one that includes the antivirus
#                 root certificate (default: none, the image's own CA store)
set -euo pipefail

STACK="$(cd "$1" && pwd)"; shift
# Mount the whole repository so relative paths such as ../../dist/consumer.zip resolve
ROOT="$(git -C "$STACK" rev-parse --show-toplevel 2>/dev/null || echo "$STACK")"
ROOT="$(cd "$ROOT" && pwd)"
REL="${STACK#"$ROOT"}"
REGION="${AWS_REGION:-ap-southeast-2}"
AWS_CLI="${AWS_CLI:-aws}"
command -v "$AWS_CLI" >/dev/null 2>&1 || AWS_CLI="/c/Program Files/Amazon/AWSCLIV2/aws.exe"

# Windows Docker needs Windows paths; on Linux/macOS they are used as is
winpath() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }

ENVFILE="$(mktemp)"
trap 'rm -f "$ENVFILE"' EXIT
"$AWS_CLI" configure export-credentials --region "$REGION" ${AWS_PROFILE:+--profile "$AWS_PROFILE"} --format env-no-export | tr -d '\r' > "$ENVFILE"
{ echo "AWS_REGION=$REGION"; echo "TF_IN_AUTOMATION=1"; } >> "$ENVFILE"

MOUNTS=(-v "$(winpath "$ROOT"):/repo")
if [ -n "${CA_BUNDLE:-}" ]; then
  MOUNTS+=(-v "$(winpath "$(cd "$(dirname "$CA_BUNDLE")" && pwd)/$(basename "$CA_BUNDLE")"):/certs/ca-bundle.pem:ro")
  echo "SSL_CERT_FILE=/certs/ca-bundle.pem" >> "$ENVFILE"
fi

MSYS_NO_PATHCONV=1 docker run --rm -i --env-file "$(winpath "$ENVFILE")" "${MOUNTS[@]}" -w "/repo$REL" \
  hashicorp/terraform:1.16 "$@"
