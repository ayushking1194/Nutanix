#!/bin/bash
set -u pipefail

# =============================
# Load Configuration & Checks
# =============================
source ./config
source ./checks.sh

# =============================
# User Overrides
# =============================
read -p "Enter Prism Central IP [$PC_IP]: " input_ip
read -p "Enter Username [$USERNAME]: " input_user
read -s -p "Enter Password [$PASSWORD]: " input_pass
echo
read -p "Enter Output Directory [$OUTDIR]: " input_out

PC_IP="${input_ip:-$PC_IP}"
USERNAME="${input_user:-$USERNAME}"
PASSWORD="${input_pass:-$PASSWORD}"
OUTDIR="${input_out:-$OUTDIR}"
mkdir -p "$OUTDIR"

# =============================
# Fetch Cluster Inventory
# =============================
echo "[INFO] Fetching clusters from Prism Central ($PC_IP)..."

RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
    "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/")

if [[ -z "$RESPONSE" ]]; then
    echo "[ERROR] No response from Prism Central. Exiting."
    exit 1
fi

CLUSTER_NAMES=()
EXT_IDS=()

while IFS= read -r row; do
    name=$(echo "$row" | jq -r '.name // empty')
    extid=$(echo "$row" | jq -r '.extId // empty')

    if [[ -n "$name" && -n "$extid" && "$name" != "vx" ]]; then
        [[ "$name" == "Unnamed" ]] && name="Prism Central"
        CLUSTER_NAMES+=("$name")
        EXT_IDS+=("$extid")
    fi
done < <(echo "$RESPONSE" | jq -c '.data[]')

echo "[INFO] Found ${#CLUSTER_NAMES[@]} clusters"

# =============================
# Run Generic Check Workflow
# =============================
for i in "${!CLUSTER_NAMES[@]}"; do
    cluster="${CLUSTER_NAMES[$i]}"
    extid="${EXT_IDS[$i]}"
    csv_file="$OUTDIR/${cluster}.csv"

    echo
    echo "====== Running Fitcheck for $cluster ======"

    init_csv "$cluster"

    append_cluster_details "$cluster" "$extid"

    ext_ip=$(get_cluster_ip "$cluster")

    run_all_checks "$cluster" "$extid" "$ext_ip"

    echo "[INFO] Completed $cluster"
    echo "[INFO] CSV: $csv_file"
done
