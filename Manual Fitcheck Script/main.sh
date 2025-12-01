#!/bin/bash

# Load config & checks
source ./config
source ./checks.sh

# === Prompt user for overrides ===
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

echo "[INFO] Fetching available clusters from Prism Central ($PC_IP)..."

RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/")
if [[ -z "$RESPONSE" ]]; then
    echo "[ERROR] No response from Prism Central. Exiting."
    exit 1
fi

# === Parse clusters ===
CLUSTER_NAMES=()
EXT_IDS=() 

while IFS= read -r row; do
    name=$(echo "$row" | jq -r '.name // empty')
    extid=$(echo "$row" | jq -r '.extId // empty')

    # Skip unwanted entries
    if [[ -n "$name" && -n "$extid" && "$name" != "vx" ]]; then
        [[ "$name" == "Unnamed" ]] && name="Prism Central"
        CLUSTER_NAMES+=("$name")
        EXT_IDS+=("$extid")
    fi
done < <(echo "$RESPONSE" | jq -c '.data[]')

# Display clusters
echo "[INFO] Found ${#CLUSTER_NAMES[@]} clusters:"
for i in "${!CLUSTER_NAMES[@]}"; do
    echo "  - ${CLUSTER_NAMES[$i]} (${EXT_IDS[$i]})"
done

# === Run checks per cluster ===
for i in "${!CLUSTER_NAMES[@]}"; do
    cluster="${CLUSTER_NAMES[$i]}"
    extid="${EXT_IDS[$i]}"
    csv_file="$OUTDIR/${cluster}.csv"

    echo
    echo "===== Running checks for cluster: $cluster ====="
    echo "CHECK,RESULT" > "$csv_file"
    echo "Cluster_Name,$cluster" >> "$csv_file"

    append_cluster_details "$cluster" "$extid"
    ext_ip=$(grep '^Cluster IP,' "$csv_file" | cut -d',' -f2)

    get_cluster_security_config "$cluster" "$ext_ip"
    get_smtp_status "$cluster" "$ext_ip"
    snmp_status_check "$cluster" "$extid"
    get_directory_services_pc "$cluster"
    get_directory_services_pe_v2 "$cluster" "$ext_ip"
    get_snapshots_info "$cluster" "$ext_ip"
    get_ha_state "$cluster" "$ext_ip"
    lcm_version_check "$cluster" "$extid"
    vs_mtu_check "$cluster" "$ext_ip"
    get_hosts_and_nics "$cluster" "$extid"
    fetch_cluster_stats "$cluster" "$extid"
    fetch_storage_containers "$cluster" "$extid"
    get_all_storage_containers_io_latency "$cluster" "$extid"
    get_storage_overprovisioning_ratio "$cluster" "$ext_ip"
    get_host_count "$cluster" "$extid"
    get_offline_disks "$cluster"
    get_license_details "$cluster" "$extid"
    get_cvm_resources "$cluster" "$extid"
    get_cpu_ratio "$cluster" "$extid"
    get_alert_parameters "$cluster" "$extid"

    echo "[INFO] Workflow completed for $cluster"
    echo "[INFO] CSV available at $csv_file"
done
