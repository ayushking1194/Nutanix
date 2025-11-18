#!/bin/bash

# Load config and checks
source ./config
source ./checks.sh

# === Prompt user for config overrides ===
read -p "Enter Prism Central IP [$PC_IP]: " input_ip
read -p "Enter Username [$USERNAME]: " input_user
read -s -p "Enter Password [$PASSWORD]: " input_pass
echo
read -p "Enter Output Directory [$OUTDIR]: " input_out

# Apply inputs if provided
PC_IP="${input_ip:-$PC_IP}"
USERNAME="${input_user:-$USERNAME}"
PASSWORD="${input_pass:-$PASSWORD}"
OUTDIR="${input_out:-$OUTDIR}"

echo "[INFO] Fetching available clusters from Prism Central ($PC_IP)..."

RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/")
if [[ -z "$RESPONSE" ]]; then
    echo "[ERROR] No response from Prism Central. Exiting."
    exit 1
fi

compact_json=$(echo "$RESPONSE" | tr -d '\n\r')

# Initialize arrays
CLUSTER_NAMES=()
EXT_IDS=()

# Parse clusters using AWK (your logic)
while IFS=, read -r name extid; do
    # Append only valid entries
    if [[ -n "$name" && -n "$extid" ]]; then
        CLUSTER_NAMES+=("$name")
        EXT_IDS+=("$extid")
    fi
done < <(
    echo "$compact_json" | awk '
    BEGIN {
        RS="\\{"
        FS="\n"
    }
    {
        if ($0 ~ /"extId":/ && $0 ~ /"name":/) {
            match($0, /"extId":"[0-9a-f-]+"/)
            extid = substr($0, RSTART+8, RLENGTH-9)
            gsub(/"/, "", extid)

            match($0, /"name":"[^"]+"/)
            name = substr($0, RSTART+8, RLENGTH-9)
            gsub(/"/, "", name)

            if (name != "" && extid != "" && name != "vx" && name != "hasError" && name != "isPaginated" && name != "isTruncated")
                print name "," extid
        }
    }
    '
)

# Display fetched clusters
echo "[INFO] Found ${#CLUSTER_NAMES[@]} clusters:"
for ((i=0; i<${#CLUSTER_NAMES[@]}; i++)); do
    # Normalize "Unnamed" cluster names
    if [[ "${CLUSTER_NAMES[$i]}" == "Unnamed" ]]; then
        CLUSTER_NAMES[$i]="Prism Central"
    fi
    echo "  - ${CLUSTER_NAMES[$i]} (${EXT_IDS[$i]})"
done

# === Run checks per cluster ===
for ((i=0; i<${#CLUSTER_NAMES[@]}; i++)); do
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
    get_host_count "$cluster" "$extid"
    get_offline_disks "$cluster"
    get_license_details "$cluster" "$extid"
    get_cvm_resources "$cluster" "$extid"
    get_cpu_ratio "$cluster" "$extid"
    get_alerts "$cluster" "$extid"

    echo "[INFO] Workflow completed for $cluster"
    echo "[INFO] CSV available at $csv_file"
done