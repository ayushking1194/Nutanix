# =====================================================
# HOW TO ADD A NEW CHECK
# =====================================================
# 1. Define function using one of these signatures:
#    my_check(cluster, extid)
#    my_check(cluster, ext_ip)
#
# 2. Append CSV using:
#    echo "Key,Value" >> "$OUTDIR/${cluster}.csv"
#
# 3. Register function at bottom:
#    REGISTERED_CHECKS+=(my_check)
#
# 4. DO NOT edit main.sh
# =====================================================

# Generic CSV write function
init_csv() {
    local cluster="$1"
    local csv_file="$OUTDIR/${cluster}.csv"
    echo "CHECK,RESULT" > "$csv_file"
    echo "Cluster_Name,$cluster" >> "$csv_file"
}

write_csv() {
    local cluster="$1"
    local key="$2"
    local value="$3"

    key="${key//,/;}"
    value="${value//,/;}"
    echo "${key},${value}" >> "$OUTDIR/${cluster}.csv"
}

get_cluster_ip() {
    local cluster="$1"
    grep '^Cluster IP,' "$OUTDIR/${cluster}.csv" | cut -d',' -f2
}

# v4.0
append_cluster_details() {
    local cluster="$1"
    local extid="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    # Fetch the cluster JSON
    local JSON
    JSON=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$extid")

    # --- Extract External Address ---
    EXT_ADDRESS=$(echo "$JSON" | jq -r '.data.network.externalAddress.ipv4.value // empty')
    [[ -z "$EXT_ADDRESS" ]] && EXT_ADDRESS="*N/A*"

    # --- Extract External Data Services IP ---
    EDS_ADDRESS=$(echo "$JSON" | jq -r '.data.network.externalDataServiceIp.ipv4.value // empty')
    [[ -z "$EDS_ADDRESS" ]] && EDS_ADDRESS="*N/A*"

    # --- Extract Name Servers ---
    NAME_SERVERS=$(echo "$JSON" | jq -r '
        [.data.network.nameServerIpList[].ipv4.value] | join(",")
    ' 2>/dev/null)
    [[ -z "$NAME_SERVERS" ]] && NAME_SERVERS="*N/A*"

    # --- Extract NTP Servers ---
    NTP_SERVERS=$(echo "$JSON" | jq -r '
        [.data.network.ntpServerIpList[].ipv4.value] | join(",")
    ' 2>/dev/null)
    [[ -z "$NTP_SERVERS" ]] && NTP_SERVERS="*N/A*"

    # --- Extract AOS Version ---
    AOS_VERSION=$(echo "$JSON" | jq -r '.data.config.buildInfo.version // empty')
    [[ -z "$AOS_VERSION" ]] && AOS_VERSION="N/A"

    # --- Extract Redundancy Factor ---
    RF=$(echo "$JSON" | jq -r '.data.config.redundancyFactor // empty')
    [[ -z "$RF" ]] && RF="N/A"

    # --- Extract Pulse Status ---
    PULSE=$(echo "$JSON" | jq -r '.data.config.pulseStatus.isEnabled // empty')
    if [[ -z "$PULSE" ]]; then
        PULSE="N/A"
    elif [[ "$PULSE" == "false" ]]; then
        PULSE="*FALSE*"
    fi

    # --- Extract NCC Version ---
    NCC_VER=$(echo "$JSON" | jq -r '
        (.data.config.clusterSoftwareMap[]?
        | select(.softwareType=="NCC")
        | .version) // empty
    ')
    [[ -z "$NCC_VER" ]] && NCC_VER="N/A"

    # --- Extract Hypervisor Type ---
    HYPERVISOR=$(echo "$JSON" | jq -r '.data.config.hypervisorTypes[0] // empty')
    [[ -z "$HYPERVISOR" ]] && HYPERVISOR="N/A"

    # --- Append to CSV ---
    echo "Cluster IP,$EXT_ADDRESS" >> "$csv_file"
    echo "Data Services IP,$EDS_ADDRESS" >> "$csv_file"
    echo "AOS Version,$AOS_VERSION" >> "$csv_file"
    echo "Hypervisor Type,$HYPERVISOR" >> "$csv_file"
    echo "Name Servers,$NAME_SERVERS" >> "$csv_file"
    echo "NTP Servers,$NTP_SERVERS" >> "$csv_file"
    echo "Redundancy Factor,$RF" >> "$csv_file"
    echo "Pulse,$PULSE" >> "$csv_file"
    echo "NCC Version,$NCC_VER" >> "$csv_file"
}

lcm_version_check() {
    local cluster="$1"
    local csv_file="$OUTDIR/${cluster}.csv"

    # Fetch the LCM config JSON
    local json
    json=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/lifecycle/v4.0/resources/config")

    # ✅ Extract major.minor.patch (e.g., 3.2.1)
    local version
    version=$(echo "$json" | jq -r '
        .data.version // empty
    ' | sed -E 's/^([0-9]+\.[0-9]+\.[0-9]+).*/\1/')

    # Fallback if version not found
    [[ -z "$version" ]] && version="N/A"

    # Append to CSV
    echo "LCM Version,$version" >> "$csv_file"
}

vs_mtu_check() {
    local cluster="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    if [[ -z "$ext_ip" || "$ext_ip" == "N/A" ]]; then
        echo "[WARN] Skipping vSwitch check for $cluster — No external IP found"
        return
    fi

    echo "[INFO] Fetching vSwitch data from $ext_ip for $cluster..."

    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$ext_ip:$PORT/api/networking/v4.0.a2/config/virtual-switches")

    if [[ -z "$RESPONSE" ]]; then
        echo "[ERROR] No response from $ext_ip"
        return
    fi

    # =========================
    # 1️⃣ Bridge Name & NICs (PER HOST)
    # =========================
    echo "," >> "$csv_file"
    echo "Bridge Name,NICs" >> "$csv_file"

    echo "$RESPONSE" | jq -r '
        .data[]?.clusters[]?.hosts[]? |
        "\(.internalBridgeName),\(.hostNics | join(" "))"
    ' | while IFS= read -r line; do
        echo "$line" >> "$csv_file"

        bridge_name=$(echo "$line" | cut -d',' -f1)
        nics=$(echo "$line" | cut -d',' -f2)

        echo "[INFO] Found bridge: $bridge_name with NICs: $nics for $cluster"
    done


    # =========================
    # 2️⃣ vSwitch MTU & Bond Mode
    # =========================
    echo "," >> "$csv_file"
    echo "vSwitch,MTU,Bond_Mode" >> "$csv_file"

    echo "$RESPONSE" | jq -r '
        .data[]? |
        "\(.name),\(.mtu),\(.bondMode // "N/A")"
    ' | while IFS= read -r line; do
        echo "$line" >> "$csv_file"

        vs_name=$(echo "$line" | cut -d',' -f1)
        mtu=$(echo "$line" | cut -d',' -f2)
        bond=$(echo "$line" | cut -d',' -f3)

        echo "[INFO] Found vSwitch: $vs_name, MTU: $mtu, Bond Mode: $bond for $cluster"
    done

    echo "," >> "$csv_file"
}

snmp_status_check() {
    local cluster="$1"
    local extid="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Checking SNMP status for $cluster ($extid)..."

    # --- Fetch SNMP JSON ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$extid/snmp")

    if [[ -z "$RESPONSE" ]]; then
        echo "[ERROR] No response for SNMP status on $cluster"
        echo "SNMP Status,N/A" >> "$csv_file"
        return
    fi

    # --- Extract isEnabled safely ---
    local is_enabled
    is_enabled=$(echo "$RESPONSE" | jq -r '.data.isEnabled // empty')

    # --- Determine status ---
    local status
    if [[ "$is_enabled" == "true" ]]; then
        status="Enabled"
        echo "[INFO] SNMP is ENABLED for $cluster"
    elif [[ "$is_enabled" == "false" ]]; then
        status="Disabled"
        echo "[INFO] SNMP is DISABLED for $cluster"
    else
        status="UNKNOWN"
        echo "[WARN] Could not determine SNMP status for $cluster"
    fi

    # --- Append to CSV ---
    echo "SNMP Status,$status" >> "$csv_file"
}

fetch_cluster_stats() {
    local cluster="$1"
    local extid="$2"
    local end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    # macOS date uses -v for relative date; fallback for Linux
    local start_time=$(date -u -v-30d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "30 days ago" +"%Y-%m-%dT%H:%M:%SZ")
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Fetching stats for $cluster ($extid)..."

    local response
    response=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/stats/clusters/${extid}?\$startTime=${start_time}&\$endTime=${end_time}&\$samplingInterval=604800&\$statType=MAX&\$select=cpuUsageHz,overallMemoryUsageBytes,storageUsageBytes")

    # Check for API errors
    if echo "$response" | jq -e '.error? // empty' >/dev/null; then
        echo "[WARN] Failed to fetch stats for $cluster"
        return
    fi

    # Extract MAX values safely
    local cpu_max mem_max storage_max

    cpu_max=$(echo "$response" | jq -r '.. | objects | select(has("cpuUsageHz")) | .cpuUsageHz[]?.value // empty' | sort -nr | head -1)
    mem_max=$(echo "$response" | jq -r '.. | objects | select(has("overallMemoryUsageBytes")) | .overallMemoryUsageBytes[]?.value // empty' | sort -nr | head -1)
    storage_max=$(echo "$response" | jq -r '.. | objects | select(has("storageUsageBytes")) | .storageUsageBytes[]?.value // empty' | sort -nr | head -1)

    # Fallback for empty values
    [[ -z "$cpu_max" ]] && cpu_max="N/A"
    [[ -z "$mem_max" ]] && mem_max="N/A"
    [[ -z "$storage_max" ]] && storage_max="N/A"

    echo "," >> "$csv_file"
    echo "CPU_Max_Hz,$cpu_max" >> "$csv_file"
    echo "MEM_Max_bytes,$mem_max" >> "$csv_file"
    echo "STORAGE_Max_bytes,$storage_max" >> "$csv_file"

    echo "[INFO] Stats recorded for $cluster → CPU: $cpu_max, MEM: $mem_max, STORAGE: $storage_max"
}

fetch_storage_containers() {
    local cluster="$1"
    local extid="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Fetching storage containers for $cluster ($extid)..."

    # --- Fetch JSON ---
    local response
    response=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/storage-containers?\$filter=clusterExtId%20eq%20'${extid}'&\$select=name,erasureCode,cacheDeduplication,onDiskDedup,isCompressionEnabled")

    if [[ -z "$response" ]]; then
        echo "[WARN] No response from API for $cluster"
        return
    fi

    # --- CSV Header ---
    echo "," >> "$csv_file"
    echo "Storage Container,erasureCode,cacheDeduplication,onDiskDedup,isCompressionEnabled" >> "$csv_file"

    # --- Parse & append each container ---
    echo "$response" | jq -r '
        .data[]? |
        select(.name != null) |
        [
            .name,
            (.erasureCode // "N/A"),
            (.cacheDeduplication // "N/A"),
            (.onDiskDedup // "N/A"),
            (if .isCompressionEnabled then "Enabled" else "Disabled" end)
        ] | @csv
    ' | sed 's/"//g' >> "$csv_file"

    echo "," >> "$csv_file"
    echo "[INFO] Storage container details appended for $cluster"
}

get_all_storage_containers_io_latency() {
    local cluster_name="$1"
    local cluster_extID="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Starting container I/O latency collection for cluster: $cluster_name ($cluster_extID)"
    echo "," >> "$csv_file"
    echo "CONTAINER NAME,I/O LATENCY in ms,STATUS (> 3ms is HIGH)" >> "$csv_file"

    # --- Fetch storage containers list ---
    local resp
    resp=$(curl -sk -u "${USERNAME}:${PASSWORD}" \
        "https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/storage-containers?\$filter=clusterExtId%20eq%20'${cluster_extID}'&\$select=name,containerExtId")

    if [[ -z "$resp" ]]; then
        echo "[WARN] Storage containers API returned empty response for cluster: $cluster_name"
        return
    fi

    echo "[INFO] Extracting containers from cluster $cluster_name"

    # --- Loop over each container ---
    echo "$resp" | jq -r '.data[]? | select(.name != null) | "\(.name),\(.containerExtId)"' | while IFS=',' read -r ctr_name ctr_extid; do
        echo "[INFO] Processing container: $ctr_name"

        # macOS/Linux-compatible date
        local endTime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        local startTime=$(date -u -v-7d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "7 days ago" +"%Y-%m-%dT%H:%M:%SZ")

        local url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/stats/storage-containers/${ctr_extid}?\$samplingInterval=86400&\$startTime=${startTime}&\$endTime=${endTime}"

        local stats
        stats=$(curl -sk -u "${USERNAME}:${PASSWORD}" "$url")

        if [[ -z "$stats" ]]; then
            echo "[WARN] No stats response for container $ctr_name ($ctr_extid)"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        # Extract latency values (µs)
        local values
        values=$(echo "$stats" | jq -r '.data.controllerAvgIoLatencyuSecs[]?.value // empty')

        if [[ -z "$values" ]]; then
            echo "[WARN] No latency datapoints found for container $ctr_name"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        # Compute average latency
        local sum=0
        local count=0
        for v in $values; do
            [[ "$v" =~ ^[0-9]+$ ]] && sum=$((sum + v)) && count=$((count + 1))
        done

        if [[ $count -eq 0 ]]; then
            echo "[WARN] All latency datapoints non-numeric for $ctr_name"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        local avg=$((sum / count))  # µs
        local avg_ms=$(echo "scale=3; $avg / 1000" | bc)

        # Threshold check
        local status="OK"
        (( $(echo "$avg_ms > 3" | bc -l) )) && status="**HIGH**" && echo "[WARN] High avg I/O latency for $ctr_name: ${avg_ms} ms"

        echo "${ctr_name},${avg_ms} ms,${status}" >> "$csv_file"
    done

    echo "[INFO] Completed container I/O latency collection for cluster: $cluster_name"
}

get_host_count() {
    local cluster="$1"
    local ext_id="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    if [[ -z "$ext_id" || "$ext_id" == "N/A" ]]; then
        echo "[WARN] Skipping host info for $cluster — No cluster extID found"
        return
    fi

    echo "[INFO] Fetching host info for $cluster ($ext_id)..."

    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$ext_id/hosts?\$select=hostName,blockModel,maintenanceState,nodeStatus")

    if [[ -z "$RESPONSE" ]]; then
        echo "[ERROR] No response for host info on $cluster"
        return
    fi

    # --- Total hosts safely ---
    local total_hosts
    total_hosts=$(echo "$RESPONSE" | jq -r '.totalAvailableResults // (.data | length) // 0')

    echo "," >> "$csv_file"
    echo "Total Hosts,${total_hosts}" >> "$csv_file"
    echo "[INFO] Total hosts in $cluster: $total_hosts"

    # --- Host Table Header ---
    echo "," >> "$csv_file"
    echo "Hostname,Block Model,Maintenance State,Node Status" >> "$csv_file"

    # --- Extract and format host details ---
    echo "$RESPONSE" | jq -r '
        # Ensure we always iterate over an array, even if missing
        (.data // [])[] |
        [
            (.hostName // "**N/A**"),
            (.blockModel // "**N/A**"),
            (.maintenanceState // "N/A"),
            (.nodeStatus // "**N/A**")
        ] |
        [
            .[0],
            .[1],
            if (.[2] != "normal" and .[2] != "N/A") then "**" + .[2] + "**" else .[2] end,
            if (.[3] != "NORMAL" and .[3] != "N/A") then "**" + .[3] + "**" else .[3] end
        ] | @csv
    ' | sed 's/"//g' >> "$csv_file"

    echo "," >> "$csv_file"
    echo "[INFO] Host details appended to $csv_file"
}

get_offline_disks() {
    local cluster="$1"
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Fetching Offline disks for $cluster..."

    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/disks?\$filter=clusterName%20eq%20'$cluster'%20and%20diskAdvanceConfig/isOnline%20ne%20true&\$select=serialNumber,diskAdvanceConfig/isOnline")

    # --- Total offline disks ---
    local total
    total=$(echo "$RESPONSE" | jq -r '.totalAvailableResults // 0')

    if [[ -z "$total" || "$total" -eq 0 ]]; then
        echo "[INFO] No offline disks found for $cluster"
        echo "Offline_Disks,0" >> "$csv_file"
        return
    fi

    echo "[INFO] Found $total offline disks for $cluster"

    # --- Extract disk serial numbers and mark Offline ---
    echo "," >> "$csv_file"
    echo "SerialNumber,Status" >> "$csv_file"

    echo "$RESPONSE" | jq -r '
        .data[]? |
        [
            (.serialNumber // "N/A"),
            "Offline"
        ] | @csv
    ' | sed 's/"//g' >> "$csv_file"

    echo "[INFO] Offline disk info appended to $csv_file"
}

get_license_details() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"
    local api_url="https://${PC_IP}:${PORT}/api/licensing/v4.0/config/entitlements?\$filter=clusterExtId%20eq%20'${cluster_extid}'&\$expand=details"

    echo "[INFO] Fetching license details for $cluster_name..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response for License API ($cluster_name)"
        echo "License_Name,Quantity (Meter),Expiry_Date" >> "$csv_file"
        echo "N/A,N/A,N/A" >> "$csv_file"
        return
    fi

    echo "," >> "$csv_file"
    echo "License_Name,Quantity (Meter),Expiry_Date" >> "$csv_file"

    # --- Parse and exclude NODE-based licenses ---
    echo "$response" | jq -r '
        .data[]?.details[]?
        | select(.meter != "NODE")
        | [
            (.name // "N/A"),
            ((.quantity // 0 | tostring) + " (" + (.meter // "N/A") + ")"),
            (.earliestExpiryDate // "N/A")
          ] 
        | @csv
    ' | sed 's/"//g' >> "$csv_file"

    echo "[INFO] License details appended for $cluster_name"
}

get_cpu_ratio() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Calculating vCPU:pCPU ratio for $cluster_name..."

    # -------------------------------------------------------
    # Step 0: Detect Hypervisor Type from existing CSV
    # -------------------------------------------------------
    local hypervisor_type
    hypervisor_type=$(awk -F',' '$1=="Hypervisor Type"{print $2}' "$csv_file")

    echo "[INFO] Detected Hypervisor Type: $hypervisor_type"

    if [[ -z "$hypervisor_type" ]]; then
        echo "[WARN] Hypervisor Type not found in CSV for $cluster_name"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    # Normalize hypervisor string for API path
    local hv_api_type
    case "$hypervisor_type" in
        AHV)   hv_api_type="ahv"  ;;
        ESXi) hv_api_type="esxi" ;;
        *)
            echo "[WARN] Unsupported hypervisor type: $hypervisor_type"
            echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
            return
            ;;
    esac

    # -------------------------------------------------------
    # Step 1: Fetch Hosts (pCPU threads)
    # -------------------------------------------------------
    local hosts_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts?\$select=numberOfCpuThreads"
    local hosts_response
    hosts_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$hosts_api_url")

    if [[ -z "$hosts_response" ]]; then
        echo "[WARN] Empty response for Hosts API ($cluster_name)"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    local total_threads
    total_threads=$(echo "$hosts_response" | jq '[.data[]?.numberOfCpuThreads // 0] | add // 0')

    echo "[INFO] Total pCPU threads (all hosts): $total_threads"

    if [[ "$total_threads" -eq 0 ]]; then
        echo "[WARN] Total pCPU threads is zero for $cluster_name"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    # -------------------------------------------------------
    # Step 2: Fetch VMs (AHV or ESXi dynamically)
    # -------------------------------------------------------
    local vms_api_url="https://${PC_IP}:${PORT}/api/vmm/v4.0/${hv_api_type}/config/vms?\$select=numSockets,numCoresPerSocket,numThreadsPerCore&\$filter=cluster/extId%20eq%20'${cluster_extid}'"
    local vms_response
    vms_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$vms_api_url")

    if [[ -z "$vms_response" ]]; then
        echo "[WARN] Empty response for ${hypervisor_type} VMs API ($cluster_name)"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    # -------------------------------------------------------
    # Step 3: Calculate total vCPUs from VMs
    # -------------------------------------------------------
    local total_vm_vcpus
    total_vm_vcpus=$(echo "$vms_response" | jq '[.data[] | (.numSockets * .numCoresPerSocket * .numThreadsPerCore)] | add // 0')

    echo "[INFO] Total vCPUs from ${hypervisor_type} VMs: $total_vm_vcpus"

    # -------------------------------------------------------
    # Step 4: Add CVM vCPUs (already global)
    # -------------------------------------------------------
    local total_vcpus=$((total_vm_vcpus + total_cvm_vcpus))

    echo "[INFO] Including CVM vCPUs: $total_cvm_vcpus"
    echo "[INFO] Total vCPUs (VMs + CVMs): $total_vcpus"

    # -------------------------------------------------------
    # Step 5: Compute Ratio
    # -------------------------------------------------------
    local ratio
    ratio=$(awk "BEGIN {printf \"%.2f\", $total_vcpus / $total_threads}")

    echo "vCPU_to_pCPU_Ratio,${ratio}:1" >> "$csv_file"
    echo "[INFO] vCPU:pCPU ratio for $cluster_name = ${ratio}:1"
}

get_directory_services_pc() {
    local cluster_name="$1"
    local csv_file="$OUTDIR/${cluster_name}.csv"
    local api_url="https://${PC_IP}:${PORT}/api/iam/v4.0/authn/directory-services?\$select=directoryType,url,name,domainName"

    echo "[INFO] Fetching directory services from Prism Central..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response from Directory Services API"
        echo "," >> "$csv_file"
        echo "From Prism Central Directory Services," >> "$csv_file"
        echo "Name,DomainName,DirectoryType,URL" >> "$csv_file"
        echo "N/A,N/A,N/A,N/A" >> "$csv_file"
        return
    fi

    # Write CSV header
    echo "," >> "$csv_file"
    echo "From Prism Central Directory Services," >> "$csv_file"
    echo "Name,DomainName,DirectoryType,URL" >> "$csv_file"

    # Extract and format using jq
    echo "$response" | jq -r '
        .data[]? 
        | select(.name != null and .name != "hasError" and .name != "isPaginated")
        | [
            .name // "N/A",
            .domainName // "N/A",
            .directoryType // "N/A",
            .url // "N/A"
          ] 
        | @csv
    ' >> "$csv_file"

    echo "[INFO] Directory services saved to $csv_file"
}

get_hosts_and_nics() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching hosts for cluster $cluster_extid ..."

    # Create CSV header once
    echo "host,nic,status" >> "$csv_file"

    local hosts_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts/?\$select=hostName,extId"
    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$hosts_api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response from Hosts API for cluster $cluster_extid"
        return
    fi

    # Loop through hosts
    echo "$response" | jq -r '
        .data[]?
        | select(.hostName != null)
        | [.hostName, .extId]
        | @tsv
    ' | while IFS=$'\t' read -r host_name host_id; do

        echo "[INFO] Fetching NICs for host $host_name ($host_id)..."

        local page=0
        local limit=100
        local has_more=true

        while [[ "$has_more" == "true" ]]; do

            local nics_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts/${host_id}/host-nics?\$limit=${limit}&\$page=${page}"
            local nics_response
            nics_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$nics_api_url")

            if [[ -z "$nics_response" ]]; then
                echo "[WARN] Empty NIC response for host $host_name (page $page)"
                break
            fi

            # Write NIC data to CSV
            echo "$nics_response" | jq -r --arg host "$host_name" '
                .data[]?
                | select(.name != null)
                | [
                    $host,
                    .name,
                    (if .interfaceStatus == "1" then "Connected"
                     elif .interfaceStatus == "0" then "Not Connected"
                     else .interfaceStatus end)
                  ]
                | @csv
            ' >> "$csv_file"

            # Pagination handling
            local total_results
            total_results=$(echo "$nics_response" | jq -r '.metadata.totalAvailableResults // 0')

            ((page++))
            if (( page * limit >= total_results )); then
                has_more=false
            fi

        done
    done

    echo "[INFO] Host NICs saved to $csv_file"
}

get_alert_parameters() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching alerts for $cluster_name with pagination..."

    {
        echo ","
        echo "SEVERITY,TITLE,CAUSE,RESOLUTION"
    } >> "$csv_file"

    local warn_count=0
    local crit_count=0
    local page=0
    local limit=100
    local has_more=true

    while [[ "$has_more" == "true" ]]; do
        local alerts_api_url="https://${PC_IP}:${PORT}/api/monitoring/v4.0/serviceability/alerts?\$filter=originatingClusterUUID%20eq%20'${cluster_extid}'%20and%20isResolved%20eq%20false&\$select=title,severity,rootCauseAnalysis,parameters&\$limit=${limit}&\$page=${page}"

        local response
        response=$(curl -sk -u "$USERNAME:$PASSWORD" "$alerts_api_url")

        if [[ -z "$response" || "$response" == "null" ]]; then
            echo "[WARN] Empty response for Alerts API ($cluster_name, page $page)"
            break
        fi

        local record_count
        record_count=$(echo "$response" | jq '.data | length')

        [[ "$record_count" -eq 0 ]] && break

        while IFS= read -r alert_json; do
            local sev
            sev=$(echo "$alert_json" | jq -r '.severity // empty')
            [[ -z "$sev" || "$sev" == "INFO" ]] && continue

            if [[ "$sev" == "WARNING" ]]; then
                ((warn_count++))
            elif [[ "$sev" == "CRITICAL" ]]; then
                ((crit_count++))
            fi

            local tit cau res
            tit=$(echo "$alert_json" | jq -r '.title // "N/A"')

            cau=$(echo "$alert_json" | jq -r '[ .rootCauseAnalysis[]?.cause // "" ] | map(select(length>0)) | join(" | ")')
            res=$(echo "$alert_json" | jq -r '[ .rootCauseAnalysis[]?.resolution // "" ] | map(select(length>0)) | join(" | ")')

            [[ -z "$cau" ]] && cau="N/A"
            [[ -z "$res" ]] && res="N/A"

            while IFS='|' read -r key val; do
                [[ -z "$key" || -z "$val" ]] && continue
                [[ "$key" =~ (uuid|id|version) ]] && continue
                tit="${tit//\{$key\}/$val}"
            done < <(
                echo "$alert_json" | jq -r '
                    .parameters[]?
                    | select(.paramName != null and (.paramValue.stringValue != null))
                    | "\(.paramName)|\(.paramValue.stringValue)"
                '
            )

            sev="${sev//,/;}"
            tit="${tit//,/;}"
            cau="${cau//,/;}"
            res="${res//,/;}"
            sev="${sev//$'\n'/ }"
            tit="${tit//$'\n'/ }"
            cau="${cau//$'\n'/ }"
            res="${res//$'\n'/ }"

            echo "$sev,$tit,$cau,$res" >> "$csv_file"

        done < <(echo "$response" | jq -c '.data[]')

        local is_truncated
        is_truncated=$(echo "$response" | jq -r '.metadata.flags[]? | select(.name=="isTruncated") | .value')

        if [[ "$is_truncated" == "true" ]]; then
            ((page++))
        else
            has_more=false
        fi
    done

    {
        echo ","
        echo "Alert_Summary,Count"
        echo "WARNING,$warn_count"
        echo "CRITICAL,$crit_count"
    } >> "$csv_file"

    echo "[INFO] Alerts appended for $cluster_name"
    echo "[INFO] WARNING Alerts: $warn_count | CRITICAL Alerts: $crit_count"
}

# v2.0
get_cluster_security_config() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching Security & Compliance config for $cluster_name..."

    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$ext_ip:$PORT/api/nutanix/v2.0/cluster")

    if [[ -z "$RESPONSE" || "$RESPONSE" == "null" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "Security_Config,**N/A**" >> "$csv_file"
        return
    fi

    # ---------- Helper Functions (SAFE) ----------
    bool_to_status() {
        if [[ "$1" == "true" ]]; then
            echo "Enabled"
        else
            echo "**Disabled**"
        fi
    }

    pc_status_text() {
        if [[ "$1" == "true" ]]; then
            echo "Connected"
        elif [[ "$1" == "false" ]]; then
            echo "Not Connected"
        else
            echo "N/A"
        fi
    }

    # ---------- Safe jq Extracts ----------
    local pc_status_raw
    pc_status_raw=$(echo "$RESPONSE" | jq -r '.is_registered_to_pc // empty')
    local pc_status
    pc_status=$(pc_status_text "$pc_status_raw")

    local rebuild_raw
    rebuild_raw=$(echo "$RESPONSE" | jq -r '.enable_rebuild_reservation // false')
    local rebuild_reservation
    rebuild_reservation=$(bool_to_status "$rebuild_raw")

    local degraded_raw
    degraded_raw=$(echo "$RESPONSE" | jq -r '.disable_degraded_node_monitoring // false')
    local degraded_monitor
    if [[ "$degraded_raw" == "true" ]]; then
        degraded_monitor="**Disabled**"
    else
        degraded_monitor="Enabled"
    fi

    local recycle_bin_ttl
    recycle_bin_ttl=$(echo "$RESPONSE" | jq -r '.recycle_bin_dto.recycle_bin_ttlsecs // 0')
    local recycle_bin_status
    if [[ "$recycle_bin_ttl" -gt 0 ]]; then
        recycle_bin_status="Enabled"
    else
        recycle_bin_status="**Disabled**"
    fi

    local schedule
    schedule=$(echo "$RESPONSE" | jq -r '.security_compliance_config.schedule // "N/A"')

    local aide_raw
    aide_raw=$(echo "$RESPONSE" | jq -r '.security_compliance_config.enable_aide // false')
    local aide
    aide=$(bool_to_status "$aide_raw")

    local core_raw
    core_raw=$(echo "$RESPONSE" | jq -r '.security_compliance_config.enable_core // false')
    local core
    core=$(bool_to_status "$core_raw")

    local high_strength_raw
    high_strength_raw=$(echo "$RESPONSE" | jq -r '.security_compliance_config.enable_high_strength_password // false')
    local high_strength
    high_strength=$(bool_to_status "$high_strength_raw")

    local banner_raw
    banner_raw=$(echo "$RESPONSE" | jq -r '.security_compliance_config.enable_banner // false')
    local banner
    banner=$(bool_to_status "$banner_raw")

    # ---------- Append to CSV ----------
    {
        echo ","
        echo "Custom Hardening Parameters,Value"
        echo "Security_Schedule,$schedule"
        echo "AIDE,$aide"
        echo "Core,$core"
        echo "High_Strength_Password,$high_strength"
        echo "Welcome_Banner,$banner"
        echo ","
        echo "Prism Central Connection status,$pc_status"
        echo "Enable_Rebuild_Reservation,$rebuild_reservation"
        echo "Degraded_Node_Detection,$degraded_monitor"
        echo "Retain_Deleted_VMs,$recycle_bin_status"
    } >> "$csv_file"

    echo "[INFO] Security & Compliance config appended to $csv_file"

    # ---------- WARNINGS ----------
    [[ "$rebuild_reservation" == "**Disabled**" ]] && echo "[WARN] Rebuild reservation is DISABLED on $cluster_name"
    [[ "$aide" == "**Disabled**" ]] && echo "[WARN] AIDE DISABLED on $cluster_name"
    [[ "$core" == "**Disabled**" ]] && echo "[WARN] Core compliance DISABLED on $cluster_name"
    [[ "$high_strength" == "**Disabled**" ]] && echo "[WARN] High-strength password policy DISABLED on $cluster_name"
    [[ "$banner" == "**Disabled**" ]] && echo "[WARN] Welcome banner DISABLED on $cluster_name"
    [[ "$degraded_monitor" == "**Disabled**" ]] && echo "[WARN] Degraded node detection is DISABLED on $cluster_name"
    [[ "$recycle_bin_status" == "**Disabled**" ]] && echo "[WARN] Recycle bin is DISABLED on $cluster_name"
}

get_snapshots_info() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching Snapshots info for $cluster_name..."

    # --- API Call ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" "https://$ext_ip:$PORT/api/nutanix/v2.0/snapshots/")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "Snapshots,N/A" >> "$csv_file"
        return
    fi

    # --- Extract total_entities ---
    local total
    total=$(echo "$RESPONSE" | jq -r '.metadata.total_entities // 0')

    if [[ "$total" -eq 0 ]]; then
        echo "[INFO] No snapshots found for $cluster_name"
        echo "total_snapshots_in_cluster,0" >> "$csv_file"
        return
    fi

    echo "[INFO] Total snapshots found: $total"

    # --- Append CSV Header ---
    echo "," >> "$csv_file"
    echo "VM_Name,Snapshot_Names" >> "$csv_file"

    # --- Group snapshots by VM using macOS-compatible awk ---
    echo "$RESPONSE" | jq -r '.entities[] | [.vm_create_spec.name, .snapshot_name] | @tsv' | \
    awk '{
        vm=$1
        snap=$2
        if (vm in snaps) {
            snaps[vm] = snaps[vm] "," snap
        } else {
            snaps[vm] = snap
        }
    } END {
        for (v in snaps) print v "," snaps[v]
    }' >> "$csv_file"

    # --- Append total count ---
    echo "total_snapshots_in_cluster,${total}" >> "$csv_file"
    echo "," >> "$csv_file"
    echo "[INFO] Snapshot info appended to $csv_file"
}

get_ha_state() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching HA state for $cluster_name..."

    # --- API Call ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" "https://$ext_ip:$PORT/api/nutanix/v2.0/ha")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "ha_state,N/A" >> "$csv_file"
        return
    fi

    # --- Extract ha_state safely with jq ---
    local ha_state
    ha_state=$(echo "$RESPONSE" | jq -r '.ha_state // "N/A"')

    # --- Log and warn if needed ---
    if [[ "$ha_state" == "N/A" ]]; then
        echo "[WARN] HA state not found for $cluster_name"
    else
        echo "[INFO] HA State for $cluster_name: $ha_state"
    fi

    # --- Append to CSV ---
    echo "," >> "$csv_file"
    echo "ha_state,${ha_state}" >> "$csv_file"
}

get_smtp_status() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching SMTP configuration for $cluster_name..."

    # --- API Call ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$ext_ip:$PORT/api/nutanix/v2.0/cluster/smtp")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "SMTP_Status,**N/A**" >> "$csv_file"
        return
    fi

    # --- Extract status safely with jq ---
    local smtp_status
    smtp_status=$(echo "$RESPONSE" | jq -r '.email_status.status // "N/A"')

    case "$smtp_status" in
        "SUCCESS") smtp_status="Success" ;;
        "N/A") smtp_status="**N/A**" ;;
        *) smtp_status="**Failed**" ;;
    esac

    # --- Append to CSV ---
    echo "SMTP_Status,$smtp_status" >> "$csv_file"
    echo "[INFO] SMTP configuration status appended to $csv_file"

    # --- Warning if failed ---
    if [[ "$smtp_status" != "Success" ]]; then
        echo "[WARN] SMTP status check FAILED or not configured properly on $cluster_name (Status: $smtp_status)"
    fi
}

get_directory_services_pe_v2() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"
    local api_url="https://${ext_ip}:$PORT/PrismGateway/services/rest/v2.0/authconfig/"

    echo "[INFO] Fetching directory services from Prism Element..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response from Prism Element Directory Services API"
        echo "," >> "$csv_file"
        echo "From Prism Element Directory Services," >> "$csv_file"
        echo "Name,Domain,DirectoryType,URL" >> "$csv_file"
        echo "N/A,N/A,N/A,N/A" >> "$csv_file"
        return
    fi

    # Write CSV header
    echo "," >> "$csv_file"
    echo "From Prism Element Directory Services," >> "$csv_file"
    echo "Name,Domain,DirectoryType,URL" >> "$csv_file"

    # Parse using jq and append to CSV
    echo "$response" | jq -r '
        .directory_list[]? |
        [
            .name // "N/A",
            .domain // "N/A",
            .directory_type // "N/A",
            .directory_url // "N/A"
        ] | @csv
    ' >> "$csv_file"
    echo "," >> "$csv_file"

    echo "[INFO] Directory services from PE (v2 API) saved to $csv_file"
}

# v1.0
total_cvm_vcpus=0  # global variable

get_cvm_resources() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "," >> "$csv_file"
    echo "CVM_name,memory (GiB),vCPU" >> "$csv_file"

    local cvm_api_url="https://${PC_IP}:${PORT}/PrismGateway/services/rest/v1/vms?filterCriteria=is_cvm%3D%3D1&proxyClusterUuid=${cluster_extid}"
    echo "[INFO] Fetching CVM resources for $cluster_name..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$cvm_api_url")
    if [[ -z "$response" ]]; then
        echo "[WARN] Empty CVM API response"
        echo "N/A,N/A,N/A" >> "$csv_file"
        total_cvm_vcpus=0
        return
    fi

    # --- Parse CVM info with jq ---
    local cvm_info
    cvm_info=$(echo "$response" | jq -r '
        .entities[]? |
        select(.controllerVm == true) |
        [
            .vmName // "N/A",
            ((.memoryCapacityInBytes // 0) / 1073741824 | tostring),
            (.numVCpus // 0)
        ] | @csv
    ')

    if [[ -z "$cvm_info" ]]; then
        echo "N/A,N/A,N/A" >> "$csv_file"
        total_cvm_vcpus=0
        return
    fi

    # --- Append to CSV ---
    echo "$cvm_info" >> "$csv_file"

    # --- Calculate total CVM vCPUs ---
    total_cvm_vcpus=$(echo "$response" | jq '[.entities[]? | select(.controllerVm == true) | (.numVCpus // 0)] | add // 0')
    echo "[INFO] Total CVM vCPUs for $cluster_name: $total_cvm_vcpus"
}

get_storage_overprovisioning_ratio() {
    local cluster_name="$1"
    local cluster_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching storage overprovisioning ratio for $cluster_name..."

    local api_url="https://${cluster_ip}:${PORT}/PrismGateway/services/rest/v1/clusters"

    local response
    response=$(curl -s -k -u "$USERNAME:$PASSWORD" "$api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response from usage_stats API for $cluster_name"
        echo "Storage_Overprovisioning_Ratio,**N/A**" >> "$csv_file"
        return
    fi

    # Extract values using correct path
    local pre_bytes free_bytes ratio
    pre_bytes=$(echo "$response" | jq -r '.entities[0].usageStats["data_reduction.thin_provision.pre_reduction_bytes"] // empty')
    free_bytes=$(echo "$response" | jq -r '.entities[0].usageStats["storage.free_bytes"] // empty')

    if [[ -z "$pre_bytes" || -z "$free_bytes" || "$free_bytes" -eq 0 ]]; then
        echo "[WARN] Could not extract valid bytes for $cluster_name"
        echo "Storage_Overprovisioning_Ratio,**N/A**" >> "$csv_file"
        return
    fi

    # Calculate ratio
    ratio=$(awk "BEGIN {printf \"%.2f\", $pre_bytes/$free_bytes}")

    echo "," >> "$csv_file"
    echo "Storage_Overprovisioning_Ratio,$ratio:1" >> "$csv_file"
    echo "[INFO] Storage overprovisioning ratio for $cluster_name: $ratio"
}

# =============================
# CHECK REGISTRY (EXECUTION ORDER)
# =============================

REGISTERED_CHECKS=(
  get_cluster_security_config
  get_smtp_status
  snmp_status_check
  get_directory_services_pc
  get_directory_services_pe_v2
  get_snapshots_info
  get_ha_state
  lcm_version_check
  vs_mtu_check
  get_hosts_and_nics
  fetch_cluster_stats
  fetch_storage_containers
  get_all_storage_containers_io_latency
  get_storage_overprovisioning_ratio
  get_host_count
  get_offline_disks
  get_license_details
  get_cvm_resources
  get_cpu_ratio
  get_alert_parameters
)

run_all_checks() {
    local cluster="$1"
    local extid="$2"
    local ext_ip="$3"

    for check in "${REGISTERED_CHECKS[@]}"; do
        echo "[INFO] Running $check on $cluster"

        if declare -f "$check" >/dev/null; then

            # --- ROUTING BASED ON FUNCTION NAME ---
            case "$check" in
                get_cluster_security_config|get_smtp_status|get_directory_services_pe_v2|get_snapshots_info|get_ha_state|get_storage_overprovisioning_ratio|vs_mtu_check)
                    "$check" "$cluster" "$ext_ip"
                    ;;
                get_directory_services_pc|get_offline_disks)
                    "$check" "$cluster"
                    ;;
                *)
                    "$check" "$cluster" "$extid"
                    ;;
            esac

        else
            echo "[WARN] Function $check not found"
            write_csv "$cluster" "$check" "FUNCTION_NOT_FOUND"
        fi
    done
}
