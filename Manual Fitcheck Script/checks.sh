# v4.0
append_cluster_details() {
    local cluster="$1"
    local extid="$2"
    local csv_file="$OUTDIR/${cluster}.csv"
    # Step 1: Fetch the cluster JSON and flatten it
    local JSON
    JSON=$(curl -s -k -u "$USERNAME:$PASSWORD" \
    "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$extid" | tr -d '\n')

    # Step 2: Extract the "externalAddress" 
    local EXT_BLOCK
    EXT_BLOCK=$(echo "$JSON" | sed -n 's/.*"externalAddress":{\(.*\)},"externalDataServiceIp".*/\1/p')
    local EXT_ADDRESS
    EXT_ADDRESS=$(echo "$EXT_BLOCK" | sed -n 's/.*"ipv4":{.*"value":"\([0-9\.]*\)".*/\1/p')
    [[ -z "$EXT_ADDRESS" ]] && EXT_ADDRESS="*N/A*"

    # Step 4: Extract externalDataServiceIp
    local EDS_BLOCK
    EDS_BLOCK=$(echo "$JSON" | sed -n 's/.*"externalDataServiceIp":{\(.*\)},"nameServerIpList".*/\1/p')
    if [[ -z "$EDS_BLOCK" ]]; then
        EDS_BLOCK=$(echo "$JSON" | sed -n 's/.*"externalDataServiceIp":{\(.*\)},"nfsSubnetWhitelist".*/\1/p')
    fi
    local EDS_ADDRESS
    EDS_ADDRESS=$(echo "$EDS_BLOCK" | sed -n 's/.*"ipv4":{.*"value":"\([0-9\.]*\)".*/\1/p')
    [[ -z "$EDS_ADDRESS" ]] && EDS_ADDRESS="*N/A*"

    # Step 5: Extract Name Servers
    local NS_BLOCK
    NS_BLOCK=$(echo "$JSON" | sed -n 's/.*"nameServerIpList":\[\(.*\)\],"ntpServerIpList".*/\1/p')
    local NAME_SERVERS
    NAME_SERVERS=$(echo "$NS_BLOCK" | grep -o '"value":"[^"]*"' | sed -E 's/"value":"([^"]+)"/\1/g' | paste -sd "," -)
    [[ -z "$NAME_SERVERS" ]] && NAME_SERVERS="*N/A*"

    # Step 6: Extract NTP Servers
    local NTP_BLOCK
    NTP_BLOCK=$(echo "$JSON" | sed -n 's/.*"ntpServerIpList":\[\(.*\)\],"smtpServer".*/\1/p')
    local NTP_SERVERS
    NTP_SERVERS=$(echo "$NTP_BLOCK" | grep -o '"value":"[^"]*"' | sed -E 's/"value":"([^"]+)"/\1/g' | paste -sd "," -)
    [[ -z "$NTP_SERVERS" ]] && NTP_SERVERS="*N/A*"

    # Step 7: Extract AOS version
    AOS_BLOCK=$(echo "$JSON" | sed -n 's/.*"buildInfo":{\(.*\)},"clusterFunction".*/\1/p')
    AOS_VERSION=$(echo "$AOS_BLOCK" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
    [[ -z "$AOS_VERSION" ]] && AOS_VERSION="N/A"

    # Step 8: Extract Redundancy Factor
    RF=$(echo "$JSON" | sed -n 's/.*"redundancyFactor":\([0-9]*\).*/\1/p')
    [[ -z "$RF" ]] && RF="N/A"

    # Step 9: Extract pulseStatus block only
    PULSE_BLOCK=$(echo "$JSON" | tr -d '\n' | sed -n 's/.*"pulseStatus":{\(.*\)},"network".*/\1/p')
    # Extract isEnabled value from the pulseStatus block
    PULSE=$(echo "$PULSE_BLOCK" | sed -n 's/.*"isEnabled":\([^,}]*\).*/\1/p')
    # Handle default / false formatting
    if [[ -z "$PULSE" ]]; then
        PULSE="N/A"
    elif [[ "$PULSE" == "false" ]]; then
        PULSE="*FALSE*"
    fi

    # Step 10: Extract clusterSoftwareMap block only
    # Flatten JSON
    JSON_FLAT=$(echo "$JSON" | tr -d '\n')
    # Extract all objects in clusterSoftwareMap
    CSM_OBJECTS=$(echo "$JSON_FLAT" | sed -n 's/.*"clusterSoftwareMap":\[\(.*\)\],".*/\1/p')
    # Loop over each object (separated by '},{')
    NCC_VER="N/A"
    IFS='}' read -ra OBJ_ARR <<< "$CSM_OBJECTS"
    for obj in "${OBJ_ARR[@]}"; do
        if echo "$obj" | grep -q '"softwareType":"NCC"'; then
            NCC_VER=$(echo "$obj" | sed -n 's/.*"version":"\([^"]*\)".*/\1/p')
            break
        fi
    done

    # Step 11: Extract Hypervisor Type
    CONFIG_BLOCK=$(echo "$JSON" | sed -n 's/.*"config":{\(.*\)},"dataReductionEfficiencyStats".*/\1/p')
    HYPERVISOR=$(echo "$JSON" | tr -d '\r' | tr '\n' ' ' | \
        sed -n 's/.*"hypervisorTypes":[[]"\([^"]*\)".*/\1/p')
    [[ -z "$HYPERVISOR" ]] && HYPERVISOR="N/A"

    # Append to CSV
    echo "Cluster IP,$EXT_ADDRESS" >> "$csv_file"
    echo "Data Services IP,$EDS_ADDRESS" >> "$csv_file"
    echo "AOS Version,$AOS_VERSION" >> "$csv_file"
    echo "Hypervisor Type,$HYPERVISOR" >> "$csv_file"
    echo "Name Servers,$NAME_SERVERS" >> "$csv_file"
    echo "NTP Servers,$NTP_SERVERS" >> "$csv_file"
    echo "Redundancy Factor,$RF" >> "$csv_file"
    echo "Pulse,$PULSE" >> "$csv_file"
    echo "NCC Version,$NCC_VER" >> "$csv_file"

    echo "[INFO] Appended cluster details for $cluster"
}

lcm_version_check() {
    local cluster="$1"
    local csv_file="$OUTDIR/${cluster}.csv"

    # Fetch the LCM config JSON
    local json
    json=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/lifecycle/v4.0/resources/config")

    # Extract the major.minor version from "version" field
    local version
    version=$(echo "$json" | grep -o '"version":"[^"]*"' | head -1 | sed -E 's/.*"version":"([0-9]+\.[0-9]+).*/\1/')

    # Fallback if version not found
    [[ -z "$version" ]] && version="N/A"

    # Append to CSV
    echo "LCM Version,$version" >> "$csv_file"
    echo "[INFO] LCM version ($version) appended to $csv_file"
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

    # Extract per-host internalBridgeName and hostNics using awk
            echo "$RESPONSE" | awk '
            {
                print ","
                print "Bridge Name,NICs"
                line = $0
                # Loop through each host object
                while (match(line, /"internalBridgeName"[[:space:]]*:[[:space:]]*"[^"]+"/)) {
                    bridge = substr(line, RSTART, RLENGTH)
                    gsub(/.*:/,"",bridge)
                    gsub(/"/,"",bridge)

                    # Move line past this internalBridgeName to avoid infinite loop
                    line = substr(line, RSTART + RLENGTH)

                    # Extract hostNics array for this host
                    if (match(line, /"hostNics"[[:space:]]*:\[[^]]+\]/)) {
                        nics = substr(line, RSTART, RLENGTH)
                        gsub(/.*\[/,"",nics)
                        gsub(/\].*/,"",nics)
                        gsub(/"/,"",nics)
                        gsub(/,/," ",nics)
                        print bridge "," nics

                        # Move line past this hostNics array so next host can be found
                        line = substr(line, RSTART + RLENGTH)
                    }
                }
            }
            ' >> "$csv_file"

    # Compact JSON to single line
    local compact_json
    compact_json=$(echo "$RESPONSE" | tr -d '\n\r')

    # Extract only inside the "data" array
    local data_array
    data_array=$(echo "$compact_json" | sed -n 's/.*"data":\[\(.*\)\],"\$reserved".*/\1/p')

    # If $reserved not found, fallback
    if [[ -z "$data_array" ]]; then
        data_array=$(echo "$compact_json" | sed -n 's/.*"data":\[\(.*\)\].*/\1/p')
    fi

    if [[ -z "$data_array" ]]; then
        echo "[WARN] No 'data' array found in vSwitch JSON for $cluster. Check Authorization on Prism Element."
        return
    fi

    # Split the VirtualSwitch objects (they are separated by "},{" )
    echo "$data_array" | sed 's/},{/}§{/g' | tr '§' '\n' | while IFS= read -r vs; do
        # Extract the name field
        local name
        name=$(echo "$vs" | grep -o '"name":"[^"]*"' | head -1 | sed 's/"name":"\([^"]*\)"/\1/')

        # Extract the MTU value
        local mtu
        mtu=$(echo "$vs" | grep -o '"mtu":[0-9]\+' | head -1 | sed 's/"mtu"://')

        # Extract the bond mode
        local bond_mode
        bond_mode=$(echo "$vs" | grep -o '"bondMode":"[^"]*"' | head -1 | sed 's/"bondMode":"\([^"]*\)"/\1/')

        if [[ -n "$name" && -n "$mtu" ]]; then
            echo "," >> "$csv_file"
            echo "${name},mtu=${mtu},bond_mode=${bond_mode}" >> "$csv_file"
            echo "[INFO] Found vSwitch: $name, MTU: $mtu for $cluster"
        fi
    done
    echo "," >> "$csv_file"
}

snmp_status_check() {
    local cluster="$1"
    local extid="$2"
    local csv_file="$OUTDIR/${cluster}.csv"
    echo "[INFO] Checking SNMP status for $cluster ($extid)..."

    # Fetch SNMP JSON
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$extid/snmp")

    if [[ -z "$RESPONSE" ]]; then
        echo "[ERROR] No response for SNMP status on $cluster"
        echo "SNMP_Status,N/A" >> "$csv_file"
        return
    fi

    # Compact JSON (remove newlines and spaces)
    compact_json=$(echo "$RESPONSE" | tr -d '\n\r')

    # Extract the value of "isEnabled" safely
    is_enabled=$(echo "$compact_json" | sed -E 's/.*"isEnabled":(true|false).*/\1/')

    if [[ "$is_enabled" == "true" ]]; then
        echo "[INFO] SNMP is ENABLED for $cluster"
        echo "\"snmp\",\"Enabled\"" >> "$OUTDIR/${cluster}.csv"
    elif [[ "$is_enabled" == "false" ]]; then
        echo "[INFO] SNMP is DISABLED for $cluster"
        echo "\"snmp\",\"Disabled\"" >> "$OUTDIR/${cluster}.csv"
    else
        echo "[WARN] Could not determine SNMP status for $cluster"
        echo "\"snmp\",\"UNKNOWN\"" >> "$OUTDIR/${cluster}.csv"
    fi

}

fetch_cluster_stats() {
    local cluster="$1"
    local extid="$2"
    local end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local start_time=$(date -u -v-30d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "30 days ago" +"%Y-%m-%dT%H:%M:%SZ")
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Fetching stats for $cluster ($extid)..."
    local response=$(curl -s -k -u "$USERNAME:$PASSWORD" "https://$PC_IP:$PORT/api/clustermgmt/v4.0/stats/clusters/${extid}?\$startTime=${start_time}&\$endTime=${end_time}&\$samplingInterval=604800&\$statType=MAX&\$select=cpuUsageHz,overallMemoryUsageBytes,storageUsageBytes")

    if echo "$response" | grep -q '"error"'; then
        echo "[WARN] Failed to fetch stats for $cluster"
        return
    fi

    local cpu_max=$(echo "$response" | tr -d '\n\r' | grep -o '"cpuUsageHz":[^]]*' | grep -o '"value":[0-9]*' | grep -o '[0-9]*' | sort -nr | head -1)
    local mem_max=$(echo "$response" | tr -d '\n\r' | grep -o '"overallMemoryUsageBytes":[^]]*' | grep -o '"value":[0-9]*' | grep -o '[0-9]*' | sort -nr | head -1)
    local storage_max=$(echo "$response" | tr -d '\n\r' | grep -o '"storageUsageBytes":[^]]*' | grep -o '"value":[0-9]*' | grep -o '[0-9]*' | sort -nr | head -1)

    if [[ -z "$cpu_max" && -z "$mem_max" && -z "$storage_max" ]]; then
        echo "[WARN] No stats found for $cluster"
        return
    fi

    # echo "${cluster},${cpu_max},${mem_max},${storage_max}" >> "$csv_file"
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

    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/storage-containers?\$filter=clusterExtId%20eq%20'${extid}'&\$select=name,erasureCode,cacheDeduplication,onDiskDedup,isCompressionEnabled")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response from API for $cluster"
        return
    fi

    echo "," >> "$csv_file"
    echo "Storage Containers,erasureCode,cacheDeduplication,onDiskDedup,isCompressionEnabled" >> "$csv_file"

    # Compact the JSON (remove newlines)
    local JSON_COMPACT
    JSON_COMPACT=$(echo "$RESPONSE" | tr -d '\n\r')

    # Parse all containers
    echo "$JSON_COMPACT" | awk '
        BEGIN {
            RS="\\{"
            FS="\n"
        }
        {
            # Match required fields
            if ($0 ~ /"name":/ && $0 ~ /"erasureCode":/ && $0 ~ /"cacheDeduplication":/ && $0 ~ /"onDiskDedup":/ && $0 ~ /"isCompressionEnabled":/) {

                match($0, /"name":"[^"]+"/)
                name = substr($0, RSTART+8, RLENGTH-9)

                match($0, /"erasureCode":"[^"]+"/)
                erasureCode = substr($0, RSTART+15, RLENGTH-16)

                # match($0, /"cacheDeduplication":"[^"]+"/)
                # cacheDedup = substr($0, RSTART+24, RLENGTH-25)
                # gsub(/"OFF"/, "OFF", cacheDedup)
                # gsub(/"ON"/, "ON", cacheDedup)

                match($0, /"cacheDeduplication":"[^"]+"/)
                cacheDedup = substr($0, RSTART, RLENGTH)         # Get the full matched string
                gsub(/.*:"/, "", cacheDedup)                     # Remove everything before colon + quote
                gsub(/"/, "", cacheDedup)                        # Remove ending quote


                match($0, /"onDiskDedup":"[^"]+"/)
                onDiskDedup = substr($0, RSTART+15, RLENGTH-16)
                gsub(/"OFF"/, "OFF", onDiskDedup)
                gsub(/"ON"/, "ON", onDiskDedup)

                match($0, /"isCompressionEnabled":[^,}]+/)
                compState = substr($0, RSTART+23, RLENGTH-23)
                gsub(/true/,"Enabled",compState)
                gsub(/false/,"Disabled",compState)
                gsub(/[:,}]/,"",compState)

                # Skip unwanted metadata
                if (name != "hasError" && name != "isPaginated" && name != "isTruncated") {
                    print name "," erasureCode "," cacheDedup "," onDiskDedup "," compState
                }
            }
        }
    ' >> "$csv_file"

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

    # === STEP 1: Fetch storage containers list ===
    local resp
    resp=$(curl -sk -u "${USERNAME}:${PASSWORD}" \
        "https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/storage-containers?\$filter=clusterExtId%20eq%20'${cluster_extID}'&\$select=name,containerExtId")

    if [[ -z "$resp" ]]; then
        echo "[WARN] Storage containers API returned empty response for cluster: $cluster_name"
        echo "[INFO] Wrote CSV header to $csv_file and exiting"
        return
    fi

    # === STEP 2: Extract all name + extId ===
    echo "[INFO] Extracting Containers from cluster $cluster_name"
    printf "%s" "$resp" | awk '
    BEGIN {
        in_data = 0
        brace = 0
        obj = ""
    }
    {
        line = $0
        gsub(/\r/, "", line)
        buffer = buffer line
    }
    END {
        start = index(buffer, "\"data\":[")
        if (!start) exit
        i = start + length("\"data\":[")
        while (i <= length(buffer)) {
            c = substr(buffer, i, 1)
            if (c == "{") {
                brace++
                obj = "{"
                i++
                while (i <= length(buffer) && brace > 0) {
                    ch = substr(buffer, i, 1)
                    obj = obj ch
                    if (ch == "{") brace++
                    if (ch == "}") brace--
                    i++
                }
                process(obj)
            }
            i++
        }
    }

    function process(o,   name, id, s, p, q) {
        key_name="\"name\":\""
        p = index(o, key_name)
        if (p > 0) {
            s = substr(o, p + length(key_name))
            q = index(s, "\"")
            name = substr(s, 1, q-1)
        }
        if (name == "hasError" || name == "isPaginated" || name == "isTruncated") return

        key_id="\"containerExtId\":\""
        p = index(o, key_id)
        if (p > 0) {
            s = substr(o, p + length(key_id))
            q = index(s, "\"")
            id = substr(s, 1, q-1)
        }

        if (name != "" && id != "")
            print name "," id
    }
    ' | while IFS=',' read -r ctr_name ctr_extid; do

        echo "[INFO] Processing container: $ctr_name"

        local endTime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        local startTime=$(date -u -v-7d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "7 days ago" +"%Y-%m-%dT%H:%M:%SZ")

        url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/stats/storage-containers/${ctr_extid}?\$samplingInterval=86400&\$startTime=${startTime}&\$endTime=${endTime}"

        stats=$(curl -sk -u "${USERNAME}:${PASSWORD}" "$url")
        if [[ -z "$stats" ]]; then
            echo "[WARN] No stats response for container $ctr_name ($ctr_extid)"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        values=$(echo "$stats" |
        sed -n 's/.*"controllerAvgIoLatencyuSecs":\[\(.*\)\],"controllerNumReadIops".*/\1/p' |
        grep -o '"value":[0-9]*' |
        sed 's/"value"://'
        )

        if [[ -z "$values" ]]; then
            echo "[WARN] No latency datapoints found for container $ctr_name"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        sum=0; count=0
        for v in $values; do
            # guard against non-numeric
            if [[ "$v" =~ ^[0-9]+$ ]]; then
                sum=$((sum + v))
                count=$((count + 1))
            fi
        done

        if [[ $count -eq 0 ]]; then
            echo "[WARN] All latency datapoints were non-numeric for $ctr_name"
            echo "${ctr_name},N/A,NO_DATA" >> "$csv_file"
            continue
        fi

        avg=$((sum / count))   # avg in microseconds

        # Convert avg from µs → ms accurately
        avg_ms=$(echo "scale=3; $avg / 1000" | bc)

        # Now check > 3 ms
        if (( $(echo "$avg_ms > 3" | bc -l) )); then
            status="**HIGH**"
            echo "[WARN] High average I/O latency for $ctr_name: ${avg_ms} ms"
        else
            status="OK"
            echo "[INFO] Average I/O latency for $ctr_name: ${avg_ms} ms (OK)"
        fi

        echo "${ctr_name},${avg_ms} ms,${status}" >> "$csv_file"

    done

    echo "[INFO] Completed container I/O latency collection for cluster: $cluster_name"
}

get_host_count() {
    local cluster="$1"
    local ext_id="$2"
    local csv_file="$OUTDIR/${cluster}.csv"

    if [[ -z "$ext_id" || "$ext_id" == "N/A" ]]; then
        echo "[WARN] Skipping host info for $cluster — No external IP found"
        return
    fi

    echo "[INFO] Fetching host info from $ext_id for $cluster..."
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/clusters/$ext_id/hosts?\$select=hostName,blockModel,maintenanceState,nodeStatus")

    if [[ -z "$RESPONSE" ]]; then
        echo "[ERROR] No response from $ext_id"
        return
    fi

    # Compact JSON to single line
    local compact_json
    compact_json=$(echo "$RESPONSE" | tr -d '\n\r')

    # Extract "totalAvailableResults"
    local total_hosts
    total_hosts=$(echo "$compact_json" | grep -o '"totalAvailableResults":[0-9]\+' | sed 's/"totalAvailableResults"://')
    echo "," >> "$csv_file"
    echo "total_hosts,${total_hosts}" >> "$csv_file"

    echo "[INFO] Total hosts in $cluster: $total_hosts"

    echo "$compact_json" | awk '
    {
        # find hostName anywhere
        print ","
        print "Hostname,Block Model,Maintenance State,Node Status"
        while (match($0, /"hostName":"[^"]+"/)) {
            host_name = substr($0, RSTART+12, RLENGTH-13)
            $0 = substr($0, RSTART + RLENGTH)  # move past hostName

            # search for blockModel in the remaining text
            if (match($0, /"blockModel":"[^"]+"/)) {
                block_model = substr($0, RSTART+13, RLENGTH-14)
                gsub(/"/, "", block_model)  # remove any stray quotes
                $0 = substr($0, RSTART + RLENGTH)
            } else {
                block_model = "**N/A**"
            }

            # search for maintenanceState
            if (match($0, /"maintenanceState":"[^"]+"/)) {
                maintenance_state = substr($0, RSTART+19, RLENGTH-20)
                gsub(/"/, "", maintenance_state)
                if (maintenance_state != "normal" && maintenance_state != "N/A") {
                    maintenance_state = "**" maintenance_state " **"
                }
                $0 = substr($0, RSTART + RLENGTH)
            } else {
                maintenance_state = "N/A"
            }

            # search for nodeStatus
            if (match($0, /"nodeStatus":"[^"]+"/)) {
                node_status = substr($0, RSTART+14, RLENGTH-15)
                gsub(/"/, "", node_status)
                if (node_status != "NORMAL" && node_status != "N/A") {
                    node_status = "**" node_status " **"
                } 
                $0 = substr($0, RSTART + RLENGTH)
            } else {
                node_status = "**N/A**"
            }

            # print all info in CSV format
            print host_name "," block_model "," maintenance_state "," node_status

            # reset variables
            host_name=""; block_model=""; maintenance_state=""; node_status=""
        }
        print ","
    }'  >> "$csv_file"

    echo "[INFO] Host details appended to $csv_file"
}

get_offline_disks() {
    local cluster="$1"
    local csv_file="$OUTDIR/${cluster}.csv"

    echo "[INFO] Fetching Offline disks for $cluster..."
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$PC_IP:$PORT/api/clustermgmt/v4.0/config/disks?\$filter=clusterName%20eq%20'$cluster'%20and%20diskAdvanceConfig/isOnline%20ne%20true&\$select=diskAdvanceConfig/isOnline")

    # Check totalAvailableResults
    local total
    total=$(echo "$RESPONSE" | awk -F':' '/totalAvailableResults/ {gsub(/[^0-9]/,"",$2); print $2}')
    if [[ -z "$total" || "$total" -eq 0 ]]; then
        echo "[INFO] No Offline disks found for $cluster"
        echo "Offline_Disks,0" >> "$csv_file"
        return
    fi

    # Compact JSON
    local compact_json
    compact_json=$(echo "$RESPONSE" | tr -d '\n\r')

    # Parse and append to CSV
    echo "$compact_json" | awk '
    {
        while (match($0, /"serialNumber":"[^"]+"/)) {
            disk_name = substr($0, RSTART+16, RLENGTH-17)
            $0 = substr($0, RSTART + RLENGTH)
            print disk_name ",Offline"
        }
    }' >> "$csv_file"

    echo "[INFO] Offline disk info appended to $csv_file"
}

get_license_details() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"
    local api_url="https://${PC_IP}:${PORT}/api/licensing/v4.0/config/entitlements?\$filter=clusterExtId%20eq%20'${cluster_extid}'"

    echo "[INFO] Fetching license details for $cluster_name..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response for License API ($cluster_name)"
        echo "License_Details,N/A" >> "$csv_file"
        return
    fi

    echo "," >> "$csv_file"
    echo "License_Name,Quantity (Meter),Expiry_Date" >> "$csv_file"

    # --- Parse all licenses using the working awk snippet ---
    echo "$response" | awk '
    BEGIN {
        RS=",";  # split JSON by commas
    }
    {
        if ($0 ~ /"details":\[/) in_details=1

        if (in_details && $0 ~ /"name":"/) {
            sub(/.*"name":"/, "", $0); sub(/".*/, "", $0)
            name=$0
        }

        if (in_details && $0 ~ /"quantity":/) {
            sub(/.*"quantity":/, "", $0); sub(/[,}].*/, "", $0)
            quantity=$0
        }

        if (in_details && $0 ~ /"meter":"/) {
            sub(/.*"meter":"/, "", $0); sub(/".*/, "", $0)
            meter=$0
        }

        if (in_details && $0 ~ /"earliestExpiryDate":"/) {
            sub(/.*"earliestExpiryDate":"/, "", $0); sub(/".*/, "", $0)
            expiry=$0
            if (name && quantity && meter && expiry && meter != "NODE") {
                printf "%s, %s (%s), %s\n", name, quantity, meter, expiry
                name=quantity=meter=expiry=""
            }
        }

        if (in_details && $0 ~ /\]\}/) in_details=0
    }' >> "$csv_file"

    echo "[INFO] License details appended for $cluster_name"
}

get_cpu_ratio() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Calculating vCPU:pCPU ratio for $cluster_name..."

    # Fetch hosts info
    local hosts_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts?\$select=numberOfCpuCores,numberOfCpuThreads"
    local hosts_response
    hosts_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$hosts_api_url")

    if [[ -z "$hosts_response" ]]; then
        echo "[WARN] Empty response for Hosts API ($cluster_name)"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    # Calculate total pCPU threads across all hosts
    local total_threads
    total_threads=$(echo "$hosts_response" | awk '
    BEGIN { sum=0 }
    {
    n = split($0, a, ",")
    for (i=1; i<=n; i++) {
        gsub(/[{}"]/,"",a[i])
        split(a[i], kv, ":")
        key = kv[1]; val = kv[2]+0
        if (key=="numberOfCpuThreads") sum += val
    }
    }
    END { print sum }')

    echo "[INFO] Total pCPU (all hosts): $total_threads"

    # Fetch all VMs for the cluster
    local vms_api_url="https://${PC_IP}:${PORT}/api/vmm/v4.0/ahv/config/vms?\$select=numSockets,numCoresPerSocket,numThreadsPerCore&\$filter=cluster/extId%20eq%20'${cluster_extid}'"
    local vms_response
    vms_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$vms_api_url")

    if [[ -z "$vms_response" ]]; then
        echo "[WARN] Empty response for VMs API ($cluster_name)"
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        return
    fi

    # Calculate total vCPUs from all VMs
    local total_vcpus
    total_vcpus=$(echo "$vms_response" | awk '
    BEGIN { sum=0 }
    {
        n = split($0, a, ",")
        for (i=1; i<=n; i++) {
            gsub(/[{}"]/,"",a[i])
            split(a[i], kv, ":")
            key = kv[1]; val = kv[2]+0
            if (key=="numSockets") sockets=val
            if (key=="numCoresPerSocket") cores=val
            if (key=="numThreadsPerCore") { threads=val; sum += sockets*cores*threads }
        }
    }
    END { print sum }')

    # Add global CVM vCPUs
    total_vcpus=$(( total_vcpus + total_cvm_vcpus ))
    echo "[INFO] Total vCPUs (User VMs + CVMs): $total_vcpus"

    # Compute vCPU:pThread ratio
    if [[ "$total_threads" -gt 0 ]]; then
        local ratio
        ratio=$(awk "BEGIN {printf \"%.2f\", $total_vcpus / $total_threads}")
        echo "vCPU_to_pCPU_Ratio,${ratio}:1" >> "$csv_file"
        echo "[INFO] vCPU:pCPU ratio for $cluster_name = ${ratio}:1"
    else
        echo "vCPU_to_pCPU_Ratio,N/A" >> "$csv_file"
        echo "[WARN] Cannot calculate ratio - total threads is zero"
    fi
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
        echo "Name,DirectoryType,URL,DomainName" >> "$csv_file"
        echo "N/A,N/A,N/A,N/A" >> "$csv_file"
        return
    fi

    # Write CSV header
    echo "," >> "$csv_file"
    echo "From Prism Central Directory Services," >> "$csv_file"
    echo "Name,DomainName,DirectoryType,URL" >> "$csv_file"

    # Loop over all entries in the data array
    echo "$response" | awk '
    BEGIN { inData=0; RS="\\{"; FS="," }
    /"data":\[/ { inData=1 } 
    inData {
        name=""; domain=""; dirType=""; url=""
        for(i=1;i<=NF;i++){
            gsub(/["\[\]{}]/,"",$i)
            split($i, kv, ":")
            key=kv[1]; val=kv[2]
            if(key=="name") name=val
            if(key=="domainName") domain=val
            if(key=="directoryType") dirType=val
            if(key=="url") {
                idx=index($i,":")
                url=substr($i, idx+1)
                gsub(/^ +| +$/,"",url)
            }
        }
        # Skip undesired names
        if(name!="" && name!="hasError" && name!="isPaginated") print name "," domain "," dirType "," url
    }' >> "$csv_file"

    echo "[INFO] Directory services saved to $csv_file"
}

get_hosts_and_nics() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching hosts for cluster $cluster_extid ..."

    local hosts_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts/?\$select=hostName,extId"
    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$hosts_api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response from Hosts API for cluster $cluster_extid"
        return
    fi

    # Loop over each host
    echo "$response" | awk '
    BEGIN { inData=0; RS="\\{"; FS="," }
    /"data":\[/ { inData=1 }
    inData {
        hostName=""; extId=""
        for(i=1;i<=NF;i++){
            gsub(/["\[\]{}]/,"",$i)
            split($i, kv, ":")
            key=kv[1]; val=kv[2]
            if(key=="hostName") hostName=val
            if(key=="extId") extId=val
        }
        if(hostName!="") print hostName, extId
    }' | while read -r host_name host_id; do

        echo "[INFO] Fetching NICs for host $host_name ($host_id)..."

        local nics_api_url="https://${PC_IP}:${PORT}/api/clustermgmt/v4.0/config/clusters/${cluster_extid}/hosts/${host_id}/host-nics?\$select=interfaceStatus,name"
        local nics_response
        nics_response=$(curl -sk -u "$USERNAME:$PASSWORD" "$nics_api_url")

        if [[ -z "$nics_response" ]]; then
            echo "[WARN] Empty response for host $host_name NICs"
            continue
        fi

        # Parse NICs and append to CSV
        echo "," >> "$csv_file"
        echo "NICs for Host: $host_name,Interfaces,Link Status" >> "$csv_file"
        echo "$nics_response" | awk -v hn="$host_name" -v hi="$host_id" '
            BEGIN { inData=0; RS="\\{"; FS="," }
            /"data":\[/ { inData=1 }
            inData {
                name=""; status=""
                for(i=1;i<=NF;i++){
                    gsub(/["\[\]{}]/,"",$i)
                    split($i, kv, ":")
                    key=kv[1]; val=kv[2]
                    if(key=="name") name=val
                    if(key=="interfaceStatus") {
                    if(val=="1") status="Connected"
                    else if(val=="0") status="**Not Connected**"
                    else status=val
        }
                }
                if(name!="" && name!="hasError" && name!="isPaginated" && name!="isTruncated")
                    print hn "," name "," status
            }' >> "$csv_file"

    done

    echo "[INFO] Host NICs saved to $csv_file"
}

# Global list
TITLE_PARAMS_LIST=()

get_alerts() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    # Alerts API URL
    local alerts_api_url="https://${PC_IP}:${PORT}/api/monitoring/v4.0/serviceability/alerts?\$filter=originatingClusterUUID%20eq%20'${cluster_extid}'%20and%20isResolved%20eq%20false&\$select=title,severity,rootCauseAnalysis"

    echo "[INFO] Fetching alerts for $cluster_name..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$alerts_api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty response for Alerts API ($cluster_name)"
        echo "," >> "$csv_file"
        echo "SEVERITY,TITLE,MESSAGE,CAUSE,RESOLUTION" >> "$csv_file"
        echo "N/A,N/A,N/A,N/A,N/A" >> "$csv_file"
        return
    fi

    # Add header separator
    echo "," >> "$csv_file"

    #
    # ---- extract placeholders from title into TITLE_PARAMS_LIST ----
    #
    while read -r key; do
        [[ -n "$key" ]] && TITLE_PARAMS_LIST+=("$key")
    done < <(
        echo "$response" \
        | sed 's/},{/}\n{/g' \
        | awk '
            BEGIN { IGNORECASE=1 }

            {
                l=$0

                # ---- extract {placeholders} from title ----
                if (l ~ /"title":"/) {

                    # Extract the whole title value
                    t=l
                    sub(/.*"title":"/,"",t)
                    sub(/".*/,"",t)

                    # BusyBox AWK-compatible placeholder extraction
                    rest=t
                    while ((start=index(rest,"{")) > 0) {
                        end=index(substr(rest,start+1),"}")
                        if (end == 0) break

                        key=substr(rest,start+1,end-1)
                        print key

                        rest=substr(rest,start+end+1)
                    }
                }

            }
        '
    )

    #
    # ---- ORIGINAL CSV PROCESSING BLOCK (unchanged) ----
    #
    echo "$response" \
    | sed 's/},{/}\n{/g' \
    | awk '
    BEGIN {
        print "SEVERITY,TITLE,CAUSE,RESOLUTION"
        warning=0; critical=0
    }
    {
        sev=""; tit=""; cau=""; res="";

        l=$0

        if (l ~ /"severity":"/) {
            x=l; sub(/.*"severity":"/, "", x); sub(/".*/, "", x); sev=x
        }

        if (l ~ /"title":"/) {
            x=l; sub(/.*"title":"/, "", x); sub(/".*/, "", x); tit=x
        }

        if (l ~ /"cause":"/) {
            x=l; sub(/.*"cause":"/, "", x); sub(/".*/, "", x); cau=x
        }

        if (l ~ /"resolution":"/) {
            x=l; sub(/.*"resolution":"/, "", x); sub(/".*/, "", x); res=x
        }

        # Skip INFO severity
        if (sev != "" && sev != "INFO") {
            gsub(/,/, ";", sev)
            gsub(/,/, ";", tit)
            gsub(/,/, ";", cau)
            gsub(/,/, ";", res)
            print sev "," tit "," cau "," res
            print ","
            if (sev=="WARNING") warning++
            if (sev=="CRITICAL") critical++
        }
    }
    END {
        print "Total WARNING alerts:", warning
        print "Total CRITICAL alerts:", critical
    }
    ' >> "$csv_file"
    echo "[INFO] Alerts appended for $cluster_name"
    echo "[INFO] Title params list ${TITLE_PARAMS_LIST[@]}"
}

get_alert_parameters() {
    local cluster_name="$1"
    local cluster_extid="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    local alerts_api_url="https://${PC_IP}:${PORT}/api/monitoring/v4.0/serviceability/alerts?\$filter=originatingClusterUUID%20eq%20'${cluster_extid}'%20and%20isResolved%20eq%20false&\$select=parameters"

    echo "[INFO] Fetching alert parameters for $cluster_name..."

    local response
    response=$(curl -sk -u "$USERNAME:$PASSWORD" "$alerts_api_url")

    if [[ -z "$response" ]]; then
        echo "[WARN] Empty Parameter response for $cluster_name"
        TITLE_PARAMS_LIST=()
        return
    fi

    ##############################################################################
    # 1. NORMALIZE + REMOVE DUPLICATES IN TITLE PARAM LIST (macOS-safe)
    ##############################################################################
    local DEDUPED_TITLE_LIST=()
    local k kn
    for k in "${TITLE_PARAMS_LIST[@]}"; do
        kn=$(printf "%s" "$k" | tr '[:upper:]' '[:lower:]' | xargs)

        if ! printf "%s\n" "${DEDUPED_TITLE_LIST[@]}" | grep -qx -- "$kn"; then
            DEDUPED_TITLE_LIST+=( "$kn" )
        fi
    done

    TITLE_PARAMS_LIST=( "${DEDUPED_TITLE_LIST[@]}" )

    ##############################################################################
    # 2. Extract all raw params
    ##############################################################################
    RAW_PAIRS=()
    while IFS= read -r line; do
        RAW_PAIRS+=( "$line" )
    done < <(
        echo "$response" \
        | sed 's/},{/}\n{/g' \
        | awk '
            BEGIN { IGNORECASE=1 }
            {
                param=""; l=$0

                if (l ~ /"paramName":"/) {
                    x=l; sub(/.*"paramName":"/,"",x); sub(/".*/,"",x)
                    param=x
                }

                if (l ~ /"stringValue":"/ && param != "") {
                    if (param ~ /version/ || param ~ /id/) { param=""; next }
                    v=l; sub(/.*"stringValue":"/,"",v); sub(/".*/,"",v)
                    gsub(/,/, ";", param)
                    gsub(/,/, ";", v)
                    print param "|" v
                    param=""
                }
            }
        '
    )

    ##############################################################################
    # 3. DE-DUPLICATE RAW_PAIRS (macOS-safe, preserves order)
    ##############################################################################
    UNIQUE_PAIRS=()
    for pair in "${RAW_PAIRS[@]}"; do
        if ! printf "%s\n" "${UNIQUE_PAIRS[@]}" | grep -qx -- "$pair"; then
            UNIQUE_PAIRS+=( "$pair" )
        fi
    done

    ##############################################################################
    # 4. ORDERED OUTPUT (old behavior preserved: list all matching values)
    ##############################################################################
    ORDERED_OUTPUT=()

    local key pname pval pname_norm key_norm
    for key in "${TITLE_PARAMS_LIST[@]}"; do
        key_norm=$(printf "%s" "$key" | tr '[:upper:]' '[:lower:]' | xargs)

        # Collect ALL matching values
        for pair in "${UNIQUE_PAIRS[@]}"; do
            pname="${pair%%|*}"
            pval="${pair#*|}"
            pname_norm=$(printf "%s" "$pname" | tr '[:upper:]' '[:lower:]' | xargs)

            if [[ "$pname_norm" == "$key_norm" ]]; then
                ORDERED_OUTPUT+=( "${pname},${pval}" )
            fi
        done
    done

    ##############################################################################
    # 5. Write to CSV
    ##############################################################################
    echo "," >> "$csv_file"
    echo "PARAMETER_NAME,PARAMETER_VALUE" >> "$csv_file"

    for entry in "${ORDERED_OUTPUT[@]}"; do
        echo "$entry" >> "$csv_file"
    done

    echo "[INFO] Ordered parameters appended for $cluster_name"

    TITLE_PARAMS_LIST=()
}


# v2.0 

get_cluster_security_config() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching Security & Compliance config for $cluster_name..."

    # --- API Call ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$ext_ip:$PORT/api/nutanix/v2.0/cluster")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "Security_Config,**N/A**" >> "$csv_file"
        return
    fi

    # Compact JSON
    local JSON
    JSON=$(echo "$RESPONSE" | tr -d '\n\r')

    # --- Extract Parameters ---

    # is_registered_to_pc
    pc_status=$(echo "$JSON" | grep -o '"is_registered_to_pc"[^,}]*' | sed -E 's/.*:[[:space:]]*"?([^"}]*)"?/\1/')

    if [[ -z "$pc_status" ]]; then
        pc_status="N/A"
    elif [[ "$pc_status" == "true" ]]; then
        pc_status="Connected"
    elif [[ "$pc_status" == "false" ]]; then
        pc_status="Not Connected"
    fi


    # enable_rebuild_reservation
    local rebuild_reservation
    rebuild_reservation=$(echo "$JSON" | sed -n 's/.*"enable_rebuild_reservation":[[:space:]]*\(true\|false\).*/\1/p')
    [[ -z "$rebuild_reservation" ]] && rebuild_reservation="N/A"
    [[ "$rebuild_reservation" == "true" ]] && rebuild_reservation="Enabled" || rebuild_reservation="**Disabled**"

    # disable_degraded_node_monitoring
    local degraded_monitor
    degraded_monitor=$(echo "$JSON" | sed -n 's/.*"disable_degraded_node_monitoring":[[:space:]]*\(true\|false\).*/\1/p')
    [[ -z "$degraded_monitor" ]] && degraded_monitor="**N/A**"
    [[ "$degraded_monitor" == "true" ]] && degraded_monitor="**Disabled**" || degraded_monitor="Enabled"

    # recycle_bin_dto.recycle_bin_ttlsecs
    local recycle_bin_ttl
    recycle_bin_ttl=$(echo "$JSON" | sed -n 's/.*"recycle_bin_ttlsecs":[[:space:]]*\([0-9]*\).*/\1/p')
    if [[ -z "$recycle_bin_ttl" || "$recycle_bin_ttl" -le 0 ]]; then
        recycle_bin_status="**Disabled**"
    else
        recycle_bin_status="Enabled"
    fi

    # --- security_compliance_config subfields ---
    local schedule
    schedule=$(echo "$JSON" | sed -n 's/.*"security_compliance_config":[^{]*{[^}]*"schedule":[[:space:]]*"\([^"]*\)".*/\1/p')
    [[ -z "$schedule" ]] && schedule="**N/A**"

    local aide
    aide=$(echo "$JSON" | sed -n 's/.*"enable_aide":[[:space:]]*\(true\|false\).*/\1/p')
    [[ "$aide" == "true" ]] && aide="Enabled" || aide="**Disabled**"

    local core
    core=$(echo "$JSON" | sed -n 's/.*"enable_core":[[:space:]]*\(true\|false\).*/\1/p')
    [[ "$core" == "true" ]] && core="Enabled" || core="**Disabled**"

    local high_strength
    high_strength=$(echo "$JSON" | sed -n 's/.*"enable_high_strength_password":[[:space:]]*\(true\|false\).*/\1/p')
    [[ "$high_strength" == "true" ]] && high_strength="Enabled" || high_strength="**Disabled**"

    local banner
    banner=$(echo "$JSON" | sed -n 's/.*"enable_banner":[[:space:]]*\(true\|false\).*/\1/p')
    [[ "$banner" == "true" ]] && banner="Enabled" || banner="**Disabled**"

    # --- Append to CSV ---
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

    # --- WARNINGS ---
    [[ "$rebuild_reservation" == "Disabled" ]] && echo "[WARN] Rebuild reservation is DISABLED on $cluster_name"
    [[ "$aide" == "Disabled" ]] && echo "[WARN] AIDE DISABLED on $cluster_name"
    [[ "$core" == "Disabled" ]] && echo "[WARN] Core compliance DISABLED on $cluster_name"
    [[ "$high_strength" == "Disabled" ]] && echo "[WARN] High-strength password policy DISABLED on $cluster_name"
    [[ "$banner" == "Disabled" ]] && echo "[WARN] Welcome banner DISABLED on $cluster_name"
    [[ "$degraded_monitor" == "Disabled" ]] && echo "[WARN] Degraded node detection is DISABLED on $cluster_name"
    [[ "$recycle_bin_status" == "Disabled" ]] && echo "[WARN] Recycle bin is DISABLED on $cluster_name"
}

get_snapshots_info() {
    local cluster_name="$1"
    local ext_ip="$2"
    local csv_file="$OUTDIR/${cluster_name}.csv"

    echo "[INFO] Fetching Snapshots info for $cluster_name..."

    # --- API Call ---
    local RESPONSE
    RESPONSE=$(curl -s -k -u "$USERNAME:$PASSWORD" \
        "https://$ext_ip:$PORT/api/nutanix/v2.0/snapshots/")

    if [[ -z "$RESPONSE" ]]; then
        echo "[WARN] No response received from $cluster_name"
        echo "Snapshots,N/A" >> "$csv_file"
        return
    fi

    # --- Extract total_entities ---
    local total
    total=$(echo "$RESPONSE" | grep -o '"total_entities":[0-9]*' | head -n1 | cut -d':' -f2)

    if [[ -z "$total" || "$total" -eq 0 ]]; then
        echo "[INFO] No snapshots found for $cluster_name"
        echo "total_snapshots_in_cluster,0" >> "$csv_file"
        return
    fi

    echo "[INFO] Total snapshots found: $total"

    # --- Compact JSON for parsing ---
    local JSON
    JSON=$(echo "$RESPONSE" | tr -d '\n\r')

    echo "," >> "$csv_file"
    echo "VM_Name,Snapshot_Names" >> "$csv_file"

    # --- Parse snapshot_name and VM name ---
    echo "$JSON" | awk '
    BEGIN { RS="\\},\\{" }
    {
        if ($0 ~ /"snapshot_name":/ && $0 ~ /"vm_create_spec":/) {
            if (match($0, /"snapshot_name":"[^"]+"/))
                snap = substr($0, RSTART+17, RLENGTH-18)
            if (match($0, /"name":"[^"]+"/))
                vm = substr($0, RSTART+8, RLENGTH-9)
            if (vm != "" && snap != "") {
                snapshots[vm] = snapshots[vm] ? snapshots[vm] "," snap : snap
            }
            vm = snap = ""
        }
    }
    END {
        for (v in snapshots)
            print v "," snapshots[v]
    }

    ' >> "$csv_file"

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

    # --- Extract ha_state value ---
    local ha_state
    ha_state=$(echo "$RESPONSE" | grep -o '"ha_state":"[^"]*"' | cut -d'"' -f4)

    if [[ -z "$ha_state" ]]; then
        echo "[WARN] Unable to extract ha_state for $cluster_name"
        echo "ha_state,N/A" >> "$csv_file"
        return
    fi

    echo "[INFO] HA State for $cluster_name: $ha_state"
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

    # Compact JSON (remove newlines for easy parsing)
    local JSON
    JSON=$(echo "$RESPONSE" | tr -d '\n\r')

    # --- Extract email status ---
    local smtp_status
    smtp_status=$(echo "$JSON" | grep -o '"status"[^,}]*' | sed -E 's/.*:[[:space:]]*"?([^"}]*)"?/\1/')

    if [[ -z "$smtp_status" ]]; then
        smtp_status="**N/A**"
    elif [[ "$smtp_status" == "SUCCESS" ]]; then
        smtp_status="Success"
    else
        smtp_status="**Failed**"
    fi

    # --- Append to CSV ---
    {
        echo "SMTP_Status,$smtp_status"
    } >> "$csv_file"

    echo "[INFO] SMTP configuration status appended to $csv_file"

    # --- WARNINGS ---
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

    # Loop over directory_list array
    echo "$response" | awk '
    BEGIN { inData=0; RS="\\{"; FS="," }
    /"directory_list":\[/ { inData=1 } 
    inData {
        name=""; domain=""; dirType=""; url=""
        for(i=1;i<=NF;i++){
            gsub(/["\[\]{}]/,"",$i)
            split($i, kv, ":")
            key=kv[1]; val=kv[2]
            if(key=="name") name=val
            if(key=="domain") domain=val
            if(key=="directory_type") dirType=val
            if(key=="directory_url") {
                idx=index($i,":")
                url=substr($i, idx+1)
                gsub(/^ +| +$/,"",url)
            }
        }
        if(name!="") print name "," domain "," dirType "," url
    }' >> "$csv_file"

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
        return
    fi

    # AWK will calculate total vCPUs
    local cvm_config
    cvm_config=$(echo "$response" | awk '
    BEGIN {
        RS="{"; FS="\n"; GIB=1073741824; total_vcpus=0;
    }
    {
        block=$0
        if (block ~ /"vmName"/) {
            name=""; mem=""; cpu=""; mem_gib=""
            if (block ~ /"vmName"[ ]*:/) {
                tmp=block; sub(/.*"vmName"[ ]*:[ ]*"/,"",tmp); sub(/".*/,"",tmp); name=tmp
            }
            if (block ~ /"memoryCapacityInBytes"[ ]*:/) {
                tmp=block; sub(/.*"memoryCapacityInBytes"[ ]*:[ ]*/,"",tmp); sub(/[,}].*/,"",tmp)
                mem_bytes = tmp + 0; mem_gib = mem_bytes / GIB
            }
            if (block ~ /"numVCpus"[ ]*:/) {
                tmp=block; sub(/.*"numVCpus"[ ]*:[ ]*/,"",tmp); sub(/[,}].*/,"",tmp); cpu=tmp
            }
            if (name != "" && cpu != "" && mem_gib != "") {
                printf "%s,%.2f GiB,%s\n", name, mem_gib, cpu
                total_vcpus += cpu
            }
        }
    }
    END { print total_vcpus }')

    # Split CSV and total vCPUs
    echo "$response" | awk '
    BEGIN { RS="{"; FS="\n"; GIB=1073741824 }
    {
        block=$0
        if (block ~ /"vmName"/) {
            name=""; mem=""; cpu=""; mem_gib=""
            if (block ~ /"vmName"[ ]*:/) { tmp=block; sub(/.*"vmName"[ ]*:[ ]*"/,"",tmp); sub(/".*/,"",tmp); name=tmp }
            if (block ~ /"memoryCapacityInBytes"[ ]*:/) { tmp=block; sub(/.*"memoryCapacityInBytes"[ ]*:[ ]*/,"",tmp); sub(/[,}].*/,"",tmp); mem_bytes = tmp+0; mem_gib=mem_bytes/GIB }
            if (block ~ /"numVCpus"[ ]*:/) { tmp=block; sub(/.*"numVCpus"[ ]*:[ ]*/,"",tmp); sub(/[,}].*/,"",tmp); cpu=tmp }
            if (name != "" && cpu != "" && mem_gib != "") { print name "," mem_gib " GiB," cpu }
        }
    }' >> "$csv_file"

    total_cvm_vcpus=$(echo "$response" | awk '
    BEGIN { RS="{"; FS="\n"; total=0 }
    /"vmName"/ && /CVM/ {
        for(i=1;i<=NF;i++){
            if ($i ~ /"numVCpus"/) {
                line=$i
                sub(/.*"numVCpus"[ ]*:[ ]*/, "", line)
                sub(/[,}].*/, "", line)
                total += line
            }
        }
    }
    END { print total }')
}