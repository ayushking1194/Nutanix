#!/bin/bash
# Enter your Prism Central IP and credentials
PC_IP="<pc_ip>"
USERNAME="<username>"
PASSWORD="<password>"
# Get UTC time 1 minute ago in ISO 8601 format
if date --version >/dev/null 2>&1; then
  ONE_MIN_AGO=$(date -u -d "-1 minute" +"%Y-%m-%dT%H:%M:%SZ")
else
  ONE_MIN_AGO=$(date -u -v -1M +"%Y-%m-%dT%H:%M:%SZ")
fi
# Construct filtered API URL (server-side filtering)
FILTER="\$filter=creationTime%20ge%20$ONE_MIN_AGO"
EVENTS_URL="https://${PC_IP}:9440/api/monitoring/v4.0/serviceability/events?$FILTER"
# Fetch events filtered by server
RESPONSE=$(curl -sk -u "$USERNAME:$PASSWORD" -H "Accept: application/json" "$EVENTS_URL")
# Parse response for each relevant message
echo "$RESPONSE" | tr '{' '\n' | grep '"creationTime"' | while read -r block; do
  CREATION_TIME=$(echo "$block" | grep -oP '"creationTime"\s*:\s*"\K[^"]+')
  MESSAGE=$(echo "$block" | grep -oP '"message"\s*:\s*"\K[^"]+')
  if [[ -n "$CREATION_TIME" && -n "$MESSAGE" ]]; then
    # Apply message filters: contains "logged", does NOT contain "v3"
    if [[ "${MESSAGE,,}" == *"logged"* ]] && [[ "${MESSAGE,,}" != *"v3"* ]]; then
      # Format timestamp for string3 field
      FORMATTED_TIME=$(echo "$CREATION_TIME" | sed -E 's/T([0-9]{2}:[0-9]{2}:[0-9]{2}).*/ \1/' | sed 's/Z//' | sed 's/T/ /')
      # Send email using sendmail
      (
      echo "To: <email@example.com>" # Replace with desired email address
      echo "Subject: Nutanix Event Notification"
      echo "From: testing_mail@nutanix.com"
      echo
      echo "Message: $MESSAGE"
      echo "Time: $FORMATTED_TIME"
      ) | /usr/sbin/sendmail -t
    else
      echo "Event message did not match filters"
    fi
  fi
done

# Make the script executable 
# chmod +x /full/path/to/user_login_notifier.sh

# Run the script
# ./full/path/to/user_login_notifier.sh

# Create a cron job using the following command to send a mail everytime someone logs in
# * * * * * /full/path/to/user_login_notifier.sh
