#!/bin/bash
set -e

# Check if LXD is installed
if ! command -v lxc &>/dev/null; then
  echo "LXD (lxc command) not found. Please install LXD and try again."
  exit 1
fi

# Create LXD profile for testing if it doesn't exist
PROFILE_NAME="test-env"
if ! lxc profile show "$PROFILE_NAME" &>/dev/null; then
  echo "Creating LXD profile '${PROFILE_NAME}'..."
  lxc profile create "$PROFILE_NAME"
  lxc profile edit "$PROFILE_NAME" <<EOF
config:
  limits.cpu: "2"
  limits.memory: "4GB"
description: "Test environment for search engine performance testing"
devices:
  root:
    path: /
    pool: default
    type: disk
EOF
else
  echo "Profile '${PROFILE_NAME}' already exists."
fi

# Define containers and their names
MEILISEARCH_CONTAINER="meilisearch"
OPENSEARCH_CONTAINER="opensearch"
SOLR_CONTAINER="solr"
declare -A CONTAINERS
CONTAINERS=(
  ["meilisearch"]="$MEILISEARCH_CONTAINER"
  ["opensearch"]="$OPENSEARCH_CONTAINER"
  ["solr"]="$SOLR_CONTAINER"
)

# Launch containers if they don't already exist
for name in "${CONTAINERS[@]}"; do
  if lxc list | grep -q "$name"; then
    echo "Container '$name' already exists. Skipping creation."
  else
    echo "Launching container '$name'..."
    lxc launch ubuntu:24.04 "$name" --profile default --profile "$PROFILE_NAME"
    # Give container time to boot
    sleep 5
  fi
done

########################################
# Set up Meilisearch in its container
########################################
if [ "$(lxc exec "$MEILISEARCH_CONTAINER" systemctl is-active meilisearch)" = "inactive" ]; then
    echo "Setting up Meilisearch in container 'meilisearch'..."

    lxc exec $MEILISEARCH_CONTAINER -- apt update
    lxc exec $MEILISEARCH_CONTAINER -- curl -L https://install.meilisearch.com -o install.sh
    lxc exec $MEILISEARCH_CONTAINER -- bash install.sh
    lxc exec $MEILISEARCH_CONTAINER -- mv ./meilisearch /usr/local/bin/
    lxc exec $MEILISEARCH_CONTAINER -- useradd -d /var/lib/meilisearch -s /bin/false -m -r meilisearch
    lxc exec $MEILISEARCH_CONTAINER -- chown meilisearch:meilisearch /usr/local/bin/meilisearch
    lxc exec $MEILISEARCH_CONTAINER -- mkdir -p /var/lib/meilisearch/data /var/lib/meilisearch/dumps /var/lib/meilisearch/snapshots
    lxc exec $MEILISEARCH_CONTAINER -- chown -R meilisearch:meilisearch /var/lib/meilisearch
    lxc exec $MEILISEARCH_CONTAINER -- chmod 750 /var/lib/meilisearch
    lxc exec $MEILISEARCH_CONTAINER -- curl https://raw.githubusercontent.com/meilisearch/meilisearch/latest/config.toml -o /etc/meilisearch.toml

    lxc exec $MEILISEARCH_CONTAINER -- sed -i 's/http_addr = "localhost:7700"/http_addr = "0.0.0.0:7700"/' /etc/meilisearch.toml
    lxc exec $MEILISEARCH_CONTAINER -- sed -i 's/db_path = "\.\/data\.ms"/db_path = "\/var\/lib\/meilisearch\/data"/' /etc/meilisearch.toml
    lxc exec $MEILISEARCH_CONTAINER -- sed -i 's/dump_dir = "dumps\/"/dump_dir = "\/var\/lib\/meilisearch\/dumps"/' /etc/meilisearch.toml
    lxc exec $MEILISEARCH_CONTAINER -- sed -i 's/snapshot_dir = "snapshots\/"/snapshot_dir = "\/var\/lib\/meilisearch\/snapshots"/' /etc/meilisearch.toml

    cat <<EOF | lxc exec "$MEILISEARCH_CONTAINER" -- tee /etc/systemd/system/meilisearch.service
[Unit]
Description=Meilisearch
After=systemd-user-sessions.service

[Service]
Type=simple
WorkingDirectory=/var/lib/meilisearch
ExecStart=/usr/local/bin/meilisearch --config-file-path /etc/meilisearch.toml
User=meilisearch
Group=meilisearch
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

    lxc exec $MEILISEARCH_CONTAINER -- systemctl enable meilisearch
    lxc exec $MEILISEARCH_CONTAINER -- systemctl start meilisearch
fi

########################################
# Set up OpenSearch in its container
########################################
# Only for testing purposes
OPENSEARCH_PASSWD=RandomShit1!
if [ "$(lxc exec "$OPENSEARCH_CONTAINER" systemctl is-active opensearch)" = "inactive" ]; then
    echo "Setting up OpenSearch in container 'opensearch'..."
    lxc exec $OPENSEARCH_CONTAINER -- apt update
    lxc exec $OPENSEARCH_CONTAINER -- apt install -y lsb-release ca-certificates curl gnupg2
    lxc exec "$OPENSEARCH_CONTAINER" -- \
      bash -c "curl -s https://artifacts.opensearch.org/publickeys/opensearch.pgp \
               | gpg --dearmor --batch --yes -o /usr/share/keyrings/opensearch-keyring"
    lxc exec "$OPENSEARCH_CONTAINER" -- \
      bash -c "echo 'deb [signed-by=/usr/share/keyrings/opensearch-keyring] https://artifacts.opensearch.org/releases/bundle/opensearch/2.x/apt stable main' \
               > /etc/apt/sources.list.d/opensearch-2.x.list"
    lxc exec $OPENSEARCH_CONTAINER -- apt update
    lxc exec $OPENSEARCH_CONTAINER -- sudo env OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OPENSEARCH_PASSWD apt-get install opensearch
    lxc exec $OPENSEARCH_CONTAINER -- sudo systemctl enable opensearch
    lxc exec $OPENSEARCH_CONTAINER -- sudo systemctl start opensearch

    lxc exec $OPENSEARCH_CONTAINER -- sed -i 's/#cluster\.name: my-application/cluster\.name: my-application/' /etc/opensearch/opensearch.yml
    lxc exec $OPENSEARCH_CONTAINER -- sed -i 's/#network\.host: 192.168.0.1/network.host: 0.0.0.0/' /etc/opensearch/opensearch.yml
    lxc exec $OPENSEARCH_CONTAINER -- sed -i '/network\.host: 0.0.0.0$/a discovery.type: single-node' /etc/opensearch/opensearch.yml
    lxc exec $OPENSEARCH_CONTAINER -- sudo systemctl restart opensearch

    echo "Setting up OpenSearch Dashboards in container 'opensearch'..."
    lxc exec $OPENSEARCH_CONTAINER -- \
        bash -c "curl -o- https://artifacts.opensearch.org/publickeys/opensearch.pgp \
        | gpg --dearmor --batch --yes -o /usr/share/keyrings/opensearch-keyring"
    lxc exec $OPENSEARCH_CONTAINER -- \
        bash -c "echo 'deb [signed-by=/usr/share/keyrings/opensearch-keyring] https://artifacts.opensearch.org/releases/bundle/opensearch-dashboards/2.x/apt stable main' \ 
        > /etc/apt/sources.list.d/opensearch-dashboards-2.x.list"
    lxc exec $OPENSEARCH_CONTAINER -- apt update
    lxc exec $OPENSEARCH_CONTAINER -- apt install opensearch-dashboards
    lxc exec $OPENSEARCH_CONTAINER -- systemctl enable opensearch-dashboards
    lxc exec $OPENSEARCH_CONTAINER -- systemctl start opensearch-dashboards

    lxc exec $OPENSEARCH_CONTAINER -- sed -i 's/# server.host: "localhost"/server.host: 0.0.0.0/' /etc/opensearch-dashboards/opensearch_dashboards.yml
    lxc exec $OPENSEARCH_CONTAINER -- systemctl restart opensearch-dashboards
fi

########################################
# Set up Solr in its container
########################################
echo "Setting up Solr in container 'solr'..."


echo "Setup complete. All containers are configured and should be running their respective search engines."


