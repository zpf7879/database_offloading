#!/usr/bin/env bash
# =============================================================
# Week 1 Setup Script — Database Offloading POC
# Run this on a fresh Amazon Linux 2023 EC2 instance.
# =============================================================

set -euo pipefail

REPO_URL="https://github.com/zpf7879/database_offloading.git"
PROJECT_DIR="$HOME/database_offloading"
CONNECT_URL="http://localhost:8083"

echo ""
echo "============================================="
echo " Database Offloading POC — Week 1 Setup"
echo "============================================="
echo ""

# -------------------------------------------------------------
# STEP 1: Install Docker
# -------------------------------------------------------------
echo "[1/8] Installing Docker..."
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
echo "      Docker installed. NOTE: group change takes effect on next login."

# -------------------------------------------------------------
# STEP 2: Install Docker Compose plugin
# -------------------------------------------------------------
echo "[2/8] Installing Docker Compose plugin..."
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
echo "      Docker Compose $(docker compose version --short) installed."

# -------------------------------------------------------------
# STEP 3: Install Docker Buildx plugin
# -------------------------------------------------------------
echo "[3/8] Installing Docker Buildx plugin..."
sudo mkdir -p /usr/local/lib/docker/cli-plugins
BUILDX_VERSION=$(curl -s https://api.github.com/repos/docker/buildx/releases/latest \
  | grep '"tag_name"' | cut -d'"' -f4)
sudo curl -SL "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" \
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
echo "      Docker Buildx $(docker buildx version) installed."

# -------------------------------------------------------------
# STEP 4: Install Node.js 20 via nvm
# -------------------------------------------------------------
echo "[4/8] Installing Node.js 20 via nvm..."
if [ ! -d "$HOME/.nvm" ]; then
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
fi
export NVM_DIR="$HOME/.nvm"
# shellcheck source=/dev/null
source "$NVM_DIR/nvm.sh"
nvm install 20
nvm use 20
nvm alias default 20
echo "      Node $(node --version) installed."

# -------------------------------------------------------------
# STEP 5: Clone repo and configure environment
# -------------------------------------------------------------
echo "[5/8] Cloning repository..."
if [ -d "$PROJECT_DIR" ]; then
  echo "      Directory already exists — pulling latest changes."
  git -C "$PROJECT_DIR" pull
else
  git clone "$REPO_URL" "$PROJECT_DIR"
fi
cd "$PROJECT_DIR"

if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo "  *** ACTION REQUIRED ***"
  echo "  Edit .env and set your MONGODB_URI before continuing."
  echo "  Run: nano $PROJECT_DIR/.env"
  echo ""
  read -rp "  Press ENTER once you have saved your .env file..."
fi

# -------------------------------------------------------------
# STEP 6: Install Node dependencies
# -------------------------------------------------------------
echo "[6/8] Installing npm dependencies..."
npm install
echo "      npm install complete."

# -------------------------------------------------------------
# STEP 7: Build custom Kafka Connect image + start Docker stack
# -------------------------------------------------------------
echo "[7/8] Building custom Kafka Connect image and starting Docker stack..."
echo "      This downloads Debezium + MongoDB connector JARs (~2-3 min first time)."

# Docker socket may require a new shell session if user was just added to docker group
if ! docker info > /dev/null 2>&1; then
  echo "      Cannot reach Docker socket. Re-running with sg docker..."
  exec sg docker "$0 $*"
fi

docker compose build kafka-connect
docker compose up -d
echo "      Containers started."

# -------------------------------------------------------------
# STEP 8: Register Debezium (source) + MongoDB Kafka (sink) connectors
# -------------------------------------------------------------
echo "[8/8] Waiting for Kafka Connect to be ready..."
MAX_WAIT=180
WAITED=0
until curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "ERROR: Kafka Connect did not become ready after ${MAX_WAIT}s."
    echo "       Check logs with: docker compose logs kafka-connect --tail=50"
    exit 1
  fi
  printf "."
  sleep 5
  WAITED=$((WAITED + 5))
done
echo ""
echo "      Kafka Connect is ready."
echo "      Registering connectors..."
bash "$PROJECT_DIR/scripts/register-connectors.sh"

# -------------------------------------------------------------
# Done
# -------------------------------------------------------------
echo ""
echo "============================================="
echo " Week 1 Setup Complete!"
echo "============================================="
echo ""
echo " Data pipeline is live:"
echo "   MySQL → Debezium → Kafka → MongoDB Atlas"
echo ""
echo " Next steps (run each in a separate terminal):"
echo "   Start profile merger: npm run merger   <- merges staging collections into customer_profile"
echo "   Start read API:       npm run api"
echo "   Seed more data:       npm run seed -- --count=5000"
echo "   Baseline load test:  npm run load:baseline"
echo "   MongoDB load test:   npm run load:mongo"
echo ""
echo " Kafka UI:   http://<your-ec2-ip>:8080"
echo " Read API:   http://<your-ec2-ip>:3000"
echo ""
echo " Check connector status:"
echo "   curl -s http://localhost:8083/connectors/poc-mysql-connector/status | python3 -m json.tool"
echo "   curl -s http://localhost:8083/connectors/mongodb-sink-connector/status | python3 -m json.tool"
echo ""
