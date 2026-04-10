#!/bin/bash
set -e

echo "Setting up ARIA on VPS..."

# Update system
apt-get update -q
apt-get install -y python3 python3-pip git nodejs npm

# Install PM2
npm install -g pm2

# Create app directory
mkdir -p /var/www/aria
mkdir -p /var/log/aria

# Clone repo (run manually with your repo URL)
# git clone https://github.com/YOUR_USERNAME/DSE_ARIA.git /var/www/aria

# Install Python dependencies
cd /var/www/aria
pip3 install -r requirements.txt --break-system-packages

# Create .env file (fill in values manually after)
cat > /var/www/aria/.env << 'EOF'
DATABASE_URL=
DEEPSEEK_API_KEY=
TELEGRAM_BOT_TOKEN=
EOF

echo "Edit /var/www/aria/.env with your credentials"
echo ""

# Start with PM2
cd /var/www/aria
pm2 start ecosystem.config.js
pm2 save
pm2 startup

echo "Setup complete. API running on port 8000."
echo "Check status: pm2 status"
echo "Check logs: pm2 logs aria-api"
