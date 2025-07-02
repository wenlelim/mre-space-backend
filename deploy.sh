#!/bin/bash

# MRE Enhancement Backend Deployment Script
# This script sets up the complete backend environment including Redis

set -e

# Configuration - UPDATE THESE FOR YOUR ENVIRONMENT
SERVER_IP="10.135.128.194"
BACKEND_PORT="5001"
REDIS_PORT="6379"
REDIS_PASSWORD="your_redis_password_here"  # Change this to a secure password
DB_HOST="localhost"
DB_NAME="mre_enhancement"
DB_USER="your_db_user"
DB_PASSWORD="your_db_password"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting MRE Enhancement Backend Deployment...${NC}"

# Function to print status
print_status() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Update system packages
print_status "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install required system packages
print_status "Installing required system packages..."
sudo apt install -y python3 python3-pip python3-venv redis-server postgresql postgresql-contrib nginx git curl

# Start and enable Redis
print_status "Configuring Redis..."
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Configure Redis with password
print_status "Setting up Redis authentication..."
sudo sed -i "s/# requirepass foobared/requirepass $REDIS_PASSWORD/" /etc/redis/redis.conf
sudo systemctl restart redis-server

# Test Redis connection
print_status "Testing Redis connection..."
redis-cli -a $REDIS_PASSWORD ping

# Setup PostgreSQL
print_status "Setting up PostgreSQL..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
print_status "Creating database and user..."
sudo -u postgres psql << EOF
CREATE DATABASE $DB_NAME;
CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
\q
EOF

# Create application directory
print_status "Setting up application directory..."
APP_DIR="/opt/mre-enhancement-backend"
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# Clone or update repository
if [ -d "$APP_DIR/.git" ]; then
    print_status "Updating existing repository..."
    cd $APP_DIR
    git pull origin main
else
    print_status "Cloning repository..."
    git clone https://github.com/your-repo/mre-enhancement-backend.git $APP_DIR
    cd $APP_DIR
fi

# Create virtual environment
print_status "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
print_status "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create environment file
print_status "Creating environment configuration..."
cat > .env << EOF
# Database Configuration
DB_HOST=$DB_HOST
DB_NAME=$DB_NAME
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_PORT=5432

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=$REDIS_PORT
REDIS_PASSWORD=$REDIS_PASSWORD
REDIS_DB=0

# Application Configuration
FLASK_ENV=production
FLASK_APP=app.py
SECRET_KEY=$(openssl rand -hex 32)

# Server Configuration
HOST=0.0.0.0
PORT=$BACKEND_PORT
EOF

# Create systemd service file
print_status "Creating systemd service..."
sudo tee /etc/systemd/system/mre-enhancement-backend.service > /dev/null << EOF
[Unit]
Description=MRE Enhancement Backend
After=network.target postgresql.service redis-server.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
Environment=PATH=$APP_DIR/venv/bin
ExecStart=$APP_DIR/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable service
print_status "Enabling and starting service..."
sudo systemctl daemon-reload
sudo systemctl enable mre-enhancement-backend
sudo systemctl start mre-enhancement-backend

# Create Nginx configuration
print_status "Setting up Nginx reverse proxy..."
sudo tee /etc/nginx/sites-available/mre-enhancement-backend > /dev/null << EOF
server {
    listen 80;
    server_name $SERVER_IP;

    location /api/ {
        proxy_pass http://127.0.0.1:$BACKEND_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location / {
        # Frontend will be served from here
        root /var/www/html;
        try_files \$uri \$uri/ /index.html;
    }
}
EOF

# Enable Nginx site
sudo ln -sf /etc/nginx/sites-available/mre-enhancement-backend /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo systemctl restart nginx

# Create health check script
print_status "Creating health check script..."
cat > health_check.sh << 'EOF'
#!/bin/bash
API_URL="http://localhost:5001/api/health"
response=$(curl -s -o /dev/null -w "%{http_code}" $API_URL)
if [ "$response" = "200" ]; then
    echo "Backend is healthy"
    exit 0
else
    echo "Backend is not responding (HTTP $response)"
    exit 1
fi
EOF

chmod +x health_check.sh

# Create log rotation
print_status "Setting up log rotation..."
sudo tee /etc/logrotate.d/mre-enhancement-backend > /dev/null << EOF
$APP_DIR/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 644 $USER $USER
}
EOF

# Set up firewall
print_status "Configuring firewall..."
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

# Final status check
print_status "Performing final status check..."
sleep 5

if sudo systemctl is-active --quiet mre-enhancement-backend; then
    print_success "Backend service is running"
else
    print_error "Backend service failed to start"
    sudo systemctl status mre-enhancement-backend
    exit 1
fi

if sudo systemctl is-active --quiet redis-server; then
    print_success "Redis service is running"
else
    print_error "Redis service is not running"
    exit 1
fi

if sudo systemctl is-active --quiet postgresql; then
    print_success "PostgreSQL service is running"
else
    print_error "PostgreSQL service is not running"
    exit 1
fi

if sudo systemctl is-active --quiet nginx; then
    print_success "Nginx service is running"
else
    print_error "Nginx service is not running"
    exit 1
fi

print_success "Deployment completed successfully!"
echo ""
echo "Next steps:"
echo "1. Update the frontend config.js with your server IP: $SERVER_IP"
echo "2. Deploy the frontend build to /var/www/html/"
echo "3. Test the API endpoints: http://$SERVER_IP/api/health"
echo "4. Monitor logs: sudo journalctl -u mre-enhancement-backend -f"
echo ""
echo "Useful commands:"
echo "  Start service: sudo systemctl start mre-enhancement-backend"
echo "  Stop service: sudo systemctl stop mre-enhancement-backend"
echo "  Restart service: sudo systemctl restart mre-enhancement-backend"
echo "  View logs: sudo journalctl -u mre-enhancement-backend -f"
echo "  Health check: ./health_check.sh" 

