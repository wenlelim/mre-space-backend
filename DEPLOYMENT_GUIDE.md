# MRE Enhancement Backend Deployment Guide

## Prerequisites

- Ubuntu 20.04+ or similar Linux distribution
- sudo privileges
- Git installed
- Internet connection

## Quick Deployment

### 1. Clone the Repository
```bash
git clone <your-repo-url> mre-enhancement-backend
cd mre-enhancement-backend
```

### 2. Update Configuration
Edit the `deploy.sh` script and update these variables:
```bash
SERVER_IP="10.135.128.194"  # Your server IP
REDIS_PASSWORD="your_secure_redis_password"  # Change this!
DB_USER="your_db_user"  # PostgreSQL user
DB_PASSWORD="your_db_password"  # PostgreSQL password
```

### 3. Run Deployment
```bash
chmod +x deploy.sh
./deploy.sh
```

## Manual Setup (Alternative)

### 1. Install System Dependencies
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv redis-server postgresql postgresql-contrib nginx git curl
```

### 2. Configure Redis
```bash
# Start Redis
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Set password (edit /etc/redis/redis.conf)
sudo sed -i "s/# requirepass foobared/requirepass your_secure_password/" /etc/redis/redis.conf
sudo systemctl restart redis-server

# Test Redis
redis-cli -a your_secure_password ping
```

### 3. Configure PostgreSQL
```bash
# Start PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql << EOF
CREATE DATABASE mre_enhancement;
CREATE USER your_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE mre_enhancement TO your_user;
\q
EOF
```

### 4. Setup Application
```bash
# Create app directory
sudo mkdir -p /opt/mre-enhancement-backend
sudo chown $USER:$USER /opt/mre-enhancement-backend

# Copy application files
cp -r * /opt/mre-enhancement-backend/
cd /opt/mre-enhancement-backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Create Environment File
```bash
cat > .env << EOF
DB_HOST=localhost
DB_NAME=mre_enhancement
DB_USER=your_user
DB_PASSWORD=your_password
DB_PORT=5432

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_secure_password
REDIS_DB=0

FLASK_ENV=production
FLASK_APP=app.py
SECRET_KEY=$(openssl rand -hex 32)

HOST=0.0.0.0
PORT=5001
EOF
```

### 6. Create Systemd Service
```bash
sudo tee /etc/systemd/system/mre-enhancement-backend.service > /dev/null << EOF
[Unit]
Description=MRE Enhancement Backend
After=network.target postgresql.service redis-server.service

[Service]
Type=simple
User=$USER
WorkingDirectory=/opt/mre-enhancement-backend
Environment=PATH=/opt/mre-enhancement-backend/venv/bin
ExecStart=/opt/mre-enhancement-backend/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable mre-enhancement-backend
sudo systemctl start mre-enhancement-backend
```

### 7. Configure Nginx
```bash
sudo tee /etc/nginx/sites-available/mre-enhancement-backend > /dev/null << EOF
server {
    listen 80;
    server_name 10.135.128.194;

    location /api/ {
        proxy_pass http://127.0.0.1:5001;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location / {
        root /var/www/html;
        try_files \$uri \$uri/ /index.html;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/mre-enhancement-backend /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo systemctl restart nginx
```

## Verification

### Check Services
```bash
# Check backend service
sudo systemctl status mre-enhancement-backend

# Check Redis
sudo systemctl status redis-server

# Check PostgreSQL
sudo systemctl status postgresql

# Check Nginx
sudo systemctl status nginx
```

### Test API
```bash
# Health check
curl http://10.135.128.194/api/health

# Test Redis cache
curl http://10.135.128.194/api/get_reserved_servers
```

### View Logs
```bash
# Backend logs
sudo journalctl -u mre-enhancement-backend -f

# Nginx logs
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Check Redis is running: `sudo systemctl status redis-server`
   - Verify password in redis.conf and .env file match
   - Test connection: `redis-cli -a your_password ping`

2. **Database Connection Failed**
   - Check PostgreSQL is running: `sudo systemctl status postgresql`
   - Verify database credentials in .env file
   - Test connection: `psql -h localhost -U your_user -d mre_enhancement`

3. **Service Won't Start**
   - Check logs: `sudo journalctl -u mre-enhancement-backend -f`
   - Verify file permissions and paths
   - Check virtual environment is activated

4. **Nginx Issues**
   - Test configuration: `sudo nginx -t`
   - Check error logs: `sudo tail -f /var/log/nginx/error.log`

## Security Notes

- Change default passwords for Redis and PostgreSQL
- Use HTTPS in production
- Configure firewall rules
- Keep system packages updated
- Monitor logs regularly

## Maintenance

### Update Application
```bash
cd /opt/mre-enhancement-backend
git pull origin main
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart mre-enhancement-backend
```

### Backup Database
```bash
pg_dump -h localhost -U your_user mre_enhancement > backup.sql
```

### Monitor Resources
```bash
# Check memory usage
free -h

# Check disk usage
df -h

# Check service status
sudo systemctl status mre-enhancement-backend redis-server postgresql nginx
``` 