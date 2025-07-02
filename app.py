import math
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
import requests
import base64
import csv
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from datetime import timezone
import concurrent.futures
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import pool
import logging
import traceback
import json
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global variable to store the token
auth_token = None

# Global variable for limiter
limiter = None

# Global variable for cache
cache = None

# Global variable to track refresh status
refresh_status = {
    'is_refreshing': False,
    'last_refresh_time': None,
    'refresh_results': {},
    'current_refresh_start': None
}

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'mre_enhancement_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Redis configuration for caching
REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'localhost'),
    'port': int(os.getenv('REDIS_PORT', '6379')),
    'password': os.getenv('REDIS_PASSWORD', None),
    'db': int(os.getenv('REDIS_DB', '0')),
    'decode_responses': True
}

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': REDIS_CONFIG['host'],
    'CACHE_REDIS_PORT': REDIS_CONFIG['port'],
    'CACHE_REDIS_PASSWORD': REDIS_CONFIG['password'],
    'CACHE_REDIS_DB': REDIS_CONFIG['db'],
    'CACHE_DEFAULT_TIMEOUT': 1800,  # 30 minutes default (increased from 5 minutes)
    'CACHE_KEY_PREFIX': 'mre_enhancement_',
    'CACHE_OPTIONS': {
        'socket_connect_timeout': 5,
        'socket_timeout': 5,
        'retry_on_timeout': True,
        'max_connections': 20
    }
}

# Connection pool for handling concurrent users
connection_pool = None

def init_cache(app):
    """Initialize the cache with the Flask app"""
    global cache
    try:
        cache = Cache(app, config=CACHE_CONFIG)
        logger.info("Cache initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing cache: {e}")
        # Fallback to simple cache if Redis is not available
        CACHE_CONFIG['CACHE_TYPE'] = 'simple'
        cache = Cache(app, config=CACHE_CONFIG)
        logger.info("Fallback to simple cache initialized")

def get_cache_key(prefix, *args, **kwargs):
    """Generate a consistent cache key from arguments"""
    key_parts = [prefix]
    
    # Add positional arguments
    for arg in args:
        key_parts.append(str(arg))
    
    # Add keyword arguments (sorted for consistency)
    for key, value in sorted(kwargs.items()):
        key_parts.append(f"{key}:{value}")
    
    # Create a hash of the key parts to ensure it's not too long
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()

def invalidate_table_cache(table_name):
    """Invalidate the cache entry for a specific table"""
    try:
        if cache:
            cache_key = get_cache_key(f"table_{table_name}")
            cache.delete(cache_key)
            logger.info(f"Invalidated cache entry for table {table_name}")
    except Exception as e:
        logger.error(f"Error invalidating cache for table {table_name}: {e}")

def invalidate_all_table_caches():
    """Invalidate all table-related cache entries"""
    try:
        if cache:
            # Get all keys with table prefix
            pattern = f"{CACHE_CONFIG['CACHE_KEY_PREFIX']}table_*"
            keys = cache.redis_client.keys(pattern)
            if keys:
                cache.redis_client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} table cache entries")
    except Exception as e:
        logger.error(f"Error invalidating all table caches: {e}")

def get_cached_data(cache_key, fetch_func, timeout=300):
    """Generic function to get cached data or fetch if not cached"""
    try:
        if cache:
            # Try to get from cache first
            cached_data = cache.get(cache_key)
            if cached_data is not None:
                logger.info(f"Cache hit for key: {cache_key}")
                return cached_data
            
            # If not in cache, fetch the data
            logger.info(f"Cache miss for key: {cache_key}, fetching data...")
            data = fetch_func()
            
            # Store in cache
            if data is not None:
                cache.set(cache_key, data, timeout=timeout)
                logger.info(f"Data cached for key: {cache_key} with timeout: {timeout}s")
            
            return data
        else:
            # If cache is not available, just fetch the data
            return fetch_func()
    except Exception as e:
        logger.error(f"Error in get_cached_data for key {cache_key}: {e}")
        # Fallback to direct fetch
        return fetch_func()

def init_connection_pool():
    """Initialize the connection pool"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=5,      # Minimum connections
            maxconn=20,     # Maximum connections
            **DB_CONFIG
        )
        print("Connection pool initialized successfully")
    except Exception as e:
        print(f"Error initializing connection pool: {e}")
        raise

def get_db_connection():
    """Get a connection from the pool"""
    if connection_pool is None:
        raise Exception("Connection pool not initialized")
    return connection_pool.getconn()

def return_db_connection(conn):
    """Return a connection to the pool"""
    if connection_pool is not None:
        connection_pool.putconn(conn)

def close_connection_pool():
    """Close the connection pool"""
    global connection_pool
    if connection_pool is not None:
        connection_pool.closeall()
        print("Connection pool closed")

def create_optimized_indexes():
    """Create PostgreSQL-specific indexes for better performance"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Indexes for reserved_servers
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reserved_servers_personnel ON reserved_servers(personnel)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reserved_servers_source_table ON reserved_servers(source_table)")
        
        # Indexes for server_config_data
        cur.execute("CREATE INDEX IF NOT EXISTS idx_server_config_name ON server_config_data(server_config_name)")
        
        # Indexes for TOC tables
        toc_tables = ['toc_seamoney_servers', 'toc_shopee_servers', 'toc_id_insurance_servers']
        for table in toc_tables:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_service_tag ON {table}(service_tag)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hostname ON {table}(hostname)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_server_config ON {table}(server_config)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_segment ON {table}(segment)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_country ON {table}(country)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_idc ON {table}(idc)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_rack ON {table}(rack)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_cluster ON {table}(cluster)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_updated_at ON {table}(updated_at)")
            
            # Composite indexes for common query patterns
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_segment_server_config ON {table}(segment, server_config)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_country_idc ON {table}(country, idc)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_service_tag_server_config ON {table}(service_tag, server_config)")
            
            # Additional indexes for Shopee servers filters
            if table == 'toc_shopee_servers':
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_network_zone ON {table}(network_zone)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hall ON {table}(hall)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_resource_type ON {table}(resource_type)")
                # Composite indexes for filter combinations
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_server_config_network_zone ON {table}(server_config, network_zone)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_country_segment ON {table}(country, segment)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_idc_rack ON {table}(idc, rack)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hall_resource_type ON {table}(hall, resource_type)")
        
        # Indexes for infracenter_servers
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_serial_number ON infracenter_servers(serial_number)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_region_code ON infracenter_servers(region_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_idc ON infracenter_servers(server_position_idc_position_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_rack ON infracenter_servers(server_position_rack_position_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_brand ON infracenter_servers(server_profile_brand)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_model ON infracenter_servers(server_profile_model)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_status ON infracenter_servers(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_updated_at ON infracenter_servers(updated_at)")
        
        # Composite indexes for common query patterns
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_region_status ON infracenter_servers(region_code, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_infracenter_idc_rack ON infracenter_servers(server_position_idc_position_name, server_position_rack_position_name)")
        
        conn.commit()
        print("All performance indexes created successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating indexes: {e}")
        raise
    finally:
        cur.close()
        return_db_connection(conn)

def create_tables():
    """Create all necessary tables if they don't exist"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create reserved_servers table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reserved_servers (
                serial_number VARCHAR(255) PRIMARY KEY,
                reserved_date TIMESTAMP,
                personnel VARCHAR(255),
                source_table VARCHAR(255)
            )
        """)
        
        # Create server_config_data table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS server_config_data (
                id SERIAL PRIMARY KEY,
                server_config_name VARCHAR(255),
                cpus VARCHAR(255),
                gpus VARCHAR(255),
                memory VARCHAR(255),
                nics VARCHAR(255),
                spec VARCHAR(255),
                full_server_power VARCHAR(255),
                eighty_percent_server_power VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create TOC servers tables
        toc_tables = ['toc_seamoney_servers', 'toc_shopee_servers', 'toc_id_insurance_servers']
        for table in toc_tables:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR(255),
                    hostname VARCHAR(255),
                    ip_lan VARCHAR(45),
                    ip_wan VARCHAR(45),
                    ip_ipmi VARCHAR(45),
                    az VARCHAR(50),
                    cluster VARCHAR(255),
                    country VARCHAR(50),
                    hall VARCHAR(100),
                    idc VARCHAR(100),
                    is_disabled BOOLEAN DEFAULT FALSE,
                    name VARCHAR(100),
                    network_zone VARCHAR(100),
                    node_name VARCHAR(255),
                    oid VARCHAR(255),
                    os VARCHAR(100),
                    os_image VARCHAR(255),
                    pod VARCHAR(100),
                    rack VARCHAR(100),
                    resource_type VARCHAR(100),
                    resource_zone_key VARCHAR(255),
                    segment VARCHAR(100),
                    segment_key VARCHAR(255),
                    server_config TEXT,
                    server_type VARCHAR(100),
                    service_tag VARCHAR(255) UNIQUE,
                    toc_cluster VARCHAR(255),
                    toc_version VARCHAR(100),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    full_server_power INTEGER,
                    eighty_percent_server_power INTEGER,
                    server_pool VARCHAR(50),
                    version INTEGER DEFAULT 1
                )
            """)
            
        
        # Create infracenter_servers table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS infracenter_servers (
                id SERIAL PRIMARY KEY,
                region_code VARCHAR(255),
                serial_number VARCHAR(255) UNIQUE,
                server_position_idc_position_name VARCHAR(255),
                server_position_position_type VARCHAR(255),
                server_position_rack_position_name VARCHAR(255),
                server_position_storeroom VARCHAR(255),
                server_position_suite_position_name VARCHAR(255),
                server_position_warehouse VARCHAR(255),
                server_profile_brand VARCHAR(255),
                server_profile_max_power VARCHAR(255),
                server_profile_model VARCHAR(255),
                server_profile_planning_power VARCHAR(255),
                server_profile_server_config_server_config VARCHAR(255),
                product_bu_name VARCHAR(255),
                status VARCHAR(255),
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1
            )
        """)
        
        conn.commit()
        print("All tables created successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {e}")
        raise
    finally:
        cur.close()
        return_db_connection(conn)

def create_app():
    app = Flask(__name__)

    # Initialize rate limiter
    global limiter
    
    # Check if rate limiting should be disabled for development
    if os.getenv('DISABLE_RATE_LIMIT', 'false').lower() == 'true':
        # Disable rate limiting for development
        limiter = Limiter(
            app=app,
            key_func=get_remote_address,
            default_limits=["10000 per day", "1000 per hour", "500 per minute"]
        )
        logger.info("Rate limiting disabled for development")
    else:
        # Normal rate limiting
        limiter = Limiter(
            app=app,
            key_func=get_remote_address,
            default_limits=["1000 per day", "200 per hour", "50 per minute"]
        )

    # Initialize CORS with specific configuration
    CORS(app, resources={
        r"/api/*": {
            "origins": [
                "http://localhost:3000",  # Development
                "http://10.135.128.194",  # Production HTTP
                "https://10.135.128.194",  # Production HTTPS
                "http://10.135.128.194:3000",  # Production with port
                "https://10.135.128.194:3000",  # Production HTTPS with port
                "http://10.135.128.194:80",  # Production HTTP port 80
                "https://10.135.128.194:443"   # Production HTTPS port 443
            ],
            "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            "allow_headers": ["Content-Type", "Authorization"]
        }
    })

    # Initialize cache
    init_cache(app)

    # Initialize connection pool and create tables on app startup
    with app.app_context():
        init_connection_pool()
        create_tables()
        create_optimized_indexes()

    @app.route('/api/create_reserved_servers', methods=['POST'])
    def create_reserved_servers():
        # Table creation is now handled by create_tables()
        return jsonify({"message": "Table reserved_servers ensured."}), 200
    
    @app.route('/api/get_reserved_servers', methods=['GET'])
    @limiter.limit("500 per minute")
    def get_reserved_servers():
        def fetch_reserved_servers():
            print("DEBUG: Fetching fresh reserved servers data from database")
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cur.execute("SELECT serial_number, reserved_date, personnel, source_table FROM reserved_servers")
                rows = cur.fetchall()
                # Convert to list of dicts
                servers = [dict(row) for row in rows]
                result = {"reserved_servers": servers}
                print(f"DEBUG: Fetched {len(servers)} reserved servers from database")
                return result
            except Exception as e:
                logger.error(f"Error fetching reserved servers: {e}")
                return {"error": str(e)}
            finally:
                cur.close()
                return_db_connection(conn)
        
        # Use caching with 10-minute timeout for reserved servers
        cache_key = get_cache_key("table_reserved_servers")
        result = get_cached_data(cache_key, fetch_reserved_servers, timeout=600)
        
        if "error" in result:
            return jsonify(result), 500
        return jsonify(result), 200
    
    @app.route('/api/add_reserved_servers', methods=['POST'])
    @limiter.limit("100 per minute")
    def add_reserved_servers():
        from datetime import datetime
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # Get data from request
            data = request.json
            serial_numbers = data.get('serial_number')
            personnel = data.get('personnel')
            source_table = data.get('source_table')
            
            # Accept both a single string or a list for serial_number
            if not serial_numbers or not personnel or not source_table:
                return jsonify({"error": "serial_number, personnel, and source_table are required"}), 400
            if isinstance(serial_numbers, str):
                serial_numbers = [serial_numbers]
            elif not isinstance(serial_numbers, list):
                return jsonify({"error": "serial_number must be a string or list"}), 400

            reserved_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Use row-level locking to prevent race conditions
            # First, lock the rows we want to update
            placeholders = ','.join(['%s'] * len(serial_numbers))
            cur.execute(f"""
                SELECT serial_number FROM reserved_servers 
                WHERE serial_number IN ({placeholders})
                FOR UPDATE
            """, tuple(serial_numbers))
            
            # Check if any servers are already reserved by someone else
            already_reserved = cur.fetchall()
            if already_reserved:
                existing_servers = [row[0] for row in already_reserved]
                return jsonify({
                    "error": f"Servers already reserved: {existing_servers}",
                    "conflicting_servers": existing_servers
                }), 409  # Conflict status code
            
            # Use PostgreSQL's bulk upsert with executemany for better performance
            from psycopg2.extras import execute_values
            data_to_insert = [(sn, reserved_date, personnel, source_table) for sn in serial_numbers]
            
            execute_values(
                cur,
                """
                INSERT INTO reserved_servers (serial_number, reserved_date, personnel, source_table) 
                VALUES %s
                ON CONFLICT (serial_number) DO UPDATE SET 
                    reserved_date = EXCLUDED.reserved_date, 
                    personnel = EXCLUDED.personnel, 
                    source_table = EXCLUDED.source_table
                """,
                data_to_insert,
                template=None,
                page_size=100
            )
            conn.commit()
            
            # Invalidate reserved servers cache
            invalidate_table_cache("reserved_servers")
            
            result = {
                "message": f"Reserved server(s) added successfully.",
                "added_serial_numbers": serial_numbers
            }
            status = 200
            
        except Exception as e:
            conn.rollback()
            result = {"error": str(e)}
            status = 500
        finally:
            cur.close()
            return_db_connection(conn)
            
        return jsonify(result), status

    @app.route('/api/delete_reserved_servers', methods=['POST'])
    def delete_reserved_servers():
        conn = get_db_connection()
        cur = conn.cursor()
        # Get data from request
        data = request.json
        serial_numbers = data.get('serial_number')
        # Accept both a single string or a list for serial_number
        if not serial_numbers:
            cur.close()
            return_db_connection(conn)
            return jsonify({"error": "serial_number is required"}), 400
        if isinstance(serial_numbers, str):
            serial_numbers = [serial_numbers]
        elif not isinstance(serial_numbers, list):
            cur.close()
            return_db_connection(conn)
            return jsonify({"error": "serial_number must be a string or list"}), 400

        try:
            # Delete the specified serial numbers
            placeholders = ','.join(['%s'] * len(serial_numbers))
            cur.execute(
                f"DELETE FROM reserved_servers WHERE serial_number IN ({placeholders})",
                tuple(serial_numbers)
            )
            conn.commit()
            
            # Invalidate reserved servers cache
            invalidate_table_cache("reserved_servers")
            
            result = {
                "message": f"Deleted {len(serial_numbers)} reserved server(s).",
                "deleted_serial_numbers": serial_numbers
            }
            status = 200
        except Exception as e:
            conn.rollback()
            result = {"error": str(e)}
            status = 500
        finally:
            cur.close()
            return_db_connection(conn)
        return jsonify(result), status
        
    @app.route('/api/update_infracenter_servers', methods=['POST'])
    def update_infracenter_servers():
        from datetime import timezone, timedelta
        print("[InfraCenter] Starting update_infracenter_servers API...")
        cursor = "1"
        result = []
        print("[InfraCenter] Connecting to database...")
        gmt8 = timezone(timedelta(hours=8))
        now_str = datetime.now(gmt8).replace(microsecond=0).isoformat()
        print("[InfraCenter] Starting fetch loop...")
        
        try:
            # First, fetch all data
            while True:
                request_body = {
                    "cursor": cursor,
                    "size": 1000,
                    "filter_statuses": [
                        "Serving", "On rack", "In inventory"
                    ],
                    "filter_business_units": [
                        "SeaMoney", "Shopee"
                    ],
                    "with_server_profile": True,
                    "with_server_position": True,
                    "with_product": True,
                    "with_business_entity": False,
                    "with_region": False
                }
                response = requests.post(
                    'https://infracenter.sea.com/api/v1/external/servers/search',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': '13lr7d0i0o75xl4u'
                    },
                    json=request_body
                )
                response.raise_for_status()
                print(f"[InfraCenter] Fetched cursor: {cursor}")
                server_data = response.json().get('data', {})
                items = server_data.get('items')
                next_cursor = server_data.get('next_cursor')

                # Only keep these keys after flattening
                selected_keys = [
                    "region.code",
                    "serial_number",
                    "server_position.idc.position_name",
                    "server_position.position_type",
                    "server_position.rack.position_name",
                    "server_position.storeroom",
                    "server_position.suite.position_name",
                    "server_position.warehouse",
                    "server_profile.brand",
                    "server_profile.max_power",
                    "server_profile.model",
                    "server_profile.planning_power",
                    "server_profile.server_config.server_config",
                    "product.bu.name",
                    "status"
                ]

                def flatten_dict(d, parent_key='', sep='.'):
                    items_flat = []
                    for k, v in d.items():
                        new_key = f"{parent_key}{sep}{k}" if parent_key else k
                        if isinstance(v, dict):
                            items_flat.extend(flatten_dict(v, new_key, sep=sep).items())
                        else:
                            items_flat.append((new_key, v))
                    return dict(items_flat)

                if not items or not next_cursor:
                    break
                # Flatten each item and only keep selected keys
                flattened_items = [
                    {k: v for k, v in flatten_dict(item).items() if k in selected_keys}
                    for item in items
                ]
                result.extend(flattened_items)
                
                cursor = next_cursor

            print(f"[InfraCenter] All data fetched. Total items: {len(result)}")
            if not result:
                return jsonify({'error': 'No data provided'}), 400

            # Add 'updated_at' column to selected_keys if not present
            updated_at_col = "updated_at"
            if updated_at_col not in selected_keys:
                selected_keys.append(updated_at_col)

            # Replace dots with underscores in selected_keys
            selected_keys = [key.replace('.', '_') for key in selected_keys]

            # Connect to PostgreSQL
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Process all data at once
            processed_data = []
            incoming_serial_numbers = set()  # Track incoming serial numbers
            for row in result:
                # Add/overwrite updated_at for each row
                row_with_updated = {**row, updated_at_col: now_str}
                # Replace dots with underscores in keys
                processed_row = {k.replace('.', '_'): v for k, v in row_with_updated.items()}
                row_values = [processed_row.get(col, None) for col in selected_keys]
                processed_data.append(row_values)
                # Track serial number
                if 'serial_number' in processed_row:
                    incoming_serial_numbers.add(processed_row['serial_number'])

            # Delete records that are no longer in the incoming data
            if incoming_serial_numbers:
                placeholders = ','.join(['%s'] * len(incoming_serial_numbers))
                cur.execute(f'DELETE FROM infracenter_servers WHERE serial_number NOT IN ({placeholders})', tuple(incoming_serial_numbers))
                print(f"[InfraCenter] Deleted records not in incoming data")

            # Perform UPSERT operation using PostgreSQL's ON CONFLICT
            print("[InfraCenter] Performing UPSERT operation...")
            colnames = ','.join([f'"{col}"' for col in selected_keys])
            placeholders = ','.join(['%s'] * len(selected_keys))
            
            if processed_data:
                set_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in selected_keys if col != 'serial_number'])
                sql = f'''
                    INSERT INTO infracenter_servers ({colnames})
                    VALUES %s
                    ON CONFLICT (serial_number) DO UPDATE SET {set_clause}
                '''
                execute_values(cur, sql, processed_data, page_size=1000)

            # Get counts
            cur.execute("SELECT COUNT(*) FROM infracenter_servers")
            total_records = cur.fetchone()[0]
            
            conn.commit()

            print(f"[InfraCenter] Successfully processed {total_records} records")
            return jsonify({
                'message': f'Successfully processed {total_records} records',
                'processed_count': total_records
            }), 200

        except Exception as e:
            print(f"[InfraCenter] Error: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
            return jsonify({'error': str(e)}), 500
        finally:
            if 'conn' in locals():
                cur.close()
                return_db_connection(conn)

    @app.route('/api/upload_infra_servers', methods=['POST'])
    def upload_infra_servers():
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400

        # Read CSV into DataFrame, specify dtype=str to avoid DtypeWarning and ensure all columns are read as strings
        try:
            df = pd.read_csv(file, low_memory=False)
        except Exception as e:
            return jsonify({'error': f'Failed to read CSV: {str(e)}'}), 400

        # Debug: print columns and first row
        print("CSV columns:", df.columns.tolist())
        if not df.empty:
            print("First row:", df.iloc[0].to_dict())
        else:
            print("CSV is empty after reading.")

        # Connect to DB and ensure table exists
        conn = get_db_connection()
        cur = conn.cursor()

        # Create table with columns matching the CSV if not exists
        columns = [col for col in df.columns]
        col_defs = ', '.join([f'"{col}" TEXT' for col in columns])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS infracenter_servers ({col_defs})'
        cur.execute(create_table_sql)

        # Insert all rows
        inserted = 0
        for idx, row in df.iterrows():
            row_values = [row[col] if pd.notnull(row[col]) else None for col in columns]
            placeholders = ','.join(['%s'] * len(columns))
            colnames = ','.join([f'"{col}"' for col in columns])
            try:
                cur.execute(f'INSERT INTO infracenter_servers ({colnames}) VALUES ({placeholders})', row_values)
                inserted += 1
                print(f"Inserted so far: {inserted}")
            except Exception as e:
                print(f"Insert error at row {idx}: {e}")
                print("Row values:", row_values)
                continue

        conn.commit()
        cur.close()
        return_db_connection(conn)

        return jsonify({
            'message': f'Inserted {inserted} server records into infracenter_servers.',
            'inserted_count': inserted
        }), 200
    
    
    @app.route('/api/update_server_config_data', methods=['POST'])
    def update_server_config_data():
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS server_config_data (
                id SERIAL PRIMARY KEY,
                server_config_name VARCHAR(255),
                cpus VARCHAR(255),
                gpus VARCHAR(255),
                memory VARCHAR(255),
                nics VARCHAR(255),
                spec VARCHAR(255),
                full_server_power VARCHAR(255),
                eighty_percent_server_power VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        try:
            # Clear the table before inserting new data
            cur.execute("DELETE FROM server_config_data")

            with open('server_config_data.csv', 'r', encoding='utf-8') as file:
                csvreader = csv.DictReader(file)
                for row in csvreader:
                    cur.execute("""
                        INSERT INTO server_config_data (
                            server_config_name, cpus, gpus, memory, nics, spec, full_server_power, eighty_percent_server_power
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get("Server Config Name", ""),
                        row.get("Cpus", ""),
                        row.get("Gpus", ""),
                        row.get("Memory", ""),
                        row.get("Nics", ""),
                        row.get("Spec", ""),
                        row.get("Full server power", ""),
                        row.get("80% of server power", "")
                    ))

            conn.commit()
            cur.close()
            return_db_connection(conn)
            return jsonify({"status": "success", "message": "Server config data updated."})

        except Exception as e:
            conn.rollback()
            cur.close()
            return_db_connection(conn)
            return jsonify({"status": "error", "message": str(e)}), 500
                    
    @app.route('/api/update_seamoney_toc_data', methods=['POST'])
    def update_seamoney_toc_data():
        import concurrent.futures
        global auth_token
        data = []
        page_size = 1000
        result = None
        status = 200
        conn = None
        print("[Seamoney] Starting update_seamoney_toc_data API...")
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            print("[Seamoney] Connected to database.")
            # Table creation is now handled by create_tables()
            print("[Seamoney] Table ensured.")
            login()
            print("[Seamoney] Logged in.")
            # Fetch first page to get total
            request_body = {
                "filter": {
                    "type": "binary_operator",
                    "kind": "AND",
                    "values": [
                        {
                            "type": "atom",
                            "key": "resource_node",
                            "method": "starts_with",
                            "value": "seamoney"
                        }
                    ]
                },
                "page": 1,
                "page_size": page_size,
                "params": {
                    "with_basic_info": True,
                    "with_resource_nodes": True
                }
            }
            print("[Seamoney] Fetching first page...")
            response = requests.post(
                'https://toc.shopee.io/api/v4/server/global-servers',
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {auth_token}'
                },
                json=request_body
            )
            response.raise_for_status()
            server_data = response.json().get('data', {})
            total = server_data.get('total', 0)
            first_page_servers = server_data.get('servers', [])
            
            # Process first page servers with node_name extraction
            processed_first_page = []
            for server in first_page_servers:
                # Extract node_name from resource_nodes with enhanced logic
                node_name = None
                if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                    # First, try to find the node with bind_type = "DEFAULT"
                    default_node = next(
                        (node for node in server['resource_nodes'] 
                         if node.get('bind_type') == "DEFAULT"),
                        None
                    )
                    if default_node:
                        node_name = default_node.get('node_name')
                    else:
                        # If no DEFAULT found, use the first available node_name
                        for node in server['resource_nodes']:
                            if node.get('node_name'):
                                node_name = node.get('node_name')
                                break
                # Add node_name to the server data
                server['node_name'] = node_name
                processed_first_page.append(server)
            
            data.extend(processed_first_page)
            print(f"[Seamoney] First page fetched. Total servers: {total}")
            if total == 0 or not first_page_servers:
                print("[Seamoney] No data provided. Exiting.")
                return jsonify({'error': 'No data provided'}), 400
            # Calculate total pages
            total_pages = (total + page_size - 1) // page_size
            print(f"[Seamoney] Total pages: {total_pages}")

            def fetch_page(page):
                req_body = {
                    "filter": request_body["filter"],
                    "page": page,
                    "page_size": page_size,
                    "params": request_body["params"]
                }
                resp = requests.post(
                    'https://toc.shopee.io/api/v4/server/global-servers',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {auth_token}'
                    },
                    json=req_body
                )
                resp.raise_for_status()
                print(f"[Seamoney] Page {page} fetched.")
                servers = resp.json().get('data', {}).get('servers', [])
                # Process and flatten node_name for each server in the page
                processed_servers = []
                for server in servers:
                    # Extract node_name from resource_nodes with enhanced logic
                    node_name = None
                    if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                        # First, try to find the node with bind_type = "DEFAULT"
                        default_node = next(
                            (node for node in server['resource_nodes'] 
                             if node.get('bind_type') == "DEFAULT"),
                            None
                        )
                        if default_node:
                            node_name = default_node.get('node_name')
                        else:
                            # If no DEFAULT found, use the first available node_name
                            for node in server['resource_nodes']:
                                if node.get('node_name'):
                                    node_name = node.get('node_name')
                                    break
                    # Add node_name to the server data
                    server['node_name'] = node_name
                    processed_servers.append(server)
                return processed_servers

            # Fetch remaining pages in parallel
            if total_pages > 1:
                pages_to_fetch = list(range(2, total_pages + 1))
                print(f"[Seamoney] Fetching pages {pages_to_fetch} in parallel...")
                print(f"[Seamoney] Total pages: {total_pages}, Pages to fetch: {len(pages_to_fetch)}")
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    results = list(executor.map(fetch_page, pages_to_fetch))
                for page_servers in results:
                    data.extend(page_servers)
            else:
                print(f"[Seamoney] Only 1 page total, no additional pages to fetch")

            print(f"[Seamoney] All data fetched. Total servers: {len(data)}")
            # Remove serial numbers not in the incoming data
            print("[Seamoney] Deleting old records not in incoming data...")
            incoming_tags = set(row['service_tag'] for row in data if 'service_tag' in row and row['service_tag'])
            if incoming_tags:
                placeholders = ','.join(['%s'] * len(incoming_tags))
                cur.execute(f"DELETE FROM toc_seamoney_servers WHERE service_tag NOT IN ({placeholders})", tuple(incoming_tags))
            else:
                cur.execute("DELETE FROM toc_seamoney_servers")
            print("[Seamoney] Upserting records...")
            # Fetch all existing service_tag values into a set before the loop
            cur.execute("SELECT service_tag FROM toc_seamoney_servers")
            existing_tags = set(row[0] for row in cur.fetchall() if row[0] is not None)
            to_insert = []
            to_update = []
            inserted = 0
            updated = 0

            # Define all columns we want to insert/update, including node_name
            all_keys = [
                'uuid', 'hostname', 'ip_lan', 'ip_wan', 'ip_ipmi', 'az', 'cluster',
                'country', 'hall', 'idc', 'is_disabled', 'name', 'network_zone',
                'node_name', 'oid', 'os', 'os_image', 'pod', 'rack', 'resource_type',
                'resource_zone_key', 'segment', 'segment_key', 'server_config',
                'server_type', 'service_tag', 'toc_cluster', 'toc_version', 'updated_at',
                'server_pool'
            ]

            for row in data:
                if 'service_tag' not in row or not row['service_tag']:
                    continue
                # Create a new row with only the keys we want
                processed_row = {}
                for key in all_keys:
                    if key == 'updated_at':
                        processed_row[key] = datetime.now().isoformat()
                    elif key == 'server_pool':
                        # Set initial server_pool based on node_name
                        node_name = row.get('node_name')
                        if node_name and node_name.startswith('seamoney.spare'):
                            processed_row[key] = 'spare_pool'
                        elif node_name and node_name.strip():
                            processed_row[key] = 'server_pool'
                        else:
                            processed_row[key] = 'unknown_pool'
                    else:
                        processed_row[key] = row.get(key)
                # Create the insert/update values tuple
                insert_values = tuple(processed_row.get(k) for k in all_keys)
                if processed_row['service_tag'] in existing_tags:
                    # For update, exclude service_tag from the SET clause
                    update_values = tuple(processed_row.get(k) for k in all_keys if k != 'service_tag') + (processed_row['service_tag'],)
                    to_update.append(update_values)
                    updated += 1
                    print(f"Updated so far: {updated}")
                else:
                    to_insert.append(insert_values)
                    inserted += 1
                    print(f"Inserted so far: {inserted}")

            # Bulk insert
            if to_insert:
                columns = ', '.join(all_keys)
                set_clause = ', '.join([f'{col}=EXCLUDED.{col}' for col in all_keys if col != 'service_tag'])
                sql = f'''
                    INSERT INTO toc_seamoney_servers ({columns})
                    VALUES %s
                    ON CONFLICT (service_tag) DO UPDATE SET {set_clause}
                '''
                # Process in smaller chunks to reduce lock duration and improve concurrency
                # Each chunk is committed separately, allowing other users to see partial updates
                chunk_size = 1000  # Smaller chunks for better concurrency
                for i in range(0, len(to_insert), chunk_size):
                    chunk = to_insert[i:i + chunk_size]
                    execute_values(cur, sql, chunk, page_size=chunk_size)
                    conn.commit()  # Commit each chunk to release locks
                    logger.info(f"Committed chunk {i//chunk_size + 1} of {(len(to_insert) + chunk_size - 1)//chunk_size}")

            # Bulk update
            if to_update:
                set_clause = ', '.join([f'{k} = %s' for k in all_keys if k != 'service_tag'])
                cur.executemany(
                    f'UPDATE toc_seamoney_servers SET {set_clause} WHERE service_tag = %s',
                    to_update
                )
            print("[Seamoney] Updating power data...")
            cur.execute("""
                UPDATE toc_seamoney_servers 
                SET 
                    full_server_power = scd.full_server_power::INTEGER,
                    eighty_percent_server_power = scd.eighty_percent_server_power::INTEGER
                FROM server_config_data scd
                WHERE toc_seamoney_servers.server_config = scd.server_config_name
                AND scd.full_server_power IS NOT NULL 
                AND scd.eighty_percent_server_power IS NOT NULL
            """)
            updated_count = cur.rowcount
            print(f"[Seamoney] Power data updated for {updated_count} servers.")
            
            # Data transformation: Categorize servers based on node_name
            print("[Seamoney] Categorizing servers into pools...")
            
            # First, let's check what node_name values we have
            cur.execute("""
                SELECT DISTINCT node_name, COUNT(*) as count 
                FROM toc_seamoney_servers 
                WHERE node_name IS NOT NULL 
                GROUP BY node_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            node_name_samples = cur.fetchall()
            print(f"[Seamoney] Sample node_name values: {node_name_samples}")
            
            # Check how many servers have node_name
            cur.execute("SELECT COUNT(*) FROM toc_seamoney_servers WHERE node_name IS NOT NULL")
            servers_with_node_name = cur.fetchone()[0]
            print(f"[Seamoney] Servers with node_name: {servers_with_node_name}")
            
            # Check how many match the spare pattern
            cur.execute("SELECT COUNT(*) FROM toc_seamoney_servers WHERE node_name LIKE 'seamoney.spare%'")
            spare_servers = cur.fetchone()[0]
            print(f"[Seamoney] Servers matching 'seamoney.spare%': {spare_servers}")
            
            # Verify the results (server_pool should already be set during data processing)
            cur.execute("SELECT server_pool, COUNT(*) FROM toc_seamoney_servers GROUP BY server_pool")
            pool_distribution = cur.fetchall()
            print(f"[Seamoney] Pool distribution: {pool_distribution}")
            
            pool_updated_count = len(data)  # All servers processed during data insertion/update
            print(f"[Seamoney] Server pool categorization completed for {pool_updated_count} servers.")
            
            conn.commit()
            print("[Seamoney] All done. Returning response.")
            result = {
                'message': 'Servers and power data in toc_seamoney_servers updated successfully',
                'updated_count': len(data),
                'power_updated': updated_count,
                'pool_categorized': pool_updated_count
            }
            status = 200
        except Exception as e:
            if conn:
                conn.rollback()
            result = {'error': str(e)}
            status = 500
        finally:
            if conn:
                cur.close()
                return_db_connection(conn)
        return jsonify(result), status
        
    @app.route('/api/update_shopee_toc_data', methods=['POST'])
    def update_shopee_toc_data():
        import concurrent.futures
        global auth_token
        data = []
        page_size = 1000
        result = None
        status = 200
        conn = None
        print("[Shopee] Starting update_shopee_toc_data API...")
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            print("[Shopee] Connected to database.")
            # Table creation is now handled by create_tables()
            print("[Shopee] Table ensured.")
            login()
            print("[Shopee] Logged in.")
            # Fetch first page to get total
            request_body = {
                "filter": {
                    "type": "binary_operator",
                    "kind": "AND",
                    "values": [
                        {
                            "type": "atom",
                            "key": "resource_node",
                            "method": "starts_with",
                            "value": "shopee"
                        }
                    ]
                },
                "page": 1,
                "page_size": page_size,
                "params": {
                    "with_basic_info": True,
                    "with_resource_nodes": True
                }
            }
            print("[Shopee] Fetching first page...")
            response = requests.post(
                'https://toc.shopee.io/api/v4/server/global-servers',
                headers={
                    'Content-Type': 'application/json',
                    "Authorization": f'Bearer {auth_token}'
                },
                json=request_body
            )
            response.raise_for_status()
            server_data = response.json().get('data', {})
            total = server_data.get('total', 0)
            first_page_servers = server_data.get('servers', [])
            
            # Process first page servers with node_name extraction
            processed_first_page = []
            for server in first_page_servers:
                # Extract node_name from resource_nodes with enhanced logic
                node_name = None
                if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                    # First, try to find the node with bind_type = "DEFAULT"
                    default_node = next(
                        (node for node in server['resource_nodes'] 
                         if node.get('bind_type') == "DEFAULT"),
                        None
                    )
                    if default_node:
                        node_name = default_node.get('node_name')
                    else:
                        # If no DEFAULT found, use the first available node_name
                        for node in server['resource_nodes']:
                            if node.get('node_name'):
                                node_name = node.get('node_name')
                                break
                # Add node_name to the server data
                server['node_name'] = node_name
                processed_first_page.append(server)
            
            data.extend(processed_first_page)
            print(f"[Shopee] First page fetched. Total servers: {total}")
            if total == 0 or not first_page_servers:
                print("[Shopee] No data provided. Exiting.")
                return jsonify({'error': 'No data provided'}), 400
            # Calculate total pages
            total_pages = (total + page_size - 1) // page_size
            print(f"[Shopee] Total pages: {total_pages}")

            def fetch_page(page):
                req_body = {
                    "filter": request_body["filter"],
                    "page": page,
                    "page_size": page_size,
                    "params": request_body["params"]
                }
                resp = requests.post(
                    'https://toc.shopee.io/api/v4/server/global-servers',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {auth_token}'
                    },
                    json=req_body
                )
                resp.raise_for_status()
                print(f"[Shopee] Page {page} fetched.")
                servers = resp.json().get('data', {}).get('servers', [])
                # Process and flatten node_name for each server in the page
                processed_servers = []
                for server in servers:
                    # Extract node_name from resource_nodes with enhanced logic
                    node_name = None
                    if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                        # First, try to find the node with bind_type = "DEFAULT"
                        default_node = next(
                            (node for node in server['resource_nodes'] 
                             if node.get('bind_type') == "DEFAULT"),
                            None
                        )
                        if default_node:
                            node_name = default_node.get('node_name')
                        else:
                            # If no DEFAULT found, use the first available node_name
                            for node in server['resource_nodes']:
                                if node.get('node_name'):
                                    node_name = node.get('node_name')
                                    break
                    # Add node_name to the server data
                    server['node_name'] = node_name
                    processed_servers.append(server)
                return processed_servers

            # Fetch remaining pages in parallel
            if total_pages > 1:
                pages_to_fetch = list(range(2, total_pages + 1))
                print(f"[Shopee] Fetching pages {pages_to_fetch} in parallel...")
                print(f"[Shopee] Total pages: {total_pages}, Pages to fetch: {len(pages_to_fetch)}")
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    results = list(executor.map(fetch_page, pages_to_fetch))
                for page_servers in results:
                    data.extend(page_servers)
            else:
                print(f"[Shopee] Only 1 page total, no additional pages to fetch")

            print(f"[Shopee] All data fetched. Total servers: {len(data)}")
            # Remove serial numbers not in the incoming data
            print("[Shopee] Deleting old records not in incoming data...")
            incoming_tags = set(row['service_tag'] for row in data if 'service_tag' in row and row['service_tag'])
            if incoming_tags:
                placeholders = ','.join(['%s'] * len(incoming_tags))
                cur.execute(f"DELETE FROM toc_shopee_servers WHERE service_tag NOT IN ({placeholders})", tuple(incoming_tags))
            else:
                cur.execute("DELETE FROM toc_shopee_servers")
            print("[Shopee] Upserting records...")
            # Fetch all existing service_tag values into a set before the loop
            cur.execute("SELECT service_tag FROM toc_shopee_servers")
            existing_tags = set(row[0] for row in cur.fetchall() if row[0] is not None)
            to_insert = []
            to_update = []
            inserted = 0
            updated = 0

            # Define all columns we want to insert/update, including node_name
            all_keys = [
                'uuid', 'hostname', 'ip_lan', 'ip_wan', 'ip_ipmi', 'az', 'cluster',
                'country', 'hall', 'idc', 'is_disabled', 'name', 'network_zone',
                'node_name', 'oid', 'os', 'os_image', 'pod', 'rack', 'resource_type',
                'resource_zone_key', 'segment', 'segment_key', 'server_config',
                'server_type', 'service_tag', 'toc_cluster', 'toc_version', 'updated_at',
                'server_pool'
            ]

            for row in data:
                if 'service_tag' not in row or not row['service_tag']:
                    continue
                # Create a new row with only the keys we want
                processed_row = {}
                for key in all_keys:
                    if key == 'updated_at':
                        processed_row[key] = datetime.now().isoformat()
                    elif key == 'server_pool':
                        # Set initial server_pool based on node_name
                        node_name = row.get('node_name')
                        if node_name and node_name.startswith('shopee.spare'):
                            processed_row[key] = 'spare_pool'
                        elif node_name and node_name.strip():
                            processed_row[key] = 'server_pool'
                        else:
                            processed_row[key] = 'unknown_pool'
                    else:
                        processed_row[key] = row.get(key)
                # Create the insert/update values tuple
                insert_values = tuple(processed_row.get(k) for k in all_keys)
                if processed_row['service_tag'] in existing_tags:
                    # For update, exclude service_tag from the SET clause
                    update_values = tuple(processed_row.get(k) for k in all_keys if k != 'service_tag') + (processed_row['service_tag'],)
                    to_update.append(update_values)
                    updated += 1
                    print(f"Updated so far: {updated}")
                else:
                    to_insert.append(insert_values)
                    inserted += 1
                    print(f"Inserted so far: {inserted}")

            # Bulk insert
            if to_insert:
                columns = ', '.join(all_keys)
                set_clause = ', '.join([f'{col}=EXCLUDED.{col}' for col in all_keys if col != 'service_tag'])
                sql = f'''
                    INSERT INTO toc_shopee_servers ({columns})
                    VALUES %s
                    ON CONFLICT (service_tag) DO UPDATE SET {set_clause}
                '''
                # Process in smaller chunks to reduce lock duration and improve concurrency
                # Each chunk is committed separately, allowing other users to see partial updates
                chunk_size = 1000  # Smaller chunks for better concurrency
                for i in range(0, len(to_insert), chunk_size):
                    chunk = to_insert[i:i + chunk_size]
                    execute_values(cur, sql, chunk, page_size=chunk_size)
                    conn.commit()  # Commit each chunk to release locks
                    logger.info(f"Committed chunk {i//chunk_size + 1} of {(len(to_insert) + chunk_size - 1)//chunk_size}")

            # Bulk update
            if to_update:
                set_clause = ', '.join([f'{k} = %s' for k in all_keys if k != 'service_tag'])
                cur.executemany(
                    f'UPDATE toc_shopee_servers SET {set_clause} WHERE service_tag = %s',
                    to_update
                )
            print("[Shopee] Updating power data...")
            cur.execute("""
                UPDATE toc_shopee_servers 
                SET 
                    full_server_power = scd.full_server_power::INTEGER,
                    eighty_percent_server_power = scd.eighty_percent_server_power::INTEGER
                FROM server_config_data scd
                WHERE toc_shopee_servers.server_config = scd.server_config_name
                AND scd.full_server_power IS NOT NULL 
                AND scd.eighty_percent_server_power IS NOT NULL
            """)
            updated_count = cur.rowcount
            print(f"[Shopee] Power data updated for {updated_count} servers.")
            
            # Data transformation: Categorize servers based on node_name
            print("[Shopee] Categorizing servers into pools...")
            
            # First, let's check what node_name values we have
            cur.execute("""
                SELECT DISTINCT node_name, COUNT(*) as count 
                FROM toc_shopee_servers 
                WHERE node_name IS NOT NULL 
                GROUP BY node_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            node_name_samples = cur.fetchall()
            print(f"[Shopee] Sample node_name values: {node_name_samples}")
            
            # Check how many servers have node_name
            cur.execute("SELECT COUNT(*) FROM toc_shopee_servers WHERE node_name IS NOT NULL")
            servers_with_node_name = cur.fetchone()[0]
            print(f"[Shopee] Servers with node_name: {servers_with_node_name}")
            
            # Check how many match the spare pattern
            cur.execute("SELECT COUNT(*) FROM toc_shopee_servers WHERE node_name LIKE 'shopee.spare%'")
            spare_servers = cur.fetchone()[0]
            print(f"[Shopee] Servers matching 'shopee.spare%': {spare_servers}")
            
            # Verify the results (server_pool should already be set during data processing)
            cur.execute("SELECT server_pool, COUNT(*) FROM toc_shopee_servers GROUP BY server_pool")
            pool_distribution = cur.fetchall()
            print(f"[Shopee] Pool distribution: {pool_distribution}")
            
            pool_updated_count = len(data)  # All servers processed during data insertion/update
            print(f"[Shopee] Server pool categorization completed for {pool_updated_count} servers.")
            
            conn.commit()
            print("[Shopee] All done. Returning response.")
            result = {
                'message': 'Servers and power data in toc_shopee_servers updated successfully',
                'updated_count': len(data),
                'power_updated': updated_count,
                'pool_categorized': pool_updated_count
            }
            status = 200
        except Exception as e:
            if conn:
                conn.rollback()
            result = {'error': str(e)}
            status = 500
        finally:
            if conn:
                cur.close()
                return_db_connection(conn)
        return jsonify(result), status

    @app.route('/api/update_id_insurance_toc_data', methods=['POST'])
    def update_id_insurance_toc_data():
        import concurrent.futures
        global auth_token
        data = []
        page_size = 1000
        result = None
        status = 200
        conn = None
        print("[ID Insurance] Starting update_id_insurance_toc_data API...")
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            print("[ID Insurance] Connected to database.")
            # Table creation is now handled by create_tables()
            print("[ID Insurance] Table ensured.")
            login()
            print("[ID Insurance] Logged in.")
            # Fetch first page to get total
            request_body = {
                "filter": {
                    "type": "binary_operator",
                    "kind": "AND",
                    "values": [
                        {
                            "type": "atom",
                            "key": "resource_node",
                            "method": "starts_with",
                            "value": "id_insurance"
                        }
                    ]
                },
                "page": 1,
                "page_size": page_size,
                "params": {
                    "with_basic_info": True,
                    "with_resource_nodes": True
                }
            }
            print("[ID Insurance] Fetching first page...")
            response = requests.post(
                'https://toc.shopee.io/api/v4/server/global-servers',
                headers={
                    'Content-Type': 'application/json',
                    "Authorization": f'Bearer {auth_token}'
                },
                json=request_body
            )
            response.raise_for_status()
            server_data = response.json().get('data', {})
            total = server_data.get('total', 0)
            first_page_servers = server_data.get('servers', [])
            
            # Process first page servers with node_name extraction
            processed_first_page = []
            for server in first_page_servers:
                # Extract node_name from resource_nodes with enhanced logic
                node_name = None
                if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                    # First, try to find the node with bind_type = "DEFAULT"
                    default_node = next(
                        (node for node in server['resource_nodes'] 
                         if node.get('bind_type') == "DEFAULT"),
                        None
                    )
                    if default_node:
                        node_name = default_node.get('node_name')
                    else:
                        # If no DEFAULT found, use the first available node_name
                        for node in server['resource_nodes']:
                            if node.get('node_name'):
                                node_name = node.get('node_name')
                                break
                # Add node_name to the server data
                server['node_name'] = node_name
                processed_first_page.append(server)
            
            data.extend(processed_first_page)
            print(f"[ID Insurance] First page fetched. Total servers: {total}")
            if total == 0 or not first_page_servers:
                print("[ID Insurance] No data provided. Exiting.")
                return jsonify({'error': 'No data provided'}), 400
            # Calculate total pages
            total_pages = (total + page_size - 1) // page_size
            print(f"[ID Insurance] Total pages: {total_pages}")

            def fetch_page(page):
                req_body = {
                    "filter": request_body["filter"],
                    "page": page,
                    "page_size": page_size,
                    "params": request_body["params"]
                }
                resp = requests.post(
                    'https://toc.shopee.io/api/v4/server/global-servers',
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {auth_token}'
                    },
                    json=req_body
                )
                resp.raise_for_status()
                print(f"[ID Insurance] Page {page} fetched.")
                servers = resp.json().get('data', {}).get('servers', [])
                # Process and flatten node_name for each server in the page
                processed_servers = []
                for server in servers:
                    # Extract node_name from resource_nodes with enhanced logic
                    node_name = None
                    if 'resource_nodes' in server and isinstance(server['resource_nodes'], list):
                        # First, try to find the node with bind_type = "DEFAULT"
                        default_node = next(
                            (node for node in server['resource_nodes'] 
                             if node.get('bind_type') == "DEFAULT"),
                            None
                        )
                        if default_node:
                            node_name = default_node.get('node_name')
                        else:
                            # If no DEFAULT found, use the first available node_name
                            for node in server['resource_nodes']:
                                if node.get('node_name'):
                                    node_name = node.get('node_name')
                                    break
                    # Add node_name to the server data
                    server['node_name'] = node_name
                    processed_servers.append(server)
                return processed_servers

            # Fetch remaining pages in parallel
            if total_pages > 1:
                pages_to_fetch = list(range(2, total_pages + 1))
                print(f"[ID Insurance] Fetching pages {pages_to_fetch} in parallel...")
                print(f"[ID Insurance] Total pages: {total_pages}, Pages to fetch: {len(pages_to_fetch)}")
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    results = list(executor.map(fetch_page, pages_to_fetch))
                for page_servers in results:
                    data.extend(page_servers)
            else:
                print(f"[ID Insurance] Only 1 page total, no additional pages to fetch")

            print(f"[ID Insurance] All data fetched. Total servers: {len(data)}")
            # Remove serial numbers not in the incoming data
            print("[ID Insurance] Deleting old records not in incoming data...")
            incoming_tags = set(row['service_tag'] for row in data if 'service_tag' in row and row['service_tag'])
            if incoming_tags:
                placeholders = ','.join(['%s'] * len(incoming_tags))
                cur.execute(f"DELETE FROM toc_id_insurance_servers WHERE service_tag NOT IN ({placeholders})", tuple(incoming_tags))
            else:
                cur.execute("DELETE FROM toc_id_insurance_servers")
            print("[ID Insurance] Upserting records...")
            # Fetch all existing service_tag values into a set before the loop
            cur.execute("SELECT service_tag FROM toc_id_insurance_servers")
            existing_tags = set(row[0] for row in cur.fetchall() if row[0] is not None)
            to_insert = []
            to_update = []
            inserted = 0
            updated = 0

            # Define all columns we want to insert/update, including node_name
            all_keys = [
                'uuid', 'hostname', 'ip_lan', 'ip_wan', 'ip_ipmi', 'az', 'cluster',
                'country', 'hall', 'idc', 'is_disabled', 'name', 'network_zone',
                'node_name', 'oid', 'os', 'os_image', 'pod', 'rack', 'resource_type',
                'resource_zone_key', 'segment', 'segment_key', 'server_config',
                'server_type', 'service_tag', 'toc_cluster', 'toc_version', 'updated_at',
                'server_pool'
            ]

            for row in data:
                if 'service_tag' not in row or not row['service_tag']:
                    continue
                # Create a new row with only the keys we want
                processed_row = {}
                for key in all_keys:
                    if key == 'updated_at':
                        processed_row[key] = datetime.now().isoformat()
                    elif key == 'server_pool':
                        # Set initial server_pool based on node_name
                        node_name = row.get('node_name')
                        if node_name and node_name.startswith('id_insurance.spare'):
                            processed_row[key] = 'spare_pool'
                        elif node_name and node_name.strip():
                            processed_row[key] = 'server_pool'
                        else:
                            processed_row[key] = 'unknown_pool'
                    else:
                        processed_row[key] = row.get(key)
                # Create the insert/update values tuple
                insert_values = tuple(processed_row.get(k) for k in all_keys)
                if processed_row['service_tag'] in existing_tags:
                    # For update, exclude service_tag from the SET clause
                    update_values = tuple(processed_row.get(k) for k in all_keys if k != 'service_tag') + (processed_row['service_tag'],)
                    to_update.append(update_values)
                    updated += 1
                    print(f"Updated so far: {updated}")
                else:
                    to_insert.append(insert_values)
                    inserted += 1
                    print(f"Inserted so far: {inserted}")

            # Bulk insert
            if to_insert:
                columns = ', '.join(all_keys)
                set_clause = ', '.join([f'{col}=EXCLUDED.{col}' for col in all_keys if col != 'service_tag'])
                sql = f'''
                    INSERT INTO toc_id_insurance_servers ({columns})
                    VALUES %s
                    ON CONFLICT (service_tag) DO UPDATE SET {set_clause}
                '''
                # Process in smaller chunks to reduce lock duration and improve concurrency
                # Each chunk is committed separately, allowing other users to see partial updates
                chunk_size = 1000  # Smaller chunks for better concurrency
                for i in range(0, len(to_insert), chunk_size):
                    chunk = to_insert[i:i + chunk_size]
                    execute_values(cur, sql, chunk, page_size=chunk_size)
                    conn.commit()  # Commit each chunk to release locks
                    logger.info(f"Committed chunk {i//chunk_size + 1} of {(len(to_insert) + chunk_size - 1)//chunk_size}")

            # Bulk update
            if to_update:
                set_clause = ', '.join([f'{k} = %s' for k in all_keys if k != 'service_tag'])
                cur.executemany(
                    f'UPDATE toc_id_insurance_servers SET {set_clause} WHERE service_tag = %s',
                    to_update
                )
            print("[ID Insurance] Updating power data...")
            cur.execute("""
                UPDATE toc_id_insurance_servers 
                SET 
                    full_server_power = scd.full_server_power::INTEGER,
                    eighty_percent_server_power = scd.eighty_percent_server_power::INTEGER
                FROM server_config_data scd
                WHERE toc_id_insurance_servers.server_config = scd.server_config_name
                AND scd.full_server_power IS NOT NULL 
                AND scd.eighty_percent_server_power IS NOT NULL
            """)
            updated_count = cur.rowcount
            print(f"[ID Insurance] Power data updated for {updated_count} servers.")
            
            # Data transformation: Categorize servers based on node_name
            print("[ID Insurance] Categorizing servers into pools...")
            
            # First, let's check what node_name values we have
            cur.execute("""
                SELECT DISTINCT node_name, COUNT(*) as count 
                FROM toc_id_insurance_servers 
                WHERE node_name IS NOT NULL 
                GROUP BY node_name 
                ORDER BY count DESC 
                LIMIT 10
            """)
            node_name_samples = cur.fetchall()
            print(f"[ID Insurance] Sample node_name values: {node_name_samples}")
            
            # Check how many servers have node_name
            cur.execute("SELECT COUNT(*) FROM toc_id_insurance_servers WHERE node_name IS NOT NULL")
            servers_with_node_name = cur.fetchone()[0]
            print(f"[ID Insurance] Servers with node_name: {servers_with_node_name}")
            
            # Check how many match the spare pattern
            cur.execute("SELECT COUNT(*) FROM toc_id_insurance_servers WHERE node_name LIKE 'id_insurance.spare%'")
            spare_servers = cur.fetchone()[0]
            print(f"[ID Insurance] Servers matching 'id_insurance.spare%': {spare_servers}")
            
            # Now perform the categorization
            cur.execute("""
                UPDATE toc_id_insurance_servers 
                SET server_pool = CASE 
                    WHEN node_name LIKE 'id_insurance.spare%' THEN 'spare_pool'
                    WHEN node_name IS NOT NULL AND node_name != '' THEN 'server_pool'
                    ELSE 'unknown_pool'
                END
            """)
            pool_updated_count = cur.rowcount
            print(f"[ID Insurance] Server pool categorization completed for {pool_updated_count} servers.")
            
            # Verify the results
            cur.execute("SELECT server_pool, COUNT(*) FROM toc_id_insurance_servers GROUP BY server_pool")
            pool_distribution = cur.fetchall()
            print(f"[ID Insurance] Pool distribution: {pool_distribution}")
            
            conn.commit()
            print("[ID Insurance] All done. Returning response.")
            result = {
                'message': 'Servers and power data in toc_id_insurance_servers updated successfully',
                'updated_count': len(data),
                'power_updated': updated_count,
                'pool_categorized': pool_updated_count
            }
            status = 200
        except Exception as e:
            if conn:
                conn.rollback()
            result = {'error': str(e)}
            status = 500
        finally:
            if conn:
                cur.close()
                return_db_connection(conn)
        return jsonify(result), status
        
    @app.route('/api/bulk_refresh_all_servers', methods=['POST'])
    def bulk_refresh_all_servers():
        """Bulk refresh all server tables - called automatically every hour"""
        global refresh_status
        
        # Check if refresh is already in progress
        if refresh_status['is_refreshing']:
            return jsonify({
                'error': 'Refresh already in progress',
                'started_at': refresh_status['current_refresh_start'],
                'timestamp': datetime.now().isoformat()
            }), 409  # Conflict status
        
        try:
            # Set refresh status
            refresh_status['is_refreshing'] = True
            refresh_status['current_refresh_start'] = datetime.now().isoformat()
            refresh_status['refresh_results'] = {}
            
            logger.info("Starting bulk refresh of all server tables")
            
            # Refresh all server tables in sequence
            refresh_results = {}
            
            # Refresh TOC tables
            toc_tables = [
                ('toc_seamoney_servers', '/api/update_seamoney_toc_data'),
                ('toc_shopee_servers', '/api/update_shopee_toc_data'),
                ('toc_id_insurance_servers', '/api/update_id_insurance_toc_data')
            ]
            
            for table_name, endpoint in toc_tables:
                try:
                    logger.info(f"Refreshing {table_name}...")
                    # Create a test client to call the endpoint internally
                    with app.test_client() as client:
                        response = client.post(endpoint)
                        if response.status_code == 200:
                            refresh_results[table_name] = "success"
                            logger.info(f"Successfully refreshed {table_name}")
                        else:
                            refresh_results[table_name] = f"error: {response.status_code}"
                            logger.error(f"Failed to refresh {table_name}: {response.status_code}")
                except Exception as e:
                    refresh_results[table_name] = f"error: {str(e)}"
                    logger.error(f"Exception refreshing {table_name}: {str(e)}")
            
            # Refresh infracenter servers
            try:
                logger.info("Refreshing infracenter_servers...")
                with app.test_client() as client:
                    response = client.post('/api/update_infracenter_servers')
                    if response.status_code == 200:
                        refresh_results['infracenter_servers'] = "success"
                        logger.info("Successfully refreshed infracenter_servers")
                    else:
                        refresh_results['infracenter_servers'] = f"error: {response.status_code}"
                        logger.error(f"Failed to refresh infracenter_servers: {response.status_code}")
            except Exception as e:
                refresh_results['infracenter_servers'] = f"error: {str(e)}"
                logger.error(f"Exception refreshing infracenter_servers: {str(e)}")
            
            # Check if all refreshes were successful
            all_successful = all(result == "success" for result in refresh_results.values())
            
            # Update global refresh status
            refresh_status['is_refreshing'] = False
            refresh_status['last_refresh_time'] = datetime.now().isoformat()
            refresh_status['refresh_results'] = refresh_results
            refresh_status['current_refresh_start'] = None
            
            logger.info(f"Bulk refresh completed. Results: {refresh_results}")
            
            return jsonify({
                'message': 'Bulk refresh completed',
                'results': refresh_results,
                'all_successful': all_successful,
                'timestamp': datetime.now().isoformat()
            }), 200 if all_successful else 207  # 207 Multi-Status
            
        except Exception as e:
            # Reset refresh status on error
            refresh_status['is_refreshing'] = False
            refresh_status['current_refresh_start'] = None
            
            logger.error(f"Bulk refresh failed: {str(e)}")
            return jsonify({
                'error': 'Bulk refresh failed',
                'details': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
        
    @app.route('/api/refresh_status', methods=['GET'])
    def get_refresh_status():
        """Get the current refresh status"""
        return jsonify({
            'is_refreshing': refresh_status['is_refreshing'],
            'last_refresh_time': refresh_status['last_refresh_time'],
            'current_refresh_start': refresh_status['current_refresh_start'],
            'last_refresh_results': refresh_status['refresh_results'],
            'timestamp': datetime.now().isoformat()
        }), 200

    @app.route('/api/get_servers_data', methods=['GET'])
    def get_servers_data():
        """
        Query parameter: table (default: toc_seamoney_servers)
        Example: /api/get_servers_data?table=toc_shopee_servers
        """
        import re
        table = request.args.get('table', 'toc_seamoney_servers')
        # Only allow alphanumeric and underscores for table name to prevent SQL injection
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            return jsonify({'error': 'Invalid table name'}), 400

        def fetch_servers_data():
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)

            try:
                cur.execute(f"SELECT * FROM {table}")
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                return result
                
            except Exception as e:
                logger.error(f"Error fetching data from table {table}: {e}")
                return {'error': f'Failed to get servers data from table {table}', 'details': str(e)}
            finally:
                cur.close()
                return_db_connection(conn)
        
        # Use caching with 20-minute timeout for server data
        cache_key = get_cache_key("table", table)
        result = get_cached_data(cache_key, fetch_servers_data, timeout=1200)
        
        if isinstance(result, dict) and "error" in result:
            return jsonify(result), 500
        return jsonify(result)

    @app.route('/api/get_infracenter_servers', methods=['GET'])
    def get_infracenter_servers():
        """
        Get servers data from infracenter_servers table
        """
        def fetch_infracenter_servers():
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            try:
                cur.execute("SELECT * FROM infracenter_servers")
                rows = cur.fetchall()
                result = [dict(row) for row in rows]
                return result
            except Exception as e:
                logger.error(f"Error fetching infracenter servers data: {e}")
                return {'error': 'Failed to get infracenter servers data', 'details': str(e)}
            finally:
                cur.close()
                return_db_connection(conn)
        # Use caching with 20-minute timeout for infracenter servers
        cache_key = get_cache_key("table", "infracenter_servers")
        result = get_cached_data(cache_key, fetch_infracenter_servers, timeout=1200)
        if isinstance(result, dict) and "error" in result:
            return jsonify(result), 500
        return jsonify(result)

    # Login endpoint
    @app.route('/api/login', methods=['POST'])
    def login():
        global auth_token
        try:
            # Create basic auth credentials
            credentials = base64.b64encode(b'wenle:Password123').decode('utf-8')
            
            # Make POST request to Shopee login API
            response = requests.post(
                'https://space.shopee.io/apis/uic/v2/auth/basic_login',
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Basic {credentials}'
                }
            )
            
            # Check if request was successful
            response.raise_for_status()
            
            # Store the token
            auth_token = response.json().get('token')
            
            # Return success response
            return jsonify({
                'message': 'Login successful',
                'token': auth_token
            })
            
        except requests.exceptions.RequestException as e:
            return jsonify({
                'error': 'Login failed',
                'details': str(e)
            }), 500

    @app.route('/api/health', methods=['GET'])
    def health_check():
        """Health check endpoint to monitor database connectivity"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            return_db_connection(conn)
            
            # Check connection pool status
            pool_status = {
                "min_connections": 5,
                "max_connections": 20,
                "status": "healthy"
            }
            
            return jsonify({
                "status": "healthy",
                "database": "connected",
                "connection_pool": pool_status,
                "timestamp": datetime.now().isoformat()
            }), 200
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return jsonify({
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }), 500

    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return jsonify({'error': 'Not found'}), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({'error': 'Internal server error'}), 500

    @app.route('/api/refresh_all_tables', methods=['POST'])
    def refresh_all_tables():
        """Refresh all server tables and invalidate caches"""
        try:
            # Invalidate all table caches
            invalidate_all_table_caches()
            logger.info("All table caches invalidated")
            
            # Optionally trigger a full data refresh
            refresh_data = request.json.get('refresh_data', False) if request.json else False
            
            if refresh_data:
                logger.info("Starting full data refresh...")
                try:
                    # Call the bulk refresh endpoint internally
                    with app.test_client() as client:
                        response = client.post('/api/bulk_refresh_all_servers')
                        if response.status_code == 200:
                            result = {
                                'message': 'All tables refreshed and caches invalidated',
                                'refresh_status': 'success',
                                'details': response.json
                            }
                        else:
                            result = {
                                'message': 'Cache invalidated but data refresh failed',
                                'refresh_status': 'partial',
                                'details': response.json if response.json else {'status_code': response.status_code}
                            }
                except Exception as refresh_error:
                    logger.error(f"Error during full data refresh: {str(refresh_error)}")
                    result = {
                        'message': 'Cache invalidated but data refresh encountered an error',
                        'refresh_status': 'partial',
                        'error_details': str(refresh_error),
                        'timestamp': datetime.now().isoformat()
                    }
            else:
                result = {
                    'message': 'All table caches invalidated successfully',
                    'refresh_status': 'cache_only',
                    'timestamp': datetime.now().isoformat()
                }
            
            return jsonify(result), 200
            
        except Exception as e:
            logger.error(f"Error refreshing tables: {str(e)}")
            return jsonify({
                'error': 'Failed to refresh tables',
                'details': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/refresh_reserved_servers', methods=['POST'])
    def refresh_reserved_servers():
        """Refresh the reserved servers cache by invalidating it"""
        try:
            # Invalidate the reserved servers cache
            invalidate_table_cache("reserved_servers")
            logger.info("Reserved servers cache invalidated successfully")
            print("DEBUG: Reserved servers cache invalidated successfully")
            return jsonify({
                "message": "Reserved servers cache refreshed successfully",
                "refresh_status": "success"
            }), 200
        except Exception as e:
            logger.error(f"Error refreshing reserved servers cache: {e}")
            print(f"DEBUG: Error refreshing reserved servers cache: {e}")
            return jsonify({
                "error": f"Failed to refresh reserved servers cache: {str(e)}"
            }), 500

    return app

if __name__ == '__main__':
    app = create_app()
    try:
        app.run(host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        close_connection_pool()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        close_connection_pool()
        raise 