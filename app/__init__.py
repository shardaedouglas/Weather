from flask import Flask
from config import *
from .extensions import mail, get_db, close_db, find_stations, parse_station_file
from flask import render_template, flash, redirect, url_for, jsonify, request
from flask_mail import Message
import os
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_required, current_user
from datetime import datetime
import re # Added for regex in error handling

# init SQLAlchemy 
db = SQLAlchemy()

def create_app(config_class=Config):
    app = Flask(__name__) 
    # Load the default config first
    app.config.from_object(Config)  
    # Then overwrite with function parameters
    if type(config_class) is dict:
        app.config.update(config_class)
    else: 
        app.config.from_object(config_class)

    ### SQL Alchemy Database Init
    # DB locations are defined in the config.py file.
    db.init_app(app)

    login_manager = LoginManager()
    login_manager.login_view = 'auth.login'
    login_manager.init_app(app)

    from .auth.models.auth_models import User

    @login_manager.user_loader
    def load_user(user_id):
        # Load user from JSON datastore by username (which is our user_id)
        return User.get(user_id)


    # Close the database when the context closes
    @app.teardown_appcontext
    def teardown_db(exception):
        close_db()
        
        
    # Initialize the corrections table
    @app.before_request
    def initialize_corrections_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS corrections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            correction_date DATE,
            begin_date DATE,
            end_date DATE,
            element TEXT,
            action TEXT,
            o_value TEXT,
            e_value TEXT,
            defaults BOOLEAN DEFAULT 1,
            datzilla_number TEXT
        )
        """)
        db.commit()
        
    # Initialize the GHCN data table
    @app.before_request
    def initialize_ghcn_data_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS ghcn_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            country TEXT NOT NULL,
            state TEXT,
            date DATE NOT NULL,
            type TEXT NOT NULL
        )
        """)
        db.commit()
        
    # Initialize the GHCN_metadata table
    @app.before_request
    def initialize_ghcn_metadata_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS ghcn_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            wmo_id TEXT NOT NULL,            
            state TEXT,
            country TEXT NOT NULL,
            Latitude REAL,
            Longitude REAL,
            gsn_flag TEXT,
            hcn_flag TEXT
            ghcnd_inventory TEXT
        )
        """)
        db.commit()
        
    ###### SMTP SETTINGS ######
    # Mail configuration
    app.config['MAIL_SERVER'] = Config.MAIL_SERVER
    app.config['MAIL_PORT'] = Config.MAIL_PORT
    app.config['MAIL_USERNAME'] = Config.MAIL_USERNAME
    app.config['MAIL_PASSWORD'] = Config.MAIL_PASSWORD
    app.config['MAIL_USE_TLS'] = Config.MAIL_USE_TLS
    app.config['MAIL_USE_SSL'] = False
    app.config['MAIL_DEFAULT_SENDER'] = Config.MAIL_DEFAULT_SENDER
    app.config['ERROR_REPORT_EMAIL'] = Config.ERROR_REPORT_EMAIL
    
    mail.init_app(app)
    # Register blueprints here
    from app.corrections import correction_bp as corrections_bp
    app.register_blueprint(corrections_bp)
    
    from app.ghcndata import ghcndata_bp as ghcndata_bp
    app.register_blueprint(ghcndata_bp)

    from app.resources import resources_bp as resources_bp
    app.register_blueprint(resources_bp)

    from app.utilities import utilities_bp as utilities_bp
    app.register_blueprint(utilities_bp)
    
    from app.auth import auth_bp as auth_bp
    app.register_blueprint(auth_bp)



    # Error handlers for detailed error information
    @app.errorhandler(400)
    def bad_request(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        # Extract concise error message and errno
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '400'
        return jsonify({
            'error': concise_error if concise_error else 'Bad Request',
            'message': 'The request was invalid. Please check your input and try again.',
            'details': str(error),
            'error_code': 'BAD_REQUEST',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 400

    @app.errorhandler(401)
    def unauthorized(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '401'
        return jsonify({
            'error': concise_error if concise_error else 'Unauthorized',
            'message': 'You are not authorized to perform this action. Please log in.',
            'details': str(error),
            'error_code': 'UNAUTHORIZED',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 401

    @app.errorhandler(403)
    def forbidden(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '403'
        return jsonify({
            'error': concise_error if concise_error else 'Forbidden',
            'message': 'You do not have permission to perform this action.',
            'details': str(error),
            'error_code': 'FORBIDDEN',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 403

    @app.errorhandler(404)
    def not_found(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '404'
        return jsonify({
            'error': concise_error if concise_error else 'Not Found',
            'message': 'The requested resource was not found.',
            'details': str(error),
            'error_code': 'NOT_FOUND',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 404

    @app.errorhandler(422)
    def unprocessable_entity(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '422'
        return jsonify({
            'error': concise_error if concise_error else 'Validation Error',
            'message': 'Please check your input and try again.',
            'details': str(error),
            'error_code': 'VALIDATION_ERROR',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 422

    @app.errorhandler(500)
    def internal_server_error(error):
        import traceback
        error_description = str(error)
        
        # Extract concise error message and errno
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '500'
        
        return jsonify({
            'error': concise_error if concise_error else 'Internal Server Error',
            'message': 'A server error occurred. Please try again later.',
            'details': str(error),
            'error_code': 'INTERNAL_SERVER_ERROR',
            'errno': errno,
            'timestamp': datetime.now().isoformat(),
            'stack_trace': traceback.format_exc() if app.debug else None
        }), 500

    @app.errorhandler(502)
    def bad_gateway(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '502'
        return jsonify({
            'error': concise_error if concise_error else 'Bad Gateway',
            'message': 'The server received an invalid response from an upstream server.',
            'details': str(error),
            'error_code': 'BAD_GATEWAY',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 502

    @app.errorhandler(503)
    def service_unavailable(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '503'
        return jsonify({
            'error': concise_error if concise_error else 'Service Unavailable',
            'message': 'The service is temporarily unavailable. Please try again later.',
            'details': str(error),
            'error_code': 'SERVICE_UNAVAILABLE',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 503

    @app.errorhandler(504)
    def gateway_timeout(error):
        error_description = str(error.description) if hasattr(error, 'description') else str(error)
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '504'
        return jsonify({
            'error': concise_error if concise_error else 'Gateway Timeout',
            'message': 'The server did not receive a timely response from an upstream server.',
            'details': str(error),
            'error_code': 'GATEWAY_TIMEOUT',
            'errno': errno,
            'timestamp': datetime.now().isoformat()
        }), 504

    @app.errorhandler(Exception)
    def handle_exception(error):
        import traceback
        error_description = str(error)
        
        # Extract concise error message and errno
        result = extract_concise_error(error_description)
        if result:
            concise_error, errno = result
        else:
            concise_error, errno = None, '1'
        
        return jsonify({
            'error': concise_error if concise_error else 'Unexpected Error',
            'message': 'An unexpected error occurred. Please try again later.',
            'details': str(error),
            'error_code': 'UNEXPECTED_ERROR',
            'errno': errno,
            'timestamp': datetime.now().isoformat(),
            'stack_trace': traceback.format_exc() if app.debug else None
        }), 500

    def extract_concise_error(error_description):
        """Extract a concise, user-friendly error message from error description"""
        if not error_description:
            return None
            
        import re
        
        # Define error patterns with specific, helpful messages and errno
        error_patterns = [
            (r'FileNotFoundError: (.+)', 'The requested file could not be found on the server', '2'),
            (r'PermissionError: (.+)', 'You do not have permission to access this resource', '13'),
            (r'ValueError: (.+)', 'The server received invalid data that could not be processed', '22'),
            (r'KeyError: (.+)', 'Required data is missing from the server request', '2'),
            (r'ConnectionError: (.+)', 'The server cannot connect to required external services', '111'),
            (r'TimeoutError: (.+)', 'The server request took too long and timed out', '110'),
            (r'DatabaseError: (.+)', 'The server database is currently unavailable', '2002'),
            (r'ValidationError: (.+)', 'The data sent to the server is not in the correct format', '22'),
            (r'AuthenticationError: (.+)', 'Your login session has expired or is invalid', '13'),
            (r'ImportError: (.+)', 'A required server component is missing or corrupted', '2'),
            (r'AttributeError: (.+)', 'A server component is not properly configured', '2'),
            (r'TypeError: (.+)', 'The server received data in an unexpected format', '22'),
            (r'IndexError: (.+)', 'The server tried to access data that does not exist', '22'),
            (r'OSError: (.+)', 'The server encountered a system-level problem', '1'),
            (r'IOError: (.+)', 'The server cannot read or write required files', '5'),
            (r'MemoryError: (.+)', 'The server has run out of available memory', '12'),
            (r'DiskSpaceError: (.+)', 'The server has run out of disk space', '28'),
            (r'NetworkError: (.+)', 'The server cannot communicate over the network', '101'),
            (r'ConfigurationError: (.+)', 'The server is not properly configured', '22'),
            (r'Exception: (.+)', 'An unexpected error occurred on the server', '1'),
            (r'Error: (.+)', 'An unexpected error occurred on the server', '1')
        ]
        
        for pattern, specific_message, errno in error_patterns:
            match = re.search(pattern, error_description, re.IGNORECASE)
            if match:
                return specific_message, errno
        
        # If no pattern matches, try to extract a specific message from the first line
        lines = [line.strip() for line in error_description.split('\n') 
                if line.strip() and 
                not line.startswith('Traceback') and 
                not line.startswith('File "') and
                'line ' not in line and
                len(line.strip()) > 5 and
                len(line.strip()) < 100]
        
        if lines:
            specific_error = lines[0].strip()
            # Remove common prefixes
            specific_error = re.sub(r'^(Error|Exception|Warning):\s*', '', specific_error, flags=re.IGNORECASE)
            # Remove quotes
            specific_error = re.sub(r'^[\'"]|[\'"]$', '', specific_error)
            # Make it more user-friendly
            if 'not found' in specific_error.lower():
                specific_error = 'The requested resource could not be found on the server'
            elif 'permission' in specific_error.lower():
                specific_error = 'You do not have permission to access this resource'
            elif 'invalid' in specific_error.lower():
                specific_error = 'The server received invalid data that could not be processed'
            elif 'connection' in specific_error.lower():
                specific_error = 'The server cannot connect to required external services'
            elif 'timeout' in specific_error.lower():
                specific_error = 'The server request took too long and timed out'
            elif 'database' in specific_error.lower():
                specific_error = 'The server database is currently unavailable'
            elif 'memory' in specific_error.lower():
                specific_error = 'The server has run out of available memory'
            elif 'disk' in specific_error.lower():
                specific_error = 'The server has run out of disk space'
            elif 'network' in specific_error.lower():
                specific_error = 'The server cannot communicate over the network'
            elif 'config' in specific_error.lower():
                specific_error = 'The server is not properly configured'
            else:
                # Generic but helpful message
                specific_error = 'An unexpected error occurred on the server'
            
            return specific_error, '1'  # Default errno for unknown errors
        
        return None, None


    @app.route('/test_error_popup')
    @login_required
    def test_error_popup():
        return render_template('test_error_popup.html')
    
    @app.route('/test_flash_error')
    @login_required
    def test_flash_error():
        flash('This is a test error flash message!', 'error')
        return redirect(url_for('test_error_popup'))
    
    @app.route('/test_flash_success')
    @login_required
    def test_flash_success():
        flash('This is a test success flash message!', 'success')
        return redirect(url_for('test_error_popup'))
    
    @app.route('/test_flash_warning')
    @login_required
    def test_flash_warning():
        flash('This is a test warning flash message!', 'warning')
        return redirect(url_for('test_error_popup'))
    
    @app.route('/test_flash_info')
    @login_required
    def test_flash_info():
        flash('This is a test info flash message!', 'info')
        return redirect(url_for('test_error_popup'))
    
    # Test routes for detailed error handling
    @app.route('/test_server_error')
    @login_required
    def test_server_error():
        """Test route that triggers a 500 error"""
        raise FileNotFoundError("The requested file '/path/to/missing/file.txt' could not be found (errno 2)")
    
    @app.route('/test_validation_error')
    @login_required
    def test_validation_error():
        """Test route that triggers a 422 error"""
        from flask import abort
        abort(422, description="The email field is required and must be a valid email address (errno 22)")
    
    @app.route('/test_not_found')
    @login_required
    def test_not_found():
        """Test route that triggers a 404 error"""
        from flask import abort
        abort(404, description="The requested user with ID 12345 does not exist (errno 2)")
    
    @app.route('/test_bad_request')
    @login_required
    def test_bad_request():
        """Test route that triggers a 400 error"""
        from flask import abort
        abort(400, description="The 'username' parameter is required but was not provided (errno 22)")
    
    @app.route('/test_permission_error')
    @login_required
    def test_permission_error():
        """Test route that triggers a permission error"""
        raise PermissionError("You do not have sufficient privileges to access this resource (errno 13)")
    
    @app.route('/test_value_error')
    @login_required
    def test_value_error():
        """Test route that triggers a value error"""
        raise ValueError("The provided age value 'abc' is not a valid number (errno 22)")
    
    @app.route('/test_connection_error')
    @login_required
    def test_connection_error():
        """Test route that triggers a connection error"""
        raise ConnectionError("Unable to connect to the database server at localhost:5432 (errno 111)")
    
    @app.route('/test_timeout_error')
    @login_required
    def test_timeout_error():
        """Test route that triggers a timeout error"""
        raise TimeoutError("The request timed out after 30 seconds (errno 110)")
    
    @app.route('/test_database_error')
    @login_required
    def test_database_error():
        """Test route that triggers a database error"""
        raise Exception("Database connection failed: Unable to connect to MySQL server (errno 2002)")
    
    @app.route('/test_memory_error')
    @login_required
    def test_memory_error():
        """Test route that triggers a memory error"""
        raise MemoryError("Server has run out of available memory (errno 12)")
    
    @app.route('/test_disk_space_error')
    @login_required
    def test_disk_space_error():
        """Test route that triggers a disk space error"""
        raise OSError("No space left on device (errno 28)")
    
    @app.route('/test_network_error')
    @login_required
    def test_network_error():
        """Test route that triggers a network error"""
        raise ConnectionError("Network is unreachable (errno 101)")
    
    @app.route('/test_config_error')
    @login_required
    def test_config_error():
        """Test route that triggers a configuration error"""
        raise ValueError("Invalid configuration: missing required setting 'database_url' (errno 22)")
    
    @app.route('/test_email_report')
    @login_required
    def test_email_report():
        """Test route to verify email reporting functionality"""
        try:
            # Create a test error report - commented out due to missing flask_mail
            # msg = Message(
            #     subject="Test Error Report",
            #     recipients=[app.config['ERROR_REPORT_EMAIL']],
            #     sender=app.config['MAIL_DEFAULT_SENDER']
            # )
            
            # msg.body = """
            # Test Error Report
            # ================
            # 
            # This is a test email to verify that the error reporting system is working correctly.
            # 
            # Timestamp: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
            # Test Type: Email Functionality Test
            # Status: SUCCESS
            # 
            # If you receive this email, the error reporting system is properly configured and working.
            # """
            
            # mail.send(msg)  # Commented out due to missing flask_mail
            
            return jsonify({
                'success': True,
                'message': 'Email functionality disabled due to missing flask_mail dependency'
            }), 200
            
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Failed to send test email: {str(e)}'
            }), 500
    
    @app.route('/report_error', methods=['POST'])
    @login_required
    def report_error():
        """Handle error report submissions via email"""
        try:
            data = request.get_json()
            
            # Debug logging
            print(f"Error report received: {data}")
            print(f"Mail config - Server: {app.config.get('MAIL_SERVER')}")
            print(f"Mail config - Username: {app.config.get('MAIL_USERNAME')}")
            print(f"Mail config - Default Sender: {app.config.get('MAIL_DEFAULT_SENDER')}")
            print(f"Mail config - Error Report Email: {app.config.get('ERROR_REPORT_EMAIL')}")
            
            # Extract error information
            error_title = data.get('title', 'Unknown Error')
            error_message = data.get('message', 'No message provided')
            error_details = data.get('details', {})
            user_agent = request.headers.get('User-Agent', 'Unknown')
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Get user information if available
            user_info = "Anonymous User"
            if not current_user.is_anonymous:
                user_info = f"User: {current_user.username if hasattr(current_user, 'username') else 'Unknown'}"
            
            # Create email message - commented out due to missing flask_mail
            # msg = Message(
            #     subject=f"Error Report: {error_title}",
            #     recipients=[app.config['ERROR_REPORT_EMAIL']],
            #     sender=app.config['MAIL_DEFAULT_SENDER']
            # )
            
            # Create email body
            email_body = f"""
Error Report Details
==================

Timestamp: {timestamp}
User: {user_info}
User Agent: {user_agent}
Page URL: {data.get('url', 'Unknown')}

Error Information:
-----------------
Title: {error_title}
Message: {error_message}

Technical Details:
------------------
HTTP Status: {error_details.get('status', 'Unknown')}
Error Code: {error_details.get('error_code', 'Unknown')}
Error Number: {error_details.get('errno', 'Unknown')}

Request Information:
-------------------
URL: {error_details.get('url', 'Unknown')}
Method: {error_details.get('method', 'Unknown')}
Timestamp: {error_details.get('timestamp', 'Unknown')}

Server Details:
---------------
{error_details.get('serverDetails', 'No server details available')}

Stack Trace:
-----------
{error_details.get('stackTrace', 'No stack trace available')}

Response Data:
--------------
{error_details.get('responseData', 'No response data available')}

Browser Information:
-------------------
{error_details.get('browserInfo', 'No browser information available')}
"""
            
            # msg.body = email_body
            
            # Send email - commented out due to missing flask_mail
            # mail.send(msg)
            
            return jsonify({
                'success': True,
                'message': 'Error report functionality disabled due to missing flask_mail dependency'
            }), 200
            
        except Exception as e:
            # Enhanced error logging
            print(f"Error sending email report: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            
            return jsonify({
                'success': False,
                'message': f'Failed to send error report: {str(e)}'
            }), 500


    @app.route('/health')
    def health_check():
        """Health check endpoint for load balancers and monitoring"""
        try:
            # Check database connection
            db = get_db()
            db.execute("SELECT 1")
            
            # Check mail configuration
            mail_configured = all([
                app.config.get('MAIL_SERVER'),
                app.config.get('MAIL_USERNAME'),
                app.config.get('MAIL_PASSWORD')
            ])
            
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0',
                'checks': {
                    'database': 'ok',
                    'mail': 'ok' if mail_configured else 'warning'
                }
            }), 200
            
        except Exception as e:
            return jsonify({
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }), 503

    @app.route('/')
    def index():
        """Root route - redirect to login if not authenticated, otherwise to corrections"""
        if current_user.is_authenticated:
            return redirect(url_for('corrections.index'))
        else:
            return redirect(url_for('auth.login'))

    @app.route('/test')
    @login_required
    def test_page():
        # return '<h1>Test</h1>' # Link html templates here instead..
        # return render_template('test.html')
        if (current_user.is_anonymous):
            name = "Guest"
        else:
            name = current_user.username
        return render_template('test.html', name=name)


    return app