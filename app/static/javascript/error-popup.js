/**
 * Error Popup System for Weather Application
 * Provides modern, user-friendly error notifications with different types and animations
 */

class ErrorPopup {
    constructor() {
        this.container = null;
        this.init();
    }

    init() {
        // Create the error popup container if it doesn't exist
        if (!document.getElementById('error-popup-container')) {
            this.createContainer();
        }
        this.container = document.getElementById('error-popup-container');
    }

    createContainer() {
        const container = document.createElement('div');
        container.id = 'error-popup-container';
        container.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
            max-width: 400px;
            width: 100%;
        `;
        document.body.appendChild(container);
    }

    /**
     * Show an error popup
     * @param {string} message - The error message to display
     * @param {string} type - Type of error: 'error', 'warning', 'info', 'success'
     * @param {Object} options - Additional options
     */
    show(message, type = 'error', options = {}) {
        const config = {
            duration: options.duration || 5000,
            closable: options.closable !== false,
            autoClose: options.autoClose !== false,
            title: options.title || this.getDefaultTitle(type),
            icon: options.icon || this.getDefaultIcon(type),
            ...options
        };

        const popup = this.createPopup(message, type, config);
        this.container.appendChild(popup);

        // Add action button event listeners
        if (config.actions) {
            this.addActionListeners(popup, config.actions, config);
        }

        // Animate in
        setTimeout(() => {
            popup.classList.add('show');
        }, 10);

        // Auto close if enabled
        if (config.autoClose && config.duration > 0) {
            setTimeout(() => {
                this.close(popup);
            }, config.duration);
        }

        return popup;
    }

    createPopup(message, type, config) {
        const popup = document.createElement('div');
        popup.className = `error-popup error-popup-${type}`;
        
        // Create detailed error information if available
        const detailedInfo = config.detailedInfo ? this.createDetailedInfo(config.detailedInfo) : '';
        
        popup.innerHTML = `
            <div class="error-popup-content">
                <div class="error-popup-header">
                    <div class="error-popup-icon">
                        <i class="${config.icon}"></i>
                    </div>
                    <div class="error-popup-title">${config.title}</div>
                    ${config.closable ? '<button class="error-popup-close" type="button">&times;</button>' : ''}
                </div>
                <div class="error-popup-body">
                    ${message}
                    ${detailedInfo}
                </div>
                ${config.actions ? this.createActions(config.actions) : ''}
            </div>
        `;

        // Add close button event listener
        if (config.closable) {
            const closeBtn = popup.querySelector('.error-popup-close');
            closeBtn.addEventListener('click', () => this.close(popup));
        }

        // Add click outside to close
        popup.addEventListener('click', (e) => {
            if (e.target === popup) {
                this.close(popup);
            }
        });

        return popup;
    }

    createDetailedInfo(detailedInfo) {
        const detailsId = 'error-details-' + Math.random().toString(36).substr(2, 9);
        
        let detailsHtml = `
            <div class="error-popup-details">
                <button class="error-popup-details-toggle" type="button" data-bs-toggle="collapse" data-bs-target="#${detailsId}" aria-expanded="false" aria-controls="${detailsId}">
                    <i class="fas fa-chevron-down"></i>
                    <span>Show Details</span>
                </button>
                <div class="collapse" id="${detailsId}">
                    <div class="error-popup-details-content">
        `;
        
        if (detailedInfo.status) {
            detailsHtml += `<div class="error-detail-item"><strong>Status:</strong> ${detailedInfo.status}</div>`;
        }
        
        if (detailedInfo.statusText) {
            detailsHtml += `<div class="error-detail-item"><strong>Status Text:</strong> ${detailedInfo.statusText}</div>`;
        }
        
        if (detailedInfo.url) {
            detailsHtml += `<div class="error-detail-item"><strong>URL:</strong> ${detailedInfo.url}</div>`;
        }
        
        if (detailedInfo.method) {
            detailsHtml += `<div class="error-detail-item"><strong>Method:</strong> ${detailedInfo.method}</div>`;
        }
        
        if (detailedInfo.timestamp) {
            detailsHtml += `<div class="error-detail-item"><strong>Time:</strong> ${detailedInfo.timestamp}</div>`;
        }
        
        if (detailedInfo.errorCode) {
            detailsHtml += `<div class="error-detail-item"><strong>Error Code:</strong> ${detailedInfo.errorCode}</div>`;
        }
        
        if (detailedInfo.stackTrace) {
            detailsHtml += `<div class="error-detail-item"><strong>Stack Trace:</strong><pre class="error-stack-trace">${detailedInfo.stackTrace}</pre></div>`;
        }
        
        if (detailedInfo.responseText) {
            detailsHtml += `<div class="error-detail-item"><strong>Response:</strong><pre class="error-response">${detailedInfo.responseText}</pre></div>`;
        }
        
        if (detailedInfo.requestData) {
            detailsHtml += `<div class="error-detail-item"><strong>Request Data:</strong><pre class="error-request-data">${detailedInfo.requestData}</pre></div>`;
        }
        
        detailsHtml += `
                    </div>
                </div>
            </div>
        `;
        
        return detailsHtml;
    }

    createActions(actions) {
        let actionsHtml = '<div class="error-popup-actions">';
        actions.forEach(action => {
            actionsHtml += `<button class="error-popup-btn error-popup-btn-${action.type || 'secondary'}" data-action="${action.action}">${action.text}</button>`;
        });
        actionsHtml += '</div>';
        return actionsHtml;
    }

    addActionListeners(popup, actions, config) {
        const actionButtons = popup.querySelectorAll('[data-action]');
        actionButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                e.stopPropagation();
                const action = e.target.getAttribute('data-action');
                this.handleAction(action, config, popup);
            });
        });
    }

    handleAction(action, config, popup) {
        switch (action) {
            case 'retry':
                // Close the popup and trigger a page reload or retry
                this.close(popup);
                if (typeof window.handleAjaxRetry === 'function') {
                    window.handleAjaxRetry();
                } else {
                    // Fallback: reload the page
                    window.location.reload();
                }
                break;
                
            case 'report':
                // Send error report via email
                this.sendErrorReport(config, popup);
                break;
                
            case 'close':
                // Simply close the popup
                this.close(popup);
                break;
                
            case 'cancel':
                // Close the popup without any action
                this.close(popup);
                break;
                
            default:
                console.log('Unknown action:', action);
        }
    }

    sendErrorReport(config, popup) {
        // Show loading state
        const reportBtn = popup.querySelector('[data-action="report"]');
        const originalText = reportBtn.textContent;
        reportBtn.textContent = 'Sending...';
        reportBtn.disabled = true;

        // Prepare error data
        const errorData = {
            title: config.title || 'Unknown Error',
            message: config.message || 'No message provided',
            details: config.detailedInfo || {},
            url: window.location.href,
            timestamp: new Date().toISOString(),
            userAgent: navigator.userAgent
        };

        // Send error report via AJAX
        fetch('/report_error', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(errorData)
        })
        .then(response => {
            console.log('Response status:', response.status);
            console.log('Response headers:', response.headers);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return response.json();
        })
        .then(data => {
            console.log('Response data:', data);
            
            if (data.success) {
                // Show success message
                reportBtn.textContent = 'Report Sent!';
                reportBtn.classList.add('error-popup-btn-success');
                
                // Show success popup
                this.success('Error report sent successfully!', {
                    duration: 3000,
                    title: 'Report Sent'
                });
                
                // Close the error popup after a delay
                setTimeout(() => {
                    this.close(popup);
                }, 2000);
            } else {
                throw new Error(data.message || 'Failed to send report');
            }
        })
        .catch(error => {
            console.error('Error sending report:', error);
            console.error('Error details:', error.message);
            
            // Reset button state
            reportBtn.textContent = originalText;
            reportBtn.disabled = false;
            
            // Show error message with more details
            this.error(`Failed to send error report: ${error.message}`, {
                duration: 5000,
                title: 'Report Failed'
            });
        });
    }

    close(popup) {
        if (popup && popup.parentNode) {
            popup.classList.add('closing');
            setTimeout(() => {
                if (popup.parentNode) {
                    popup.parentNode.removeChild(popup);
                }
            }, 300);
        }
    }

    closeAll() {
        const popups = this.container.querySelectorAll('.error-popup');
        popups.forEach(popup => this.close(popup));
    }

    getDefaultTitle(type) {
        const titles = {
            error: 'Error',
            warning: 'Warning',
            info: 'Information',
            success: 'Success'
        };
        return titles[type] || 'Notification';
    }

    getDefaultIcon(type) {
        const icons = {
            error: 'fas fa-exclamation-circle',
            warning: 'fas fa-exclamation-triangle',
            info: 'fas fa-info-circle',
            success: 'fas fa-check-circle'
        };
        return icons[type] || 'fas fa-info-circle';
    }

    // Convenience methods for different error types
    error(message, options = {}) {
        return this.show(message, 'error', options);
    }

    warning(message, options = {}) {
        return this.show(message, 'warning', options);
    }

    info(message, options = {}) {
        return this.show(message, 'info', options);
    }

    success(message, options = {}) {
        return this.show(message, 'success', options);
    }
}

// Create global instance
window.errorPopup = new ErrorPopup();

// Enhanced AJAX error handling
window.handleAjaxError = function(xhr, status, error, customMessage = null) {
    let message = customMessage || 'An unexpected error occurred. Please try again.';
    let title = 'Request Failed';
    let detailedInfo = {};
    
    // Extract detailed information from the XHR object
    if (xhr) {
        detailedInfo.status = xhr.status;
        detailedInfo.statusText = xhr.statusText;
        detailedInfo.url = xhr.responseURL || window.location.href;
        detailedInfo.method = xhr.method || 'GET';
        detailedInfo.timestamp = new Date().toLocaleString();
        
        // Try to get response text
        if (xhr.responseText) {
            detailedInfo.responseText = xhr.responseText;
        }
        
        // Try to get request data if available
        if (xhr.requestData) {
            detailedInfo.requestData = typeof xhr.requestData === 'string' ? xhr.requestData : JSON.stringify(xhr.requestData);
        }
    }
    
    // Extract user-friendly error highlights from server response
    let errorHighlight = null;
    let errorTitle = null;
    if (xhr && xhr.responseText) {
        try {
            const response = JSON.parse(xhr.responseText);
            if (response.error) {
                errorHighlight = response.error;
            }
            if (response.errno) {
                errorTitle = `Error ${response.errno}`;
                detailedInfo.errno = response.errno;
            }
            if (response.details) {
                detailedInfo.serverDetails = response.details;
            }
            if (response.stack_trace) {
                detailedInfo.stackTrace = response.stack_trace;
            }
        } catch (e) {
            // If not JSON, try to extract meaningful error from text
            const responseText = xhr.responseText;
            if (responseText) {
                // Look for errno patterns first
                const errnoPattern = /errno\s*(\d+)/i;
                const errnoMatch = responseText.match(errnoPattern);
                if (errnoMatch) {
                    const errno = errnoMatch[1];
                    errorTitle = `Error ${errno}`;
                    detailedInfo.errno = errno;
                }
                
                // Look for common error patterns and extract specific messages
                const errorPatterns = [
                    { pattern: /FileNotFoundError: (.+)/i, message: 'The requested file could not be found on the server', errno: '2' },
                    { pattern: /PermissionError: (.+)/i, message: 'You do not have permission to access this resource', errno: '13' },
                    { pattern: /ValueError: (.+)/i, message: 'The server received invalid data that could not be processed', errno: '22' },
                    { pattern: /KeyError: (.+)/i, message: 'Required data is missing from the server request', errno: '2' },
                    { pattern: /ConnectionError: (.+)/i, message: 'The server cannot connect to required external services', errno: '111' },
                    { pattern: /TimeoutError: (.+)/i, message: 'The server request took too long and timed out', errno: '110' },
                    { pattern: /DatabaseError: (.+)/i, message: 'The server database is currently unavailable', errno: '2002' },
                    { pattern: /ValidationError: (.+)/i, message: 'The data sent to the server is not in the correct format', errno: '22' },
                    { pattern: /AuthenticationError: (.+)/i, message: 'Your login session has expired or is invalid', errno: '13' },
                    { pattern: /ImportError: (.+)/i, message: 'A required server component is missing or corrupted', errno: '2' },
                    { pattern: /AttributeError: (.+)/i, message: 'A server component is not properly configured', errno: '2' },
                    { pattern: /TypeError: (.+)/i, message: 'The server received data in an unexpected format', errno: '22' },
                    { pattern: /IndexError: (.+)/i, message: 'The server tried to access data that does not exist', errno: '22' },
                    { pattern: /OSError: (.+)/i, message: 'The server encountered a system-level problem', errno: '1' },
                    { pattern: /IOError: (.+)/i, message: 'The server cannot read or write required files', errno: '5' },
                    { pattern: /MemoryError: (.+)/i, message: 'The server has run out of available memory', errno: '12' },
                    { pattern: /DiskSpaceError: (.+)/i, message: 'The server has run out of disk space', errno: '28' },
                    { pattern: /NetworkError: (.+)/i, message: 'The server cannot communicate over the network', errno: '101' },
                    { pattern: /ConfigurationError: (.+)/i, message: 'The server is not properly configured', errno: '22' },
                    { pattern: /Exception: (.+)/i, message: 'An unexpected error occurred on the server', errno: '1' },
                    { pattern: /Error: (.+)/i, message: 'An unexpected error occurred on the server', errno: '1' }
                ];
                
                for (const { pattern, message, errno } of errorPatterns) {
                    const match = responseText.match(pattern);
                    if (match) {
                        errorHighlight = message;
                        if (!errorTitle) {
                            errorTitle = `Error ${errno}`;
                            detailedInfo.errno = errno;
                        }
                        break;
                    }
                }
                
                // If no pattern matches, try to get a specific error message
                if (!errorHighlight) {
                    const lines = responseText.split('\n').filter(line => 
                        line.trim() && 
                        !line.includes('Traceback') && 
                        !line.includes('File "') &&
                        !line.includes('line ') &&
                        line.trim().length > 5 &&
                        line.trim().length < 100
                    );
                    if (lines.length > 0) {
                        // Take the first meaningful line and make it specific
                        let specificError = lines[0].trim();
                        // Remove common prefixes
                        specificError = specificError.replace(/^(Error|Exception|Warning):\s*/i, '');
                        // Remove quotes
                        specificError = specificError.replace(/^['"]|['"]$/g, '');
                        // Make it more user-friendly and specific
                        if (specificError.toLowerCase().includes('not found')) {
                            specificError = 'The requested resource could not be found on the server';
                        } else if (specificError.toLowerCase().includes('permission')) {
                            specificError = 'You do not have permission to access this resource';
                        } else if (specificError.toLowerCase().includes('invalid')) {
                            specificError = 'The server received invalid data that could not be processed';
                        } else if (specificError.toLowerCase().includes('connection')) {
                            specificError = 'The server cannot connect to required external services';
                        } else if (specificError.toLowerCase().includes('timeout')) {
                            specificError = 'The server request took too long and timed out';
                        } else if (specificError.toLowerCase().includes('database')) {
                            specificError = 'The server database is currently unavailable';
                        } else if (specificError.toLowerCase().includes('memory')) {
                            specificError = 'The server has run out of available memory';
                        } else if (specificError.toLowerCase().includes('disk')) {
                            specificError = 'The server has run out of disk space';
                        } else if (specificError.toLowerCase().includes('network')) {
                            specificError = 'The server cannot communicate over the network';
                        } else if (specificError.toLowerCase().includes('config')) {
                            specificError = 'The server is not properly configured';
                        } else {
                            // Generic but helpful message
                            specificError = 'An unexpected error occurred on the server';
                        }
                        errorHighlight = specificError;
                    }
                }
            }
        }
    }
    
    if (xhr && xhr.status) {
        switch (xhr.status) {
            case 400:
                title = 'Bad Request';
                message = customMessage || 'The request was invalid. Please check your input and try again.';
                detailedInfo.errorCode = 'BAD_REQUEST';
                break;
            case 401:
                title = 'Unauthorized';
                message = customMessage || 'You are not authorized to perform this action. Please log in.';
                detailedInfo.errorCode = 'UNAUTHORIZED';
                break;
            case 403:
                title = 'Forbidden';
                message = customMessage || 'You do not have permission to perform this action.';
                detailedInfo.errorCode = 'FORBIDDEN';
                break;
            case 404:
                title = 'Not Found';
                message = customMessage || 'The requested resource was not found.';
                detailedInfo.errorCode = 'NOT_FOUND';
                break;
            case 422:
                title = 'Validation Error';
                message = customMessage || 'Please check your input and try again.';
                detailedInfo.errorCode = 'VALIDATION_ERROR';
                break;
            case 500:
                title = 'Internal Server Error';
                message = customMessage || 'A server error occurred. Please try again later.';
                detailedInfo.errorCode = 'INTERNAL_SERVER_ERROR';
                break;
            case 502:
                title = 'Bad Gateway';
                message = customMessage || 'The server received an invalid response from an upstream server.';
                detailedInfo.errorCode = 'BAD_GATEWAY';
                break;
            case 503:
                title = 'Service Unavailable';
                message = customMessage || 'The service is temporarily unavailable. Please try again later.';
                detailedInfo.errorCode = 'SERVICE_UNAVAILABLE';
                break;
            case 504:
                title = 'Gateway Timeout';
                message = customMessage || 'The server did not receive a timely response from an upstream server.';
                detailedInfo.errorCode = 'GATEWAY_TIMEOUT';
                break;
        }
    }

    // Use error highlight if available and more specific than the default message
    if (errorHighlight && !customMessage) {
        // Clean up the error highlight to be more user-friendly
        let cleanHighlight = errorHighlight
            .replace(/^['"]|['"]$/g, '') // Remove surrounding quotes
            .replace(/^Error:\s*/i, '') // Remove "Error:" prefix
            .replace(/^Exception:\s*/i, '') // Remove "Exception:" prefix
            .trim();
        
        // If the highlight is meaningful and not too technical, use it
        if (cleanHighlight.length > 0 && cleanHighlight.length < 100 && 
            !cleanHighlight.includes('Traceback') && 
            !cleanHighlight.includes('File "') &&
            !cleanHighlight.includes('line ')) {
            message = cleanHighlight;
        }
    }

    // Use error title if available, otherwise use default title
    const finalTitle = errorTitle || title;

    // Add JavaScript error information if available
    if (error && error.stack) {
        detailedInfo.stackTrace = error.stack;
    }

    window.errorPopup.error(message, {
        title: finalTitle,
        duration: 10000,
        closable: true,
        detailedInfo: detailedInfo,
        actions: [
            {
                text: 'Retry',
                type: 'primary',
                action: 'retry'
            },
            {
                text: 'Report Issue',
                type: 'secondary',
                action: 'report'
            }
        ]
    });
};

// Enhanced success handling
window.handleAjaxSuccess = function(message, options = {}) {
    window.errorPopup.success(message, {
        duration: 3000,
        ...options
    });
};

// Form validation error handler
window.handleValidationError = function(errors) {
    if (typeof errors === 'string') {
        window.errorPopup.warning(errors);
    } else if (Array.isArray(errors)) {
        errors.forEach(error => {
            window.errorPopup.warning(error);
        });
    } else if (typeof errors === 'object') {
        Object.keys(errors).forEach(field => {
            const fieldErrors = Array.isArray(errors[field]) ? errors[field] : [errors[field]];
            fieldErrors.forEach(error => {
                window.errorPopup.warning(`${field}: ${error}`);
            });
        });
    }
};

// Network error handler
window.handleNetworkError = function() {
    window.errorPopup.error('Network connection error. Please check your internet connection and try again.', {
        title: 'Connection Error',
        duration: 10000,
        actions: [
            {
                text: 'Retry',
                type: 'primary',
                action: 'retry'
            }
        ]
    });
};

// Replace all alert() calls with error popup
window.originalAlert = window.alert;
window.alert = function(message) {
    window.errorPopup.info(message, {
        title: 'Alert',
        duration: 5000
    });
};

// Console error handler for unhandled errors
window.addEventListener('error', function(event) {
    if (event.error && !event.error.handled) {
        window.errorPopup.error(`An unexpected error occurred: ${event.error.message}`, {
            title: 'JavaScript Error',
            duration: 10000,
            closable: true
        });
    }
});

// Unhandled promise rejection handler
window.addEventListener('unhandledrejection', function(event) {
    window.errorPopup.error(`An unexpected error occurred: ${event.reason}`, {
        title: 'Promise Rejection',
        duration: 10000,
        closable: true
    });
});
