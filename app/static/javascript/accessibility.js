/**
 * Global Accessibility JavaScript
 * Ensures accessibility settings apply across the entire site
 */

// Global accessibility state
window.AccessibilitySettings = {
    initialized: false,
    fontResize: 0,
    isDarkMode: false,
    isHighContrast: false,
    isReduceMotion: false,
    tooltipsEnabled: false,
    enhancedFocus: true
};

// Initialize accessibility settings when DOM is ready
$(document).ready(function() {
    initializeGlobalAccessibility();
});

/**
 * Initialize all global accessibility settings
 */
function initializeGlobalAccessibility() {
    if (window.AccessibilitySettings.initialized) {
        return;
    }

    // Load settings from cookies
    loadAccessibilitySettings();
    
    // Apply all settings
    applyAccessibilitySettings();
    
    // Set up global keyboard shortcuts
    setupGlobalKeyboardShortcuts();
    
    // Initialize tooltips
    initializeTooltips();
    
    window.AccessibilitySettings.initialized = true;
}

/**
 * Load accessibility settings from cookies
 */
function loadAccessibilitySettings() {
    // Font size
    var fontResize = readCookie("font-resize");
    window.AccessibilitySettings.fontResize = fontResize ? parseInt(fontResize) : 0;
    
    // Dark mode
    window.AccessibilitySettings.isDarkMode = readCookie("theme") === "dark";
    
    // High contrast
    window.AccessibilitySettings.isHighContrast = readCookie("high-contrast") === "true";
    
    // Reduce motion
    window.AccessibilitySettings.isReduceMotion = readCookie("reduce-motion") === "true";
    
    // Tooltips
    window.AccessibilitySettings.tooltipsEnabled = readCookie("tooltips") === "true";
    
    // Enhanced focus
    window.AccessibilitySettings.enhancedFocus = readCookie("focus-indicators") !== "false";
}

/**
 * Apply all accessibility settings to the page
 */
function applyAccessibilitySettings() {
    // Apply font size
    if (window.AccessibilitySettings.fontResize !== 0) {
        $("html").css("font-size", (100 + window.AccessibilitySettings.fontResize) + "%");
    }
    
    // Apply dark mode
    if (window.AccessibilitySettings.isDarkMode) {
        $("html").addClass("dark-mode");
    }
    
    // Apply high contrast
    if (window.AccessibilitySettings.isHighContrast) {
        $("html").addClass("high-contrast");
    }
    
    // Apply reduce motion
    if (window.AccessibilitySettings.isReduceMotion) {
        $("html").addClass("reduce-motion");
    }
    
    // Apply enhanced focus
    if (window.AccessibilitySettings.enhancedFocus) {
        $("html").addClass("enhanced-focus");
    }
    
    // Apply tooltips
    enableTooltips(window.AccessibilitySettings.tooltipsEnabled);
}

/**
 * Setup global keyboard shortcuts
 */
function setupGlobalKeyboardShortcuts() {
    $(document).keydown(function(e) {
        if (e.ctrlKey) {
            switch(e.key) {
                case '+':
                case '=':
                    e.preventDefault();
                    increaseFontSize();
                    break;
                case '-':
                    e.preventDefault();
                    decreaseFontSize();
                    break;
                case 'd':
                    e.preventDefault();
                    toggleTheme();
                    break;
                case 'r':
                    e.preventDefault();
                    resetAllAccessibilitySettings();
                    break;
            }
        }
    });
}

/**
 * Initialize tooltips globally
 */
function initializeTooltips() {
    // Initialize Bootstrap tooltips for elements with .tt class
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('.tt'));
    window.tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

/**
 * Global accessibility functions
 */

function increaseFontSize() {
    window.AccessibilitySettings.fontResize += 10;
    $("html").css("font-size", (100 + window.AccessibilitySettings.fontResize) + "%");
    createCookie("font-resize", window.AccessibilitySettings.fontResize, "");
    
    // Show feedback
    showAccessibilityFeedback("Font size increased");
}

function decreaseFontSize() {
    window.AccessibilitySettings.fontResize -= 10;
    $("html").css("font-size", (100 + window.AccessibilitySettings.fontResize) + "%");
    createCookie("font-resize", window.AccessibilitySettings.fontResize, "");
    
    // Show feedback
    showAccessibilityFeedback("Font size decreased");
}

function toggleTheme() {
    window.AccessibilitySettings.isDarkMode = !window.AccessibilitySettings.isDarkMode;
    
    if (window.AccessibilitySettings.isDarkMode) {
        $("html").addClass("dark-mode");
        createCookie("theme", "dark", "");
        showAccessibilityFeedback("Dark mode enabled");
    } else {
        $("html").removeClass("dark-mode");
        createCookie("theme", "", "");
        showAccessibilityFeedback("Light mode enabled");
    }
}

function toggleHighContrast() {
    window.AccessibilitySettings.isHighContrast = !window.AccessibilitySettings.isHighContrast;
    
    if (window.AccessibilitySettings.isHighContrast) {
        $("html").addClass("high-contrast");
        createCookie("high-contrast", true, "");
        showAccessibilityFeedback("High contrast enabled");
    } else {
        $("html").removeClass("high-contrast");
        createCookie("high-contrast", false, "");
        showAccessibilityFeedback("High contrast disabled");
    }
}

function toggleReduceMotion() {
    window.AccessibilitySettings.isReduceMotion = !window.AccessibilitySettings.isReduceMotion;
    
    if (window.AccessibilitySettings.isReduceMotion) {
        $("html").addClass("reduce-motion");
        createCookie("reduce-motion", true, "");
        showAccessibilityFeedback("Motion reduced");
    } else {
        $("html").removeClass("reduce-motion");
        createCookie("reduce-motion", false, "");
        showAccessibilityFeedback("Motion restored");
    }
}

function toggleEnhancedFocus() {
    window.AccessibilitySettings.enhancedFocus = !window.AccessibilitySettings.enhancedFocus;
    
    if (window.AccessibilitySettings.enhancedFocus) {
        $("html").addClass("enhanced-focus");
        createCookie("focus-indicators", true, "");
        showAccessibilityFeedback("Enhanced focus indicators enabled");
    } else {
        $("html").removeClass("enhanced-focus");
        createCookie("focus-indicators", false, "");
        showAccessibilityFeedback("Enhanced focus indicators disabled");
    }
}

function toggleTooltips() {
    window.AccessibilitySettings.tooltipsEnabled = !window.AccessibilitySettings.tooltipsEnabled;
    
    enableTooltips(window.AccessibilitySettings.tooltipsEnabled);
    createCookie("tooltips", window.AccessibilitySettings.tooltipsEnabled, "");
    
    if (window.AccessibilitySettings.tooltipsEnabled) {
        showAccessibilityFeedback("Tooltips enabled");
    } else {
        showAccessibilityFeedback("Tooltips disabled");
    }
}

function resetAllAccessibilitySettings() {
    if (confirm("Are you sure you want to reset all accessibility settings?")) {
        // Reset all settings
        window.AccessibilitySettings.fontResize = 0;
        window.AccessibilitySettings.isDarkMode = false;
        window.AccessibilitySettings.isHighContrast = false;
        window.AccessibilitySettings.isReduceMotion = false;
        window.AccessibilitySettings.tooltipsEnabled = false;
        window.AccessibilitySettings.enhancedFocus = true;
        
        // Apply resets
        $("html").css("font-size", "100%");
        $("html").removeClass("dark-mode high-contrast reduce-motion");
        $("html").addClass("enhanced-focus");
        enableTooltips(false);
        
        // Clear cookies
        createCookie("font-resize", 0, "");
        createCookie("theme", "", "");
        createCookie("high-contrast", false, "");
        createCookie("reduce-motion", false, "");
        createCookie("tooltips", false, "");
        createCookie("focus-indicators", true, "");
        
        showAccessibilityFeedback("All accessibility settings reset");
    }
}

/**
 * Show accessibility feedback to user
 */
function showAccessibilityFeedback(message) {
    // Use the error popup system if available
    if (window.errorPopup) {
        window.errorPopup.info(message, {
            duration: 2000,
            closable: false
        });
    } else {
        // Fallback to console log
        console.log("Accessibility: " + message);
    }
}

/**
 * Enhanced tooltip management
 */
function enableTooltips(enable) {
    if (window.tooltipList) {
        window.tooltipList.forEach(function(tooltip) {
            if (enable) {
                tooltip.enable();
            } else {
                tooltip.disable();
            }
        });
    }
}

/**
 * Cookie management functions
 */
function createCookie(name, value, days) {
    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        var expires = "; expires=" + date.toGMTString();
    }
    else var expires = "";
    document.cookie = name + "=" + value + expires + "; path=/";
}

function readCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') c = c.substring(1, c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
    }
    return null;
}

function eraseCookie(name) {
    createCookie(name, "", -1);
}

/**
 * Expose functions globally for use in other scripts
 */
window.AccessibilityFunctions = {
    increaseFontSize: increaseFontSize,
    decreaseFontSize: decreaseFontSize,
    toggleTheme: toggleTheme,
    toggleHighContrast: toggleHighContrast,
    toggleReduceMotion: toggleReduceMotion,
    toggleEnhancedFocus: toggleEnhancedFocus,
    toggleTooltips: toggleTooltips,
    resetAllAccessibilitySettings: resetAllAccessibilitySettings,
    enableTooltips: enableTooltips
};
