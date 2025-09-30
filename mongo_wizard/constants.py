"""
Constants and configuration values for mongo-wizard
Centralized location for magic numbers and default values
"""

# ============================================================================
# MongoDB Connection Settings
# ============================================================================

# Default connection timeout in milliseconds
DEFAULT_MONGO_TIMEOUT = 5000

# Shorter timeout for quick checks (e.g., listing hosts status)
QUICK_CHECK_TIMEOUT = 1000

# Longer timeout for operations that might take time
LONG_OPERATION_TIMEOUT = 3000

# ============================================================================
# Copy and Backup Settings
# ============================================================================

# Default batch size for document insertion
DEFAULT_BATCH_SIZE = 1000

# Default sample size for verification
DEFAULT_VERIFICATION_SAMPLE_SIZE = 100

# Threshold for running checksums (documents count)
# Collections smaller than this will get full checksum verification
CHECKSUM_THRESHOLD = 10000

# ============================================================================
# SSH/SCP Settings
# ============================================================================

# SSH connection timeout in seconds
SSH_CONNECT_TIMEOUT = 10

# SSH keep-alive interval in seconds
SSH_KEEPALIVE_INTERVAL = 5

# SSH keep-alive max count
SSH_KEEPALIVE_MAX_COUNT = 3

# SCP transfer timeout in seconds (5 minutes)
SCP_TRANSFER_TIMEOUT = 300

# ============================================================================
# FTP Settings
# ============================================================================

# Default FTP port
DEFAULT_FTP_PORT = 21

# ============================================================================
# Formatting Thresholds
# ============================================================================

# Threshold for using 'K' suffix (thousands)
FORMAT_THOUSANDS_THRESHOLD = 1_000

# Threshold for using 'M' suffix (millions)
FORMAT_MILLIONS_THRESHOLD = 1_000_000