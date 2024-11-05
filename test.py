import secrets

# Generate a random hexadecimal string (suitable for API keys, tokens, etc.)
secret_code = secrets.token_hex(32)  # Generates a 64-character hexadecimal string

print(secret_code)