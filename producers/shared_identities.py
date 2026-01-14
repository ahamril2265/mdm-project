import random
from reference_data import generate_name, generate_email, generate_phone

POOL_SIZE = 200_000

# Build a shared identity pool ONCE
_IDENTITIES = []

for _ in range(POOL_SIZE):
    name = generate_name()
    _IDENTITIES.append({
        "name": name,
        "email": generate_email(name),   # ✅ FIXED
        "phone": generate_phone()
    })

def get_identity():
    """
    Returns (name, email, phone)
    Shared across ALL producers
    """
    identity = random.choice(_IDENTITIES)
    return identity["name"], identity["email"], identity["phone"]
