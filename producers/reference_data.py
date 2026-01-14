import random

FIRST_NAMES = [
    "Amit","Rahul","Vikram","Suresh","Ravi","Ankit","Karan",
    "Neha","Pooja","Anjali","Priya","Kavya","Riya"
]

LAST_NAMES = [
    "Sharma","Verma","Patel","Iyer","Reddy","Khan","Singh"
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com"
]

def generate_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def generate_email(name):
    username = name.lower().replace(" ", "")
    if random.random() < 0.3:  # dot variation
        username = username[:3] + "." + username[3:]
    return f"{username}@{random.choice(EMAIL_DOMAINS)}"

def generate_phone():
    return f"+91{random.randint(7000000000, 9999999999)}"
