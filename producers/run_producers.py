from datetime import datetime
from sales_producer import run as run_sales
from support_producer import run as run_support
from marketing_producer import run as run_marketing

start = datetime(2024, 3, 1)

run_sales(start, hours=24, total_records=1_200_000)
run_support(start, hours=24, total_records=600_000)
run_marketing(start, hours=24, total_records=2_000_000)
