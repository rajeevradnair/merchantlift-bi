
from merchantlift.bronze_config import BRONZE_TABLES

print("Bronze ingestion inventory")

print("=" * 80)
for table in BRONZE_TABLES:
    print(
        f"{table.table_name:<45} "
        f"{table.partition_column}"
    )
print("=" * 80)
print(f"Count of bronze tables: {len(BRONZE_TABLES)}")

