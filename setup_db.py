import os
import sys
import psycopg2
import time
from utils.db_utils import *
from faker import Faker 
import numpy as np
from io import StringIO
import random


# Configs to create dummy data
TOTAL_ROWS_TO_INSERT = 10_000_000  # 10 million rows
BATCH_SIZE = 100_000             # Process 100,000 rows at a time
NUM_DEVICES = 500
NUM_MISSIONS = 100

# --- Data Generation ---
def generate_data_batch(fake, device_ids, mission_ids):
    """Generates a batch of realistic-looking sensor data."""
    data = []
    for _ in range(BATCH_SIZE):
        # Add some correlation for realism
        is_error_state = random.random() < 0.05 # 5% chance of an error
        
        row = (
            fake.date_time_this_year().isoformat(),
            random.choice(device_ids),
            round(random.uniform(-15.0, 45.0), 2), # latitude
            round(random.uniform(-45.0, -43.0), 2), # longitude (centered on Minas Gerais)
            round(random.uniform(0, 150), 2), # altitude
            round(random.uniform(0, 80), 2), # speed
            round(random.uniform(-10, 50) + (15 if is_error_state else 0), 2), # temperature
            round(random.uniform(20, 90), 2), # humidity
            round(random.uniform(980, 1030), 2), # pressure
            round(random.uniform(300, 2000), 2), # gas sensor
            round(random.uniform(5, 100), 2), # battery
            round(random.uniform(5, 80) + (15 if is_error_state else 0), 2), # cpu
            round(random.uniform(10, 90), 2), # memory
            round(random.uniform(5, 95), 2), # disk
            500 if is_error_state else 200, # status code
            round(random.uniform(-2, 2), 6), # gyro_x
            round(random.uniform(-2, 2), 6), # gyro_y
            round(random.uniform(-2, 2), 6), # gyro_z
            round(random.uniform(-3, 3), 6), # accel_x
            round(random.uniform(-3, 3), 6), # accel_y
            round(random.uniform(-3, 3), 6), # accel_z
            random.choice(['AUTONOMOUS', 'MANUAL', 'RETURN_HOME']),
            round(random.uniform(1, 25), 2),
            random.choice(['SURVEY', 'DELIVERY', 'INSPECTION', 'NONE']),
            random.randint(-90, -30), # signal strength
            round(random.uniform(0.1, 5.0), 2), # data usage
            random.choice(['v1.2.3', 'v1.2.4', 'v1.3.0']),
            random.choice(mission_ids)
        )
        data.append(row)
    return data



# --- Main Execution ---
if __name__ == "__main__":
    fake = Faker()
    conn = get_db_connection()

    if conn:
        with conn.cursor() as cur:
            # Check if data already exists
            cur.execute(open("create_table.sql", "r").read()) 
            cur.execute("SELECT COUNT(*) FROM drone_readings;")
            if cur.fetchone()[0] >= TOTAL_ROWS_TO_INSERT:
                print(f"ℹ️ Table already has {TOTAL_ROWS_TO_INSERT} or more rows. Skipping population.")
                sys.exit(0)

        print(f"🚀 Starting data population for {TOTAL_ROWS_TO_INSERT:,} rows...")
        start_time = time.time()
        
        device_ids = [f"drone-{fake.uuid4()[:8]}" for _ in range(NUM_DEVICES)]
        mission_ids = [f"mission-{fake.uuid4()[:12]}" for _ in range(NUM_MISSIONS)]

        total_inserted = 0
        for i in range(TOTAL_ROWS_TO_INSERT // BATCH_SIZE):
            batch_start_time = time.time()
            data_batch = generate_data_batch(fake, device_ids, mission_ids)
            
            # Use StringIO to create an in-memory CSV file
            buffer = StringIO()
            for row in data_batch:
                buffer.write('\t'.join(map(str, row)) + '\n')
            buffer.seek(0)

            # Use COPY for ultra-fast insertion
            with conn.cursor() as cur:
                cur.copy_expert("COPY drone_readings FROM STDIN", buffer)
            conn.commit()
            
            total_inserted += BATCH_SIZE
            batch_end_time = time.time()
            print(f"  -> Inserted batch {i+1}: {BATCH_SIZE:,} rows in {batch_end_time - batch_start_time:.2f} seconds. Total: {total_inserted:,}")

        end_time = time.time()
        print(f"\n🎉 Finished populating database in {end_time - start_time:.2f} seconds.")
        conn.close()









   
