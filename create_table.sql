-- Create the main table for our IoT drone sensor readings
CREATE TABLE IF NOT EXISTS drone_readings (
    -- Primary Keys
    timestamp TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(50) NOT NULL,

    -- Location Data
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude_m NUMERIC(10, 2),
    speed_mph NUMERIC(10, 2),

    -- Environmental Sensors
    temperature_c NUMERIC(5, 2),
    humidity_percent NUMERIC(5, 2),
    air_pressure_hpa NUMERIC(10, 2),
    gas_sensor_ppm NUMERIC(10, 2), -- e.g., CO2 or methane

    -- Device Status
    battery_level_percent NUMERIC(5, 2),
    cpu_usage_percent NUMERIC(5, 2),
    memory_usage_percent NUMERIC(5, 2),
    disk_usage_percent NUMERIC(5, 2),
    status_code INTEGER, -- e.g., 200 for OK, 500 for error

    -- Gyroscope & Accelerometer
    gyro_x NUMERIC(10, 6),
    gyro_y NUMERIC(10, 6),
    gyro_z NUMERIC(10, 6),
    accel_x NUMERIC(10, 6),
    accel_y NUMERIC(10, 6),
    accel_z NUMERIC(10, 6),

    -- Flight & Payload
    flight_mode VARCHAR(50), -- e.g., 'MANUAL', 'AUTONOMOUS', 'RETURN_HOME'
    payload_weight_kg NUMERIC(8, 2),
    payload_type VARCHAR(50),

    -- Network Info
    signal_strength_dbm INTEGER,
    data_usage_mb NUMERIC(10, 2),

    -- Metadata
    firmware_version VARCHAR(20),
    mission_id VARCHAR(50),

    PRIMARY KEY (timestamp, device_id)
);

-- Create indexes on columns that will be frequently used in queries
CREATE INDEX IF NOT EXISTS idx_device_id ON drone_readings (device_id);
CREATE INDEX IF NOT EXISTS idx_mission_id ON drone_readings (mission_id);
CREATE INDEX IF NOT EXISTS idx_status_code ON drone_readings (status_code);
CREATE INDEX IF NOT EXISTS idx_payload_type ON drone_readings (payload_type);

