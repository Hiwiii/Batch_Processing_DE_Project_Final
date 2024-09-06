-- Select all records from the 'aggregated_ecommerce_data' table.
SELECT * FROM aggregated_ecommerce_data;

-- Truncate the tables to remove all rows 

TRUNCATE TABLE aggregated_ecommerce_data;

-- Create the 'aggregated_ecommerce_data' 
CREATE TABLE aggregated_ecommerce_data (
    category_id BIGINT NOT NULL,
    event_count BIGINT NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL
);
