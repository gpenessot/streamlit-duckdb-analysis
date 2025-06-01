import duckdb
from pathlib import Path

def analyze_taxi_data():
    """Analyze NYC Yellow Taxi data using DuckDB"""
    # Connect to a new in-memory database
    conn = duckdb.connect(database=':memory:')
    
    # Path to the data file
    data_path = Path('../data/yellow_taxi_2022_01.parquet')
    
    if not data_path.exists():
        print(f"Error: Data file not found at {data_path}")
        print("Please run download_data.py first.")
        return
    
    print("Analyzing NYC Yellow Taxi data...")
    
    # Create a table from the Parquet file
    conn.execute(f"CREATE TABLE yellow_taxi AS SELECT * FROM '{data_path}'")
    
    # Display basic info
    print("\n=== Data Summary ===")
    row_count = conn.execute("SELECT COUNT(*) FROM yellow_taxi").fetchone()[0]
    print(f"Total records: {row_count:,}")
    
    # Sample some records
    print("\n=== Sample Data ===")
    sample = conn.execute("SELECT * FROM yellow_taxi LIMIT 5").fetchall()
    for row in sample:
        print(row)
    
    # Basic statistics
    print("\n=== Trip Statistics ===")
    stats = conn.execute("""
        SELECT 
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            AVG(total_amount) AS avg_total
        FROM yellow_taxi
    """).fetchone()
    
    print(f"Average Trip Distance: {stats[0]:.2f} miles")
    print(f"Average Fare Amount: ${stats[1]:.2f}")
    print(f"Average Tip Amount: ${stats[2]:.2f}")
    print(f"Average Total Amount: ${stats[3]:.2f}")
    
    # Hourly distribution
    print("\n=== Hourly Trip Distribution ===")
    hourly = conn.execute("""
        SELECT 
            EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
            COUNT(*) AS trip_count
        FROM yellow_taxi
        GROUP BY hour
        ORDER BY hour
    """).fetchall()
    
    for hour, count in hourly:
        print(f"Hour {hour:02d}: {count:,} trips")
    
    # Export some analyzed data to use in the Streamlit app
    print("\n=== Exporting Analysis Results ===")
    
    # Export hourly data
    conn.execute(f"""
        COPY (
            SELECT 
                EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
                COUNT(*) AS trip_count
            FROM yellow_taxi
            GROUP BY hour
            ORDER BY hour
        ) TO '../data/hourly_trips.parquet'
    """)
    
    # Export daily data
    conn.execute(f"""
        COPY (
            SELECT 
                EXTRACT(DAY FROM tpep_pickup_datetime) AS day,
                COUNT(*) AS trip_count,
                AVG(trip_distance) AS avg_distance,
                AVG(total_amount) AS avg_amount
            FROM yellow_taxi
            GROUP BY day
            ORDER BY day
        ) TO '../data/daily_trips.parquet'
    """)
    
    # Export payment type data
    conn.execute(f"""
        COPY (
            SELECT 
                payment_type,
                COUNT(*) AS count,
                AVG(total_amount) AS avg_amount,
                SUM(total_amount) AS total_amount
            FROM yellow_taxi
            GROUP BY payment_type
            ORDER BY count DESC
        ) TO '../data/payment_types.parquet'
    """)
    
    print("Analysis complete! Results exported to data directory.")

if __name__ == "__main__":
    analyze_taxi_data()