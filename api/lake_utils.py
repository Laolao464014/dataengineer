docker
exec
rest - api
bash - c
"cat > /app/api/lake_utils.py << 'EOF'
import os
import glob
import pyarrow.parquet as pq
from datetime import datetime


def query_curated_data(lake_path, sensor_type=None, anomaly_only=False, limit=100, order_by='event_time',
                       order_desc=True):
    if sensor_type:
        curated_path = f"{lake_path}/curated/domain=iot/sensor={sensor_type}"
    else:
        curated_path = f"{lake_path}/curated/domain=iot"

    if not os.path.exists(curated_path):
        return []

    parquet_files = glob.glob(f"{curated_path}/**/*.parquet", recursive=True)
    parquet_files = [f for f in parquet_files if not os.path.basename(f).startswith('.') and 'SUCCESS' not in f]

    if not parquet_files:
        return []

    results = []
    for parquet_file in parquet_files:
        try:
            table = pq.read_table(parquet_file)
            for i in range(table.num_rows):
                row = {}
                for col_name in table.column_names:
                    val = table.column(col_name)[i].as_py()
                    if hasattr(val, 'item'):
                        val = val.item()
                    row[col_name] = val
                if sensor_type:
                    row['sensor'] = sensor_type

                if anomaly_only and not row.get('is_anomaly', False):
                    continue

                results.append(row)
        except Exception as e:
            continue

    results.sort(key=lambda x: x.get(order_by, ''), reverse=order_desc)
    return results[:limit]


def get_latest_reading(lake_path, sensor_type):
    results = query_curated_data(lake_path, sensor_type, limit=1, order_by='event_time', order_desc=True)
    return results[0] if results else None


def query_consumption_data(lake_path, sensor_type, start_date, end_date):
    consumption_path = f"{lake_path}/consumption/use_case=sensor_averages/sensor={sensor_type}"
    if not os.path.exists(consumption_path):
        return []

    parquet_files = glob.glob(f"{consumption_path}/**/*.parquet", recursive=True)
    parquet_files = [f for f in parquet_files if not os.path.basename(f).startswith('.')]

    results = []
    for parquet_file in parquet_files:
        try:
            table = pq.read_table(parquet_file)
            for i in range(table.num_rows):
                row = {}
                for col_name in table.column_names:
                    val = table.column(col_name)[i].as_py()
                    if hasattr(val, 'item'):
                        val = val.item()
                    row[col_name] = val

                window_start = row.get('window_start')
                if window_start:
                    if isinstance(window_start, (int, float)):
                        window_start = datetime.fromtimestamp(window_start / 1000)
                    if start_date <= window_start <= end_date:
                        results.append(row)
        except Exception as e:
            continue

    results.sort(key=lambda x: x.get('window_start', ''))
    return results


EOF
"