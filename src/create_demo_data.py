import pandas as pd
import numpy as np
import os
import random

random.seed(42)
np.random.seed(42)

N_RECORDS = 75000
REGIONS = [
    'Sahel_Burkina_01', 'Kenya_Rift_01', 'Ethiopia_Highland',
    'Ghana_Volta_01', 'Tanzania_Usambara', 'Uganda_Rwenzori',
    'Rwanda_Volcanoes', 'Senegal_Casamance', 'Mali_Dogon',
    'Cameroon_Adamawa'
]
SURVEY_TYPES = ['tree_inventory', 'biodiversity_survey', 'soil_measurement', 'gps_waypoint']
SPECIES = ['Acacia', 'Eucalyptus', 'Moringa', 'Baobab', 'Shea', 'Mango', 'Neem', 'Teak']

n_clean = int(N_RECORDS * 0.90)
lats = np.random.uniform(-15, 20, n_clean)
lons = np.random.uniform(-18, 45, n_clean)

clean_df = pd.DataFrame({
    'record_id': [f'REC_{i:06d}' for i in range(n_clean)],
    'region': np.random.choice(REGIONS, n_clean),
    'survey_type': np.random.choice(SURVEY_TYPES, n_clean),
    'species': np.random.choice(SPECIES + [None], n_clean, p=[0.1]*8 + [0.2]),
    'latitude': lats,
    'longitude': lons,
    'tree_height_m': np.random.uniform(1.5, 35.0, n_clean).round(2),
    'canopy_cover_pct': np.random.uniform(5, 95, n_clean).round(1),
    'soil_ph': np.random.uniform(4.5, 8.5, n_clean).round(2),
    'biomass_kg': np.random.uniform(10, 5000, n_clean).round(1),
    'survey_date': pd.date_range('2018-01-01', periods=n_clean, freq='1min').strftime('%Y-%m-%d'),
    'surveyor_id': [f'SRV_{random.randint(1,50):03d}' for _ in range(n_clean)],
    'data_quality_flag': 'CLEAN',
})

n_dirty = N_RECORDS - n_clean
dirty_records = []
error_types = ['invalid_gps', 'missing_required', 'invalid_range', 'duplicate_record', 'future_date', 'invalid_ph']

for i in range(n_dirty):
    error = random.choice(error_types)
    base = {
        'record_id': f'REC_{n_clean + i:06d}',
        'region': random.choice(REGIONS),
        'survey_type': random.choice(SURVEY_TYPES),
        'species': random.choice(SPECIES),
        'latitude': round(random.uniform(-15, 20), 6),
        'longitude': round(random.uniform(-18, 45), 6),
        'tree_height_m': round(random.uniform(1.5, 35.0), 2),
        'canopy_cover_pct': round(random.uniform(5, 95), 1),
        'soil_ph': round(random.uniform(4.5, 8.5), 2),
        'biomass_kg': round(random.uniform(10, 5000), 1),
        'survey_date': '2022-06-15',
        'surveyor_id': f'SRV_{random.randint(1,50):03d}',
        'data_quality_flag': error,
    }
    if error == 'invalid_gps':
        base['latitude'] = round(random.uniform(200, 999), 2)
        base['longitude'] = round(random.uniform(-200, -100), 2)
    elif error == 'missing_required':
        base['region'] = None
        base['surveyor_id'] = None
    elif error == 'invalid_range':
        base['canopy_cover_pct'] = round(random.uniform(101, 200), 1)
        base['tree_height_m'] = round(random.uniform(-10, -1), 2)
    elif error == 'duplicate_record':
        base['record_id'] = f'REC_{random.randint(0, 100):06d}'
    elif error == 'future_date':
        base['survey_date'] = '2099-12-31'
    elif error == 'invalid_ph':
        base['soil_ph'] = round(random.uniform(15, 50), 2)
    dirty_records.append(base)

dirty_df = pd.DataFrame(dirty_records)
df = pd.concat([clean_df, dirty_df], ignore_index=True).sample(frac=1, random_state=42).reset_index(drop=True)

os.makedirs('data/raw', exist_ok=True)
df.to_csv('data/raw/field_records.csv', index=False)
print(f'Generated {len(df):,} records')
print(f'Clean: {len(clean_df):,}  |  Dirty: {len(dirty_df):,}')
print('Saved to: data/raw/field_records.csv')
