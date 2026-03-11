import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = 'postgresql://postgres:YOUR_PASSWORD_HERE @localhost:5433/env_monitoring'

def load_clean_records(csv_path='data/processed_clean.csv'):
    engine = create_engine(DB_URL)
    df = pd.read_csv(csv_path)
    df['survey_date'] = pd.to_datetime(df['survey_date'], errors='coerce')
    print(f'Loading {len(df):,} clean records...')

    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text('''
                INSERT INTO field_records
                (record_id, region, survey_type, species, latitude, longitude,
                 tree_height_m, canopy_cover_pct, soil_ph, biomass_kg, survey_date, surveyor_id)
                VALUES (:record_id, :region, :survey_type, :species, :latitude, :longitude,
                        :tree_height_m, :canopy_cover_pct, :soil_ph, :biomass_kg, :survey_date, :surveyor_id)
                ON CONFLICT (record_id) DO NOTHING
            '''), row.to_dict())
        conn.commit()
    print('Load complete!')

    with engine.connect() as conn:
        result = conn.execute(text('''
            SELECT region, COUNT(*) as total,
                   ROUND(AVG(canopy_cover_pct)::numeric,1) as avg_canopy,
                   RANK() OVER (ORDER BY AVG(canopy_cover_pct) DESC) as rank
            FROM field_records GROUP BY region ORDER BY rank
        '''))
        print('\nRegion rankings by canopy cover:')
        for row in result:
            print(f'  Rank {row[3]}: {row[0]:30s}  {row[1]:,} records  canopy={row[2]}%')

if __name__ == '__main__':
    load_clean_records()
