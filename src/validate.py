import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

VALID_SURVEY_TYPES = ['tree_inventory', 'biodiversity_survey', 'soil_measurement', 'gps_waypoint']
VALID_REGIONS = [
    'Sahel_Burkina_01', 'Kenya_Rift_01', 'Ethiopia_Highland', 'Ghana_Volta_01',
    'Tanzania_Usambara', 'Uganda_Rwenzori', 'Rwanda_Volcanoes', 'Senegal_Casamance',
    'Mali_Dogon', 'Cameroon_Adamawa'
]

RULES = [
    ('ExpectColumnValuesToNotBeNull',    'record_id'),
    ('ExpectColumnValuesToNotBeNull',    'region'),
    ('ExpectColumnValuesToNotBeNull',    'survey_type'),
    ('ExpectColumnValuesToNotBeNull',    'latitude'),
    ('ExpectColumnValuesToNotBeNull',    'longitude'),
    ('ExpectColumnValuesToNotBeNull',    'survey_date'),
    ('ExpectColumnValuesToNotBeNull',    'surveyor_id'),
    ('ExpectColumnValuesToBeBetween',    'latitude'),
    ('ExpectColumnValuesToBeBetween',    'longitude'),
    ('ExpectColumnValuesToBeBetween',    'canopy_cover_pct'),
    ('ExpectColumnValuesToBeBetween',    'tree_height_m'),
    ('ExpectColumnValuesToBeBetween',    'soil_ph'),
    ('ExpectColumnValuesToBeBetween',    'biomass_kg'),
    ('ExpectColumnValuesToBeUnique',     'record_id'),
    ('ExpectColumnValuesToBeInSet',      'survey_type'),
    ('ExpectColumnValuesToBeInSet',      'region'),
    ('ExpectColumnValuesToMatchStrftimeFormat', 'survey_date'),
    ('ExpectTableRowCountToBeBetween',   'table'),
]


def _check(df, rule, col):
    n = len(df)
    if n == 0:
        return True, 0, 0.0

    def _u(mask):
        cnt = int(mask.sum())
        return cnt, round(cnt / n * 100, 2)

    if rule == 'ExpectColumnValuesToNotBeNull':
        if col not in df.columns:
            return False, n, 100.0
        mask = df[col].isna()
        cnt, pct = _u(mask)
        return cnt == 0, cnt, pct

    if rule == 'ExpectColumnValuesToBeBetween':
        if col not in df.columns:
            return True, 0, 0.0
        bounds = {
            'latitude':        (-35.0, 37.0),
            'longitude':       (-18.0, 52.0),
            'canopy_cover_pct':(0.0,  100.0),
            'tree_height_m':   (0.0,  100.0),
            'soil_ph':         (0.0,   14.0),
            'biomass_kg':      (0.0, 100000.0),
        }
        lo, hi = bounds.get(col, (None, None))
        s = pd.to_numeric(df[col], errors='coerce')
        mask = s.isna() | (s < lo) | (s > hi)
        cnt, pct = _u(mask)
        return cnt == 0, cnt, pct

    if rule == 'ExpectColumnValuesToBeUnique':
        if col not in df.columns:
            return True, 0, 0.0
        mask = df.duplicated(subset=[col], keep=False)
        cnt, pct = _u(mask)
        return cnt == 0, cnt, pct

    if rule == 'ExpectColumnValuesToBeInSet':
        if col not in df.columns:
            return False, n, 100.0
        vset = VALID_SURVEY_TYPES if col == 'survey_type' else VALID_REGIONS
        mask = ~df[col].isin(vset) & df[col].notna()
        cnt, pct = _u(mask)
        return cnt == 0, cnt, pct

    if rule == 'ExpectColumnValuesToMatchStrftimeFormat':
        if col not in df.columns:
            return True, 0, 0.0
        def bad(v):
            try:
                datetime.strptime(str(v), '%Y-%m-%d')
                return False
            except Exception:
                return True
        mask = df[col].apply(bad)
        cnt, pct = _u(mask)
        return cnt == 0, cnt, pct

    if rule == 'ExpectTableRowCountToBeBetween':
        ok = 1 <= n <= 10_000_000
        return ok, 0, 0.0

    return True, 0, 0.0


def run_validation(csv_path='data/raw/field_records.csv'):
    df = pd.read_csv(csv_path)

    rule_results = []
    passed = failed = 0

    for rule, col in RULES:
        ok, cnt, pct = _check(df, rule, col)
        passed += 1 if ok else 0
        failed += 0 if ok else 1
        rule_results.append({
            'rule': rule, 'column': col, 'passed': ok,
            'unexpected_count': cnt, 'unexpected_pct': pct
        })

    quality_score = round(passed / (passed + failed) * 100, 1) if (passed + failed) > 0 else 0.0

    df['_is_valid'] = True
    for col in ['record_id', 'region', 'survey_type', 'latitude', 'longitude', 'survey_date', 'surveyor_id']:
        if col in df.columns:
            df.loc[df[col].isna(), '_is_valid'] = False
    if 'latitude' in df.columns:
        lat = pd.to_numeric(df['latitude'], errors='coerce')
        df.loc[(lat < -35) | (lat > 37), '_is_valid'] = False
    if 'longitude' in df.columns:
        lon = pd.to_numeric(df['longitude'], errors='coerce')
        df.loc[(lon < -18) | (lon > 52), '_is_valid'] = False
    if 'canopy_cover_pct' in df.columns:
        df.loc[pd.to_numeric(df['canopy_cover_pct'], errors='coerce') > 100, '_is_valid'] = False
    if 'tree_height_m' in df.columns:
        df.loc[pd.to_numeric(df['tree_height_m'], errors='coerce') < 0, '_is_valid'] = False
    if 'soil_ph' in df.columns:
        df.loc[pd.to_numeric(df['soil_ph'], errors='coerce') > 14, '_is_valid'] = False
    if 'record_id' in df.columns:
        df.loc[df.duplicated(subset=['record_id']), '_is_valid'] = False

    clean_df   = df[df['_is_valid']].drop(columns=['_is_valid'])
    flagged_df = df[~df['_is_valid']].drop(columns=['_is_valid'])

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report = {
        'timestamp': ts,
        'total_records':   len(df),
        'clean_records':   len(clean_df),
        'flagged_records': len(flagged_df),
        'quality_score_pct': quality_score,
        'rules_passed': passed,
        'rules_failed': failed,
        'rule_results': rule_results,
        'flagged_record_ids': flagged_df['record_id'].tolist()[:100] if 'record_id' in flagged_df.columns else [],
    }
    return report, clean_df, flagged_df


if __name__ == '__main__':
    run_validation()

