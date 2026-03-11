import pandas as pd
import numpy as np
import great_expectations as gx
import json
import os
from datetime import datetime

def run_validation(csv_path='data/raw/field_records.csv'):
    print(f'Loading data from {csv_path}...')
    df = pd.read_csv(csv_path)
    print(f'Loaded {len(df):,} records')

    context = gx.get_context(mode='ephemeral')
    data_source = context.data_sources.add_pandas(name='field_data')
    data_asset = data_source.add_dataframe_asset(name='records')
    batch_def = data_asset.add_batch_definition_whole_dataframe('all_records')
    batch = batch_def.get_batch(batch_parameters={'dataframe': df})

    suite = context.suites.add(gx.ExpectationSuite(name='env_monitoring_suite'))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='record_id'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='region'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='survey_type'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='latitude'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='longitude'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='survey_date'))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column='surveyor_id'))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='latitude', min_value=-35.0, max_value=37.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='longitude', min_value=-18.0, max_value=52.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='canopy_cover_pct', min_value=0.0, max_value=100.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='tree_height_m', min_value=0.0, max_value=100.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='soil_ph', min_value=0.0, max_value=14.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column='biomass_kg', min_value=0.0, max_value=100000.0))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column='record_id'))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='survey_type',
        value_set=['tree_inventory', 'biodiversity_survey', 'soil_measurement', 'gps_waypoint']
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column='region',
        value_set=['Sahel_Burkina_01','Kenya_Rift_01','Ethiopia_Highland','Ghana_Volta_01',
                   'Tanzania_Usambara','Uganda_Rwenzori','Rwanda_Volcanoes','Senegal_Casamance',
                   'Mali_Dogon','Cameroon_Adamawa']
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(column='survey_date', strftime_format='%Y-%m-%d'))
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=10000000))

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name='env_validation', data=batch_def, suite=suite)
    )
    results = vd.run(batch_parameters={'dataframe': df})

    rule_results = []
    passed = 0
    failed = 0
    for result in results.results:
        rule_name = result.expectation_config.type
        col = result.expectation_config.column if hasattr(result.expectation_config, 'column') else 'table'
        success = result.success
        passed += 1 if success else 0
        failed += 0 if success else 1
        unexpected_count = 0
        unexpected_pct = 0.0
        if hasattr(result, 'result') and result.result:
            unexpected_count = result.result.get('unexpected_count', 0) or 0
            unexpected_pct = result.result.get('unexpected_percent', 0.0) or 0.0
        rule_results.append({'rule': rule_name, 'column': col, 'passed': success,
                              'unexpected_count': unexpected_count, 'unexpected_pct': round(unexpected_pct, 2)})

    quality_score = round((passed / (passed + failed)) * 100, 1) if (passed + failed) > 0 else 0

    df['_is_valid'] = True
    df.loc[df['latitude'].isna() | (df['latitude'] < -35) | (df['latitude'] > 37), '_is_valid'] = False
    df.loc[df['longitude'].isna() | (df['longitude'] < -18) | (df['longitude'] > 52), '_is_valid'] = False
    df.loc[df['canopy_cover_pct'] > 100, '_is_valid'] = False
    df.loc[df['tree_height_m'] < 0, '_is_valid'] = False
    df.loc[df['soil_ph'] > 14, '_is_valid'] = False
    df.loc[df['region'].isna() | df['surveyor_id'].isna(), '_is_valid'] = False
    df.loc[df.duplicated(subset=['record_id']), '_is_valid'] = False

    clean_df = df[df['_is_valid']].drop(columns=['_is_valid'])
    flagged_df = df[~df['_is_valid']].drop(columns=['_is_valid'])

    os.makedirs('data/reports', exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report = {
        'timestamp': ts, 'total_records': len(df),
        'clean_records': len(clean_df), 'flagged_records': len(flagged_df),
        'quality_score_pct': quality_score, 'rules_passed': passed,
        'rules_failed': failed, 'rule_results': rule_results,
        'flagged_record_ids': flagged_df['record_id'].tolist()[:100]
    }
    with open(f'data/reports/validation_{ts}.json', 'w') as f:
        json.dump(report, f, indent=2)

    clean_df.to_csv('data/processed_clean.csv', index=False)
    flagged_df.to_csv('data/flagged_records.csv', index=False)

    print(f'Quality Score: {quality_score}%')
    print(f'Clean: {len(clean_df):,}  |  Flagged: {len(flagged_df):,}')
    return report, clean_df, flagged_df

if __name__ == '__main__':
    run_validation()
