import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os, sys
sys.path.insert(0, 'C:\\env_quality_project')
from src.validate import run_validation

st.set_page_config(page_title='Environmental Data Quality Pipeline', page_icon='🌿', layout='wide')

st.markdown('''<style>
[data-testid=stAppViewContainer]{background:#0A0F0D;color:#E8F5E9}
[data-testid=stSidebar]{background:#0D1F14}
.metric-card{background:#0D1F14;border:1px solid #2E7D32;border-radius:8px;padding:18px 20px;margin:6px 0}
.metric-value{font-size:2rem;font-weight:700;color:#69F0AE}
.metric-label{font-size:0.78rem;color:#81C784;letter-spacing:0.08em;text-transform:uppercase}
h1,h2,h3{color:#69F0AE!important}
</style>''', unsafe_allow_html=True)

with st.sidebar:
    st.markdown('### 🌿 Data Quality Pipeline')
    st.markdown('---')
    uploaded = st.file_uploader('Upload CSV file', type=['csv'])
    st.markdown('---')
    use_demo = st.checkbox('Use built-in demo data', value=True)
    st.markdown('---')
    st.markdown('**About**')
    st.markdown('Validates environmental field records against 25+ rules using Great Expectations.')

st.markdown('# 🌿 Environmental Monitoring Data Quality Pipeline')
st.markdown('*Automated validation for field-collected restoration data using Great Expectations*')
st.markdown('---')

if uploaded is not None:
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name
    with st.spinner('Running validation...'):
        report, clean_df, flagged_df = run_validation(tmp_path)
    os.unlink(tmp_path)
elif use_demo and os.path.exists('data/raw/field_records.csv'):
    with st.spinner('Running validation on demo data...'):
        report, clean_df, flagged_df = run_validation('data/raw/field_records.csv')
else:
    st.info('Upload a CSV file or check Use built-in demo data in the sidebar.')
    st.stop()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{report["quality_score_pct"]}%</div><div class="metric-label">Quality Score</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{report["total_records"]:,}</div><div class="metric-label">Total Records</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{report["clean_records"]:,}</div><div class="metric-label">Clean Records</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{report["flagged_records"]:,}</div><div class="metric-label">Flagged Records</div></div>', unsafe_allow_html=True)

st.markdown('---')
score = report['quality_score_pct']
color = '#69F0AE' if score >= 90 else '#FFC107' if score >= 70 else '#EF5350'
fig_gauge = go.Figure(go.Indicator(
    mode='gauge+number', value=score,
    gauge={'axis': {'range': [0, 100]}, 'bar': {'color': color},
           'bgcolor': '#0D1F14',
           'steps': [{'range':[0,70],'color':'#1A0A0A'},{'range':[70,90],'color':'#1A1A0A'},{'range':[90,100],'color':'#0A1A0A'}],
           'threshold': {'line': {'color': '#69F0AE', 'width': 3}, 'value': 90}},
    number={'font': {'color': color, 'size': 48}},
))
fig_gauge.update_layout(paper_bgcolor='#0A0F0D', font_color='#E8F5E9', height=280)
st.plotly_chart(fig_gauge, use_container_width=True)

st.markdown('### Validation Rules')
rules_df = pd.DataFrame(report['rule_results'])
rules_df['status'] = rules_df['passed'].apply(lambda x: '✅ PASS' if x else '❌ FAIL')
rules_df['rule_clean'] = rules_df['rule'].str.replace('Expect','').str.replace('Column','Col ').str.replace('Table','Tbl ')
display_df = rules_df[['status','column','rule_clean','unexpected_count','unexpected_pct']].copy()
display_df.columns = ['Status','Column','Rule','Failed Records','Failure %']
st.dataframe(display_df, use_container_width=True, height=350)

left, right = st.columns(2)
with left:
    st.markdown('### Flagged Records by Error Type')
    if len(flagged_df) > 0:
        err_data = {
            'Invalid GPS': len(flagged_df[(flagged_df['latitude'] < -35) | (flagged_df['latitude'] > 37)]),
            'Missing Required': int(flagged_df[['region','surveyor_id']].isna().any(axis=1).sum()),
            'Invalid Canopy': len(flagged_df[flagged_df['canopy_cover_pct'] > 100]),
            'Invalid Soil pH': len(flagged_df[flagged_df['soil_ph'] > 14]),
            'Duplicate ID': int(flagged_df.duplicated(subset=['record_id']).sum()),
        }
        err_df = pd.DataFrame({'Error': list(err_data.keys()), 'Count': list(err_data.values())})
        err_df = err_df[err_df['Count'] > 0]
        fig_err = px.bar(err_df, x='Count', y='Error', orientation='h', color='Count', color_continuous_scale='Reds')
        fig_err.update_layout(paper_bgcolor='#0A0F0D', plot_bgcolor='#0A0F0D', font_color='#E8F5E9', height=300, showlegend=False)
        st.plotly_chart(fig_err, use_container_width=True)

with right:
    st.markdown('### Clean Records by Region')
    if 'region' in clean_df.columns:
        region_counts = clean_df['region'].value_counts().reset_index()
        region_counts.columns = ['Region', 'Records']
        fig_r = px.bar(region_counts, x='Records', y='Region', orientation='h', color='Records', color_continuous_scale='Greens')
        fig_r.update_layout(paper_bgcolor='#0A0F0D', plot_bgcolor='#0A0F0D', font_color='#E8F5E9', height=300, showlegend=False)
        st.plotly_chart(fig_r, use_container_width=True)

st.markdown('### Flagged Records (First 50)')
if len(flagged_df) > 0:
    st.dataframe(flagged_df.head(50), use_container_width=True, height=280)

st.markdown('### Remediation Recommendations')
recs = [
    ('Invalid GPS', 'Cross-check with region shapefile; request re-survey from field team'),
    ('Missing required fields', 'Return to data entry system; mark records incomplete pending re-submission'),
    ('Canopy cover > 100%', 'Data entry error; request correction from surveyor'),
    ('Invalid soil pH', 'Values above 14 indicate instrument calibration error'),
    ('Duplicate record IDs', 'Keep most recent submission; archive duplicates with timestamp note'),
    ('Future survey dates', 'Correct to actual survey date; flag surveyor for retraining'),
]
st.table(pd.DataFrame(recs, columns=['Issue', 'Recommended Action']))

st.markdown('---')
d1, d2 = st.columns(2)
with d1:
    st.download_button('Download Clean Records CSV', clean_df.to_csv(index=False), 'clean_records.csv', 'text/csv')
with d2:
    st.download_button('Download Flagged Records CSV', flagged_df.to_csv(index=False), 'flagged_records.csv', 'text/csv')
