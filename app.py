# Import python packages
import streamlit as st
import pandas as pd
import seaborn as sns

from snowflake.snowpark.context import get_active_session
from plotly import express as px

from matplotlib import pyplot as plt
from matplotlib import font_manager as fm

# DEPARTMENT_STORE_FOOT_TRAFFIC_FOR_SNOWFLAKE_STREAMLIT_HACKATHON
DEPARTMENT_VISIT_PATTERN = 'DEPARTMENT_STORE_FOOT_TRAFFIC_FOR_SNOWFLAKE_STREAMLIT_HACKATHON.PUBLIC.SNOWFLAKE_STREAMLIT_HACKATHON_LOPLAT_DEPARTMENT_STORE_DATA'

# RESIDENTIAL__WORKPLACE_TRAFFIC_PATTERNS_FOR_SNOWFLAKE_STREAMLIT_HACKATHON
RESID_WORK_PATTERN = 'RESIDENTIAL__WORKPLACE_TRAFFIC_PATTERNS_FOR_SNOWFLAKE_STREAMLIT_HACKATHON.PUBLIC.SNOWFLAKE_STREAMLIT_HACKATHON_LOPLAT_HOME_OFFICE_RATIO'

# SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS
SEOUL_BY_DIST_ASSET_INCOME_INFO = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO'
SEOUL_BY_DIST_CARD_SALES_INFO = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.CARD_SALES_INFO'
SEOUL_BY_DIST_CODE_MASTER = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.CODE_MASTER'
SEOUL_BY_DIST_FLOATING_POPULATION_INFO = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO'
SEOUL_BY_DIST_M_SCCO_MST = 'SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST'

# SEOUL_TEMPERATURE__RAINFALL_FOR_SNOWFLAKE_STREAMLIT_HACKATHON
SEOUL_TEMP_RAINFALL = 'SEOUL_TEMPERATURE__RAINFALL_FOR_SNOWFLAKE_STREAMLIT_HACKATHON.PUBLIC.SNOWFLAKE_STREAMLIT_HACKATHON_LOPLAT_SEOUL_TEMPERATURE_RAINFALL'


# Write directly to the app
st.title(f"백화점은 비오는 날을 싫어해 :umbrella:")
st.text(
  """
  강수량이 백화점 매출에 미치는 영향을 고려한 프로모션에 대한 전략적 접근  
  """
)

# Get the current credentials
session = get_active_session()

df_visit = session.sql(f'select * from {DEPARTMENT_VISIT_PATTERN}').to_pandas()
df_rainfall = session.sql(f'select * from {SEOUL_TEMP_RAINFALL}').to_pandas()

df_visit['DATE_KST'] = pd.to_datetime(df_visit['DATE_KST'])
df_rainfall['DATE_KST'] = pd.to_datetime(df_rainfall['DATE_KST'])

holidays = ['2023-01-01', '2023-01-21', '2023-01-22', '2023-01-23', '2023-01-24', '2023-03-01', '2023-05-05', '2023-05-27', '2023-06-06', '2023-08-15', '2023-09-28', '2023-09-29', '2023-09-30', '2023-10-03', '2023-10-04', '2023-10-05', '2023-10-06', '2023-10-07', '2023-10-08', '2023-10-09', '2023-12-25']
holiday_dates = pd.to_datetime(holidays).tz_localize("Asia/Seoul")

df_joined_W = pd.merge(
    df_visit
        [(df_visit['DATE_KST'].dt.weekday<5) & 
        (~df_visit['DATE_KST'].isin(holiday_dates)) & 
        (df_visit['DATE_KST'].dt.year.isin([2023, 2024]))], 
    df_rainfall
        [(df_rainfall['DATE_KST'].dt.weekday<5) & 
        (~df_rainfall['DATE_KST'].isin(holiday_dates)) &
        (df_rainfall['RAINFALL_MM'] > 20) & 
        (df_rainfall['DATE_KST'].dt.year.isin([2023,2024]))], 
    on='DATE_KST')

df_joined_H = pd.merge(
    df_visit
        [(df_visit['DATE_KST'].dt.weekday>=5) & 
        (~df_visit['DATE_KST'].isin(holiday_dates)) &
        (df_visit['DATE_KST'].dt.year.isin([2023, 2024]))], 
    df_rainfall
        [(df_rainfall['DATE_KST'].dt.weekday>=5) &
        (~df_rainfall['DATE_KST'].isin(holiday_dates)) & 
        (df_rainfall['DATE_KST'].dt.year.isin([2023, 2024])) &
        (df_rainfall['RAINFALL_MM'] > 20)], 
    on='DATE_KST')


sns.set(style="whitegrid")  
sns.set(font="NanumGothic")

plt.figure(figsize=(10,6))

# Scatter plot with regression line for trend and embellishments
ax = sns.regplot(x='RAINFALL_MM', y='COUNT', data=df_joined_W[df_joined_W['DEP_NAME']=='신세계_강남'], scatter_kws={"s":500, "alpha":0.6}, line_kws={"color":"blue"})
ax.set_title("강남 신세계백화점 방문객 vs 일일 강수량 (주중)")
ax.set_xlabel("일 강수량 (mm)")
ax.set_ylabel("방문객 수")

st.pyplot(plt)


plt.figure(figsize=(10,6))

# Scatter plot with regression line for trend and embellishments
ax = sns.regplot(x='RAINFALL_MM', y='COUNT', data=df_joined_H[df_joined_H['DEP_NAME']=='신세계_강남'], scatter_kws={"s":500, "alpha":0.6}, line_kws={"color":"blue"})

ax.set_title("강남 신세계백화점 방문객 vs 일일 강수량 (주말)")
ax.set_xlabel("일 강수량 (mm)")
ax.set_ylabel("방문객 수")

st.pyplot(plt)

df_res_work = session.sql(f'select * from {RESID_WORK_PATTERN}').to_pandas()
df_res_work = df_res_work[df_res_work['DEP_NAME']=='신세계_강남'].groupby(['ADDR_LV1', 'ADDR_LV2', 'LOC_TYPE'])['RATIO'].sum().reset_index()
df_res_work.sort_values(by=['LOC_TYPE', 'RATIO'], ascending=[True, False], inplace=True)

plt.figure(figsize=(10,6))

sns.barplot(data=df_res_work, x='ADDR_LV2', y='RATIO', hue='LOC_TYPE', ci=None)
plt.xticks(rotation=90)
plt.tight_layout()
st.pyplot(plt)









