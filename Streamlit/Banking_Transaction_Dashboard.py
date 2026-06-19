import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ── Page Config ─────────────────────────────────────────────────
st.set_page_config(
    page_title="Banking Transaction Dashboard",
    page_icon="🏦",
    layout="wide"
)

# ── Custom CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f, #2d6a9f);
        padding: 20px;
        border-radius: 15px;
        text-align: center;
        color: white;
        margin: 5px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #00d4ff;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #a0c4e8;
        margin-top: 5px;
    }
    .fraud-card {
        background: linear-gradient(135deg, #5f1e1e, #9f2d2d);
        padding: 20px;
        border-radius: 15px;
        text-align: center;
        color: white;
        margin: 5px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .fraud-value {
        font-size: 2rem;
        font-weight: bold;
        color: #ff6b6b;
    }
    .section-header {
        background: linear-gradient(90deg, #1e3a5f, transparent);
        padding: 10px 20px;
        border-radius: 10px;
        border-left: 4px solid #00d4ff;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ── Session & Data ───────────────────────────────────────────────
session = get_active_session()

@st.cache_data
def load_data():
    customer_kpi     = session.table("BANKING_DW.GOLD.CUSTOMER_KPI").to_pandas()
    customer_monthly = session.table("BANKING_DW.GOLD.CUSTOMER_MONTHLY").to_pandas()
    branch_summary   = session.table("BANKING_DW.GOLD.BRANCH_SUMMARY").to_pandas()
    region_summary   = session.table("BANKING_DW.GOLD.REGION_SUMMARY").to_pandas()
    fraud_high_value = session.table("BANKING_DW.GOLD.FRAUD_HIGH_VALUE").to_pandas()
    fraud_burst      = session.table("BANKING_DW.GOLD.FRAUD_BURST").to_pandas()
    fraud_high_spend = session.table("BANKING_DW.GOLD.FRAUD_HIGH_SPEND").to_pandas()
    fraud_dormant    = session.table("BANKING_DW.GOLD.FRAUD_DORMANT").to_pandas()
    return (customer_kpi, customer_monthly, branch_summary,
            region_summary, fraud_high_value, fraud_burst,
            fraud_high_spend, fraud_dormant)

(customer_kpi, customer_monthly, branch_summary,
 region_summary, fraud_high_value, fraud_burst,
 fraud_high_spend, fraud_dormant) = load_data()

total_fraud = (len(fraud_high_value) + len(fraud_burst) +
               len(fraud_high_spend) + len(fraud_dormant))

# ── Header ───────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center; padding:20px;
background:linear-gradient(135deg,#0a1628,#1e3a5f);
border-radius:15px; margin-bottom:20px;'>
    <h1 style='color:#00d4ff; margin:0;'>
    🏦 Banking Transaction Monitoring</h1>
    <p style='color:#a0c4e8; margin:5px 0 0 0;'>
    Azure Databricks + ADLS Gen2 + Snowflake + Streamlit</p>
</div>
""", unsafe_allow_html=True)

# ── Tabs ─────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Executive Overview",
    "🚨 Fraud Analysis",
    "🏢 Branch Analysis",
    "📈 Monthly Trends"
])

# ════════════════════════════════════════════════════════════════
# TAB 1 — EXECUTIVE OVERVIEW
# ════════════════════════════════════════════════════════════════
with tab1:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>5,000</div>
            <div class='metric-label'>👥 Total Customers</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            {int(customer_kpi['TOTAL_TRANSACTIONS'].sum()):,}</div>
            <div class='metric-label'>💳 Total Transactions</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            ₹{customer_kpi['TOTAL_DEBIT'].sum()/10000000:.1f} Cr</div>
            <div class='metric-label'>💰 Total Debit Amount</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class='fraud-card'>
            <div class='fraud-value'>{total_fraud}</div>
            <div class='metric-label'>🚨 Total Fraud Alerts</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-header'><b>🏆 Top 10 Customers by Debit</b></div>",
                    unsafe_allow_html=True)
        top_c = customer_kpi.nlargest(10, 'TOTAL_DEBIT')[
            ['CUSTOMER_NAME','ACCOUNT_TYPE','TOTAL_DEBIT']
        ].reset_index(drop=True)
        chart = alt.Chart(top_c).mark_bar().encode(
            x=alt.X('TOTAL_DEBIT:Q', title='Total Debit (₹)'),
            y=alt.Y('CUSTOMER_NAME:N', sort='-x', title='Customer'),
            color=alt.Color('ACCOUNT_TYPE:N',
                scale=alt.Scale(scheme='blues')),
            tooltip=['CUSTOMER_NAME','ACCOUNT_TYPE','TOTAL_DEBIT']
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("<div class='section-header'><b>📊 Transactions by Account Type</b></div>",
                    unsafe_allow_html=True)
        acc = customer_kpi.groupby('ACCOUNT_TYPE').agg(
            Total=('TOTAL_TRANSACTIONS','sum')
        ).reset_index()
        chart = alt.Chart(acc).mark_arc(innerRadius=60).encode(
            theta=alt.Theta('Total:Q'),
            color=alt.Color('ACCOUNT_TYPE:N',
                scale=alt.Scale(scheme='blues')),
            tooltip=['ACCOUNT_TYPE','Total']
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-header'><b>🗺️ Top 10 States by Volume</b></div>",
                    unsafe_allow_html=True)
        state_df = customer_kpi.groupby('CUSTOMER_STATE').agg(
            Total_Debit=('TOTAL_DEBIT','sum')
        ).reset_index().nlargest(10,'Total_Debit')
        chart = alt.Chart(state_df).mark_bar().encode(
            x=alt.X('CUSTOMER_STATE:N', sort='-y', title='State'),
            y=alt.Y('Total_Debit:Q', title='Total Debit (₹)'),
            color=alt.Color('Total_Debit:Q',
                scale=alt.Scale(scheme='blues')),
            tooltip=['CUSTOMER_STATE','Total_Debit']
        ).properties(height=280)
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("<div class='section-header'><b>💳 Debit vs Credit by Account Type</b></div>",
                    unsafe_allow_html=True)
        dc = customer_kpi.groupby('ACCOUNT_TYPE').agg(
            Debit=('TOTAL_DEBIT','sum'),
            Credit=('TOTAL_CREDIT','sum')
        ).reset_index()
        dc_melted = dc.melt('ACCOUNT_TYPE',
            var_name='Type', value_name='Amount')
        chart = alt.Chart(dc_melted).mark_bar().encode(
            x=alt.X('ACCOUNT_TYPE:N', title='Account Type'),
            y=alt.Y('Amount:Q', title='Amount (₹)'),
            color=alt.Color('Type:N',
                scale=alt.Scale(
                    domain=['Debit','Credit'],
                    range=['#ff6b6b','#00d4ff']
                )),
            xOffset='Type:N',
            tooltip=['ACCOUNT_TYPE','Type','Amount']
        ).properties(height=280)
        st.altair_chart(chart, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# TAB 2 — FRAUD ANALYSIS
# ════════════════════════════════════════════════════════════════
with tab2:
    col1, col2, col3, col4 = st.columns(4)
    fraud_items = [
        ("💸 High Value", len(fraud_high_value),
         f"₹{fraud_high_value['AMOUNT'].sum()/10000000:.1f} Cr"),
        ("⚡ Rapid Burst", len(fraud_burst),
         f"₹{fraud_burst['TOTAL_AMOUNT'].sum()/10000000:.1f} Cr"),
        ("📈 High Daily Spend", len(fraud_high_spend),
         f"₹{fraud_high_spend['DAILY_SPEND'].sum()/10000000:.1f} Cr"),
        ("😴 Dormant Active", len(fraud_dormant), "Suspicious"),
    ]
    for col, (label, count, amount) in zip(
            [col1,col2,col3,col4], fraud_items):
        with col:
            st.markdown(f"""<div class='fraud-card'>
                <div class='fraud-value'>{count}</div>
                <div class='metric-label'>{label}</div>
                <div style='color:#ffaa00;font-size:0.8rem;
                margin-top:5px;'>{amount}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-header'><b>🔴 Fraud Alerts by Type</b></div>",
                    unsafe_allow_html=True)
        fraud_df = pd.DataFrame({
            'Type' :['High Value','Rapid Burst',
                     'High Daily Spend','Dormant Active'],
            'Count':[len(fraud_high_value), len(fraud_burst),
                     len(fraud_high_spend), len(fraud_dormant)]
        })
        chart = alt.Chart(fraud_df).mark_bar().encode(
            x=alt.X('Type:N', title='Fraud Type'),
            y=alt.Y('Count:Q', title='Count'),
            color=alt.Color('Type:N',
                scale=alt.Scale(scheme='reds')),
            tooltip=['Type','Count']
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("<div class='section-header'><b>📡 High Value Fraud by Channel</b></div>",
                    unsafe_allow_html=True)
        ch = fraud_high_value.groupby('CHANNEL').agg(
            Count=('TRANSACTION_ID','count')
        ).reset_index()
        chart = alt.Chart(ch).mark_arc(innerRadius=60).encode(
            theta=alt.Theta('Count:Q'),
            color=alt.Color('CHANNEL:N',
                scale=alt.Scale(scheme='reds')),
            tooltip=['CHANNEL','Count']
        ).properties(height=300)
        st.altair_chart(chart, use_container_width=True)

    st.markdown("<div class='section-header'><b>💸 High Value Transactions</b></div>",
                unsafe_allow_html=True)
    hv = fraud_high_value[[
        'TRANSACTION_ID','ACCOUNT_NUMBER',
        'AMOUNT','CHANNEL','TRANSACTION_TIMESTAMP'
    ]].copy().head(20)
    hv['AMOUNT'] = hv['AMOUNT'].apply(lambda x: f"₹{x:,.0f}")
    st.dataframe(hv, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════
# TAB 3 — BRANCH ANALYSIS
# ════════════════════════════════════════════════════════════════
with tab3:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>50</div>
            <div class='metric-label'>🏦 Total Branches</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            {int(branch_summary['TOTAL_TRANSACTIONS'].sum()):,}</div>
            <div class='metric-label'>💳 Total Transactions</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            ₹{branch_summary['TOTAL_AMOUNT'].sum()/10000000:.1f} Cr</div>
            <div class='metric-label'>💰 Total Amount</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-header'><b>🏆 Top 10 Branches by Amount</b></div>",
                    unsafe_allow_html=True)
        top_b = branch_summary.nlargest(10,'TOTAL_AMOUNT')
        chart = alt.Chart(top_b).mark_bar().encode(
            x=alt.X('TOTAL_AMOUNT:Q', title='Total Amount (₹)'),
            y=alt.Y('BRANCH_NAME:N', sort='-x', title='Branch'),
            color=alt.Color('REGION:N',
                scale=alt.Scale(scheme='blues')),
            tooltip=['BRANCH_NAME','CITY','REGION',
                     'TOTAL_TRANSACTIONS','TOTAL_AMOUNT']
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("<div class='section-header'><b>🗺️ Region Wise Distribution</b></div>",
                    unsafe_allow_html=True)
        chart = alt.Chart(region_summary).mark_arc(innerRadius=60).encode(
            theta=alt.Theta('TOTAL_AMOUNT:Q'),
            color=alt.Color('REGION:N',
                scale=alt.Scale(scheme='blues')),
            tooltip=['REGION','TOTAL_TRANSACTIONS',
                     'TOTAL_AMOUNT','UNIQUE_CUSTOMERS']
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)

    st.markdown("<div class='section-header'><b>📊 Region Transactions vs Customers</b></div>",
                unsafe_allow_html=True)
    reg_melted = region_summary.melt(
        'REGION',
        value_vars=['TOTAL_TRANSACTIONS','UNIQUE_CUSTOMERS'],
        var_name='Metric', value_name='Value'
    )
    chart = alt.Chart(reg_melted).mark_bar().encode(
        x=alt.X('REGION:N', title='Region'),
        y=alt.Y('Value:Q', title='Count'),
        color=alt.Color('Metric:N',
            scale=alt.Scale(
                domain=['TOTAL_TRANSACTIONS','UNIQUE_CUSTOMERS'],
                range=['#00d4ff','#0099cc']
            )),
        xOffset='Metric:N',
        tooltip=['REGION','Metric','Value']
    ).properties(height=280)
    st.altair_chart(chart, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# TAB 4 — MONTHLY TRENDS
# ════════════════════════════════════════════════════════════════
with tab4:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>{len(customer_monthly):,}</div>
            <div class='metric-label'>📅 Monthly Records</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            ₹{customer_monthly['MONTHLY_SPEND'].mean():,.0f}</div>
            <div class='metric-label'>💰 Avg Monthly Spend</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class='metric-card'>
            <div class='metric-value'>
            ₹{customer_monthly['MONTHLY_SPEND'].max():,.0f}</div>
            <div class='metric-label'>📊 Max Monthly Spend</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("<div class='section-header'><b>📈 Monthly Spend Trend by Account Type</b></div>",
                unsafe_allow_html=True)
    trend = customer_monthly.groupby(
        ['TXN_YEAR','TXN_MONTH','ACCOUNT_TYPE']
    ).agg(Total_Spend=('MONTHLY_SPEND','sum')).reset_index()
    trend['Period'] = (
        trend['TXN_YEAR'].astype(str) + '-' +
        trend['TXN_MONTH'].astype(str).str.zfill(2)
    )
    chart = alt.Chart(trend).mark_line(point=True).encode(
        x=alt.X('Period:N', title='Month',
                axis=alt.Axis(labelAngle=45)),
        y=alt.Y('Total_Spend:Q', title='Total Spend (₹)'),
        color=alt.Color('ACCOUNT_TYPE:N',
            scale=alt.Scale(scheme='blues')),
        tooltip=['Period','ACCOUNT_TYPE','Total_Spend']
    ).properties(height=350)
    st.altair_chart(chart, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-header'><b>📅 Year Wise Comparison</b></div>",
                    unsafe_allow_html=True)
        yearly = customer_monthly.groupby('TXN_YEAR').agg(
            Total_Spend=('MONTHLY_SPEND','sum')
        ).reset_index()
        yearly['TXN_YEAR'] = yearly['TXN_YEAR'].astype(str)
        chart = alt.Chart(yearly).mark_bar().encode(
            x=alt.X('TXN_YEAR:N', title='Year'),
            y=alt.Y('Total_Spend:Q', title='Total Spend (₹)'),
            color=alt.Color('TXN_YEAR:N',
                scale=alt.Scale(scheme='blues')),
            tooltip=['TXN_YEAR','Total_Spend']
        ).properties(height=280)
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("<div class='section-header'><b>🏆 Top 10 Monthly Spenders</b></div>",
                    unsafe_allow_html=True)
        top_s = customer_monthly.nlargest(10,'MONTHLY_SPEND')[[
            'CUSTOMER_NAME','ACCOUNT_TYPE',
            'TXN_YEAR','TXN_MONTH','MONTHLY_SPEND'
        ]].reset_index(drop=True)
        top_s['MONTHLY_SPEND'] = top_s['MONTHLY_SPEND'].apply(
            lambda x: f"₹{x:,.0f}"
        )
        st.dataframe(top_s, use_container_width=True, hide_index=True)

# ── Footer ───────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center; padding:15px;
background:linear-gradient(135deg,#0a1628,#1e3a5f);
border-radius:10px; margin-top:20px;'>
    <p style='color:#a0c4e8; margin:0; font-size:0.8rem;'>
    🏦 Banking Transaction Monitoring System |
    Azure Databricks + ADLS Gen2 + Snowflake + Streamlit
    </p>
</div>
""", unsafe_allow_html=True)