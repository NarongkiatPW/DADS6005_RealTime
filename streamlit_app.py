import streamlit as st
import pandas as pd
from pinotdb import connect
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="DADS6005 Realtime Dashboard", layout="wide")
st.title("✨ Stock-Trade User Interactions")
st.write("For the Midterm Examination in DADS6005 Data Streaming and Realtime Analytics")

# Function for connecting to Druid
def create_connection():
    conn = connect(host='13.212.62.78', port=8099, path='/query/sql', scheme='http')
    return conn

# Connect to Druid
conn = create_connection()

### Query 4: User Distribution by Gender and Region
query4 = """
SELECT 
    gender,
    regionid
FROM 
    users_table;
"""
curs4 = conn.cursor()
curs4.execute(query4)
result4 = curs4.fetchall()
df4 = pd.DataFrame(result4, columns=['gender', 'regionid'])

# Process data for heatmap
heatmap_data4 = df4.groupby(["gender", "regionid"]).size().reset_index(name="count")
heatmap_matrix4 = heatmap_data4.pivot(index="gender", columns="regionid", values="count").fillna(0)

fig4 = go.Figure(
    data=go.Heatmap(
        z=heatmap_matrix4.values,
        x=heatmap_matrix4.columns,
        y=heatmap_matrix4.index,
        colorscale=px.colors.sequential.Cividis_r
    )
)
fig4.update_layout(
    title="User Distribution by Gender and Region",
    xaxis_title="Region",
    yaxis_title="Gender"
)

### Query 1: Transaction Count by User (BUY)
query1 = """
SELECT 
    userid,
    COUNT(symbol) AS Transaction_Count
FROM 
    Stock_stream
WHERE 
    side = 'BUY'
GROUP BY 
    userid
ORDER BY 
    Transaction_Count DESC;
"""
curs1 = conn.cursor()
curs1.execute(query1)
result1 = curs1.fetchall()
df1 = pd.DataFrame(result1, columns=['userid', 'Transaction_Count'])

fig1 = go.Figure(
    go.Bar(
        x=df1["Transaction_Count"],
        y=df1["userid"],
        orientation='h',
        text=df1["Transaction_Count"],
        textposition='outside',
        marker=dict(color=df1["Transaction_Count"], colorscale=px.colors.sequential.Cividis_r)
    )
)
fig1.update_layout(
    title="User Ranking in Purchase Stock_Trade",
    xaxis_title="Transaction Count",
    yaxis_title="User ID",
    yaxis=dict(categoryorder='total ascending')
)

### Query 2: Transaction Count by User (SELL)
query2 = """
SELECT 
    userid,
    COUNT(symbol) AS Transaction_Count
FROM 
    Stock_stream
WHERE 
    side = 'SELL'
GROUP BY 
    userid
ORDER BY 
    Transaction_Count DESC;
"""
curs2 = conn.cursor()
curs2.execute(query2)
result2 = curs2.fetchall()
df2 = pd.DataFrame(result2, columns=['userid', 'Transaction_Count'])

fig2 = go.Figure(
    go.Bar(
        x=df2["Transaction_Count"],
        y=df2["userid"],
        orientation='h',
        text=df2["Transaction_Count"],
        textposition='outside',
        marker=dict(color=df2["Transaction_Count"], colorscale=px.colors.sequential.Cividis_r)
    )
)
fig2.update_layout(
    title="User Ranking in Selling Stock_Trade",
    xaxis_title="Transaction Count",
    yaxis_title="User ID",
    yaxis=dict(categoryorder='total ascending')
)

### Query 3: User Interest by Stock Symbol
query3 = """
SELECT 
    symbol,
    COUNT(DISTINCT userid) AS user_count
FROM 
    Stock_stream
GROUP BY 
    symbol
ORDER BY 
    user_count DESC;
"""
curs3 = conn.cursor()
curs3.execute(query3)
result3 = curs3.fetchall()
df3 = pd.DataFrame(result3, columns=['symbol', 'user_count'])

fig3 = go.Figure(
    go.Bar(
        x=df3["user_count"],
        y=df3["symbol"],
        orientation='h',
        text=df3["user_count"],
        textposition='outside',
        marker=dict(color=df3["user_count"], colorscale=px.colors.sequential.Cividis_r)
    )
)
fig3.update_layout(
    title="User Interested Stock",
    xaxis_title="User Count",
    yaxis_title="Stock Symbol",
    yaxis=dict(categoryorder='total ascending')
)

# Layout: Arrange graphs in a grid format
col1, col2 = st.columns(2)
col1.plotly_chart(fig4, use_container_width=True)  # Heatmap in left column
col2.plotly_chart(fig1, use_container_width=True)  # Bar chart for "BUY" in right column

col3, col4 = st.columns(2)
col3.plotly_chart(fig2, use_container_width=True)  # Bar chart for "SELL" in left column
col4.plotly_chart(fig3, use_container_width=True)  # Bar chart for "Interest" in right column
