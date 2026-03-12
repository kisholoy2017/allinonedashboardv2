import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from datetime import datetime, timedelta
import yaml
import tempfile
import os
import re
import requests
import json
import hmac
import hashlib

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Multi-Platform Marketing Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header { font-size:2.5rem; font-weight:bold; color:#1f77b4; text-align:center; margin-bottom:1rem; }
    .metric-card { background-color:#f0f2f6; padding:1rem; border-radius:0.5rem; text-align:center; }
    .metric-value { font-size:2rem; font-weight:bold; color:#1f77b4; }
    .metric-label { font-size:0.9rem; color:#555; margin-top:0.5rem; }
    .positive-change { color:#28a745; font-weight:bold; }
    .negative-change { color:#dc3545; font-weight:bold; }
    div[data-testid="stHorizontalBlock"] { overflow-x:auto; }
    .dataframe-container { overflow-x:auto; }
    .platform-badge-google  { background:#e8f0fe; color:#1a73e8; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
    .platform-badge-meta    { background:#e7f3ff; color:#1877f2; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
    .platform-badge-tiktok  { background:#f0f0f0; color:#000;    padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
    .platform-badge-shopify { background:#e6f4ea; color:#2e7d32; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
defaults = {
    # Google
    'authenticated': False, 'client': None, 'customer_id': None,
    'data_loaded': False,
    'aggregate_data': None, 'campaign_data': None, 'product_data': None,
    'daily_data': None, 'daily_data_camp': None,
    'change_history_data': None,
    'daily_data_comparison': None, 'daily_data_camp_comparison': None,
    'google_connected': False, 'google_csv_uploaded': False,
    # Meta
    'meta_connected': False, 'meta_csv_uploaded': False,
    'meta_access_token': None, 'meta_ad_account_id': None,
    'meta_app_id': None, 'meta_app_secret': None,
    'meta_account_info': None,
    'meta_campaign_data': None, 'meta_daily_data': None,
    'meta_data': None,
    # TikTok / Shopify
    'tiktok_connected': False, 'tiktok_csv_uploaded': False, 'tiktok_data': None,
    'shopify_connected': False, 'shopify_csv_uploaded': False, 'shopify_data': None,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
# GOOGLE HELPERS
# ─────────────────────────────────────────────

def create_google_ads_client(developer_token, client_id, client_secret, refresh_token, login_customer_id=None):
    try:
        config_dict = {
            "developer_token": developer_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "use_proto_plus": True
        }
        if login_customer_id:
            config_dict["login_customer_id"] = login_customer_id
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_dict, f)
            config_file = f.name
        client = GoogleAdsClient.load_from_dict(config_dict)
        os.unlink(config_file)
        return client
    except Exception as e:
        st.error(f"Error creating Google Ads client: {str(e)}")
        return None

def format_date_for_query(date_obj):
    return date_obj.strftime('%Y-%m-%d')

def process_dataframe(df):
    if df.empty:
        return df
    df['cost'] = df['cost'] / 1_000_000
    df['cpc'] = df.apply(lambda x: x['cost'] / x['clicks'] if x['clicks'] > 0 else 0, axis=1)
    df['ctr'] = df.apply(lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, axis=1)
    df['cost_per_conv'] = df.apply(lambda x: x['cost'] / x['conversions'] if x['conversions'] > 0 else 0, axis=1)
    df['conv_value_cost'] = df.apply(lambda x: x['conversions_value'] / x['cost'] if x['cost'] > 0 else 0, axis=1)
    df['aov'] = df.apply(lambda x: x['conversions_value'] / x['conversions'] if x['conversions'] > 0 else 0, axis=1)
    return df

def recalculate_metrics(df):
    if df.empty:
        return df
    df['cpc'] = df.apply(lambda x: x['cost'] / x['clicks'] if x['clicks'] > 0 else 0, axis=1)
    df['ctr'] = df.apply(lambda x: (x['clicks'] / x['impressions'] * 100) if x['impressions'] > 0 else 0, axis=1)
    df['cost_per_conv'] = df.apply(lambda x: x['cost'] / x['conversions'] if x['conversions'] > 0 else 0, axis=1)
    df['conv_value_cost'] = df.apply(lambda x: x['conversions_value'] / x['cost'] if x['cost'] > 0 else 0, axis=1)
    df['aov'] = df.apply(lambda x: x['conversions_value'] / x['conversions'] if x['conversions'] > 0 else 0, axis=1)
    return df

def calculate_share_metrics(df):
    if df.empty:
        return df
    total_cost = df['cost'].sum()
    total_revenue = df['conversions_value'].sum()
    df['soc'] = (df['cost'] / total_cost * 100) if total_cost > 0 else 0
    df['sor'] = (df['conversions_value'] / total_revenue * 100) if total_revenue > 0 else 0
    df['soc_sor_ratio'] = df.apply(lambda r: r['soc'] / r['sor'] if r['sor'] > 0 else 0, axis=1)
    return df

def calculate_last_3_days_metrics(daily_df, campaign_budgets=None):
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    try:
        max_date = daily_df['date'].max()
        min_date = daily_df['date'].min()
        last_3_start = max_date - timedelta(days=2)
        prev_3_end = last_3_start - timedelta(days=1)
        prev_3_start = prev_3_end - timedelta(days=2)
        if (max_date - min_date).days < 5:
            return pd.DataFrame()
        last_3 = daily_df[daily_df['date'] >= last_3_start].copy()
        prev_3 = daily_df[(daily_df['date'] >= prev_3_start) & (daily_df['date'] <= prev_3_end)].copy()
        if last_3.empty or prev_3.empty:
            return pd.DataFrame()
        last_3_agg = last_3.groupby('campaign_name').agg({'cost':'sum','conversions_value':'sum'}).reset_index()
        last_3_agg.columns = ['campaign_name','cost_last3','revenue_last3']
        prev_3_agg = prev_3.groupby('campaign_name').agg({'cost':'sum','conversions_value':'sum'}).reset_index()
        prev_3_agg.columns = ['campaign_name','cost_prev3','revenue_prev3']
        merged = last_3_agg.merge(prev_3_agg, on='campaign_name', how='outer').fillna(0)
        merged['spend_delta_3d']   = merged.apply(lambda x: ((x['cost_last3']-x['cost_prev3'])/x['cost_prev3']*100) if x['cost_prev3']>0 else 0, axis=1)
        merged['revenue_delta_3d'] = merged.apply(lambda x: ((x['revenue_last3']-x['revenue_prev3'])/x['revenue_prev3']*100) if x['revenue_prev3']>0 else 0, axis=1)
        merged['delta_ratio_3d']   = merged.apply(lambda x: x['revenue_delta_3d']/x['spend_delta_3d'] if abs(x['spend_delta_3d'])>0.1 else 0, axis=1)
        if campaign_budgets is not None and not campaign_budgets.empty:
            merged = merged.merge(campaign_budgets[['campaign_name','budget']], on='campaign_name', how='left')
            merged['budget_spent_3d_pct'] = merged.apply(lambda x: (x['cost_last3']/x['budget']*100) if x.get('budget',0)>0 else 0, axis=1)
        result_cols = ['campaign_name','cost_last3','spend_delta_3d','revenue_delta_3d','delta_ratio_3d']
        if 'budget_spent_3d_pct' in merged.columns:
            result_cols.append('budget_spent_3d_pct')
        return merged[result_cols]
    except Exception as e:
        return pd.DataFrame()

def format_delta_html(value, reverse_colors=False):
    if abs(value) < 0.1:
        return "0.0%"
    arrow = "▲" if value > 0 else "▼"
    if reverse_colors:
        color = "#dc2626" if value > 0 else "#059669"
    else:
        color = "#059669" if value > 0 else "#dc2626"
    return f'<span style="color:{color};font-weight:600;">{arrow} {abs(value):.1f}%</span>'

def fetch_campaign_performance(client, customer_id, start_date, end_date):
    try:
        ga_service = client.get_service("GoogleAdsService")
        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign_budget.amount_micros,
                   metrics.cost_micros, metrics.clicks, metrics.impressions,
                   metrics.conversions, metrics.conversions_value,
                   metrics.ctr, metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{format_date_for_query(start_date)}' AND '{format_date_for_query(end_date)}'
              AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
        """
        response = ga_service.search(customer_id=customer_id, query=query)
        data = []
        for row in response:
            budget = 0
            if hasattr(row,'campaign_budget') and hasattr(row.campaign_budget,'amount_micros'):
                budget = row.campaign_budget.amount_micros / 1_000_000
            data.append({
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'campaign_status': row.campaign.status.name,
                'budget': budget,
                'cost': row.metrics.cost_micros,
                'clicks': row.metrics.clicks,
                'impressions': row.metrics.impressions,
                'conversions': row.metrics.conversions,
                'conversions_value': row.metrics.conversions_value,
            })
        return pd.DataFrame(data)
    except GoogleAdsException as ex:
        st.error(f"Google Ads API error: {ex}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching campaign data: {str(e)}")
        return pd.DataFrame()

def fetch_product_performance(client, customer_id, start_date, end_date):
    try:
        ga_service = client.get_service("GoogleAdsService")
        query = f"""
            SELECT campaign.name,
                   segments.product_title, segments.product_item_id,
                   metrics.cost_micros, metrics.clicks, metrics.impressions,
                   metrics.conversions, metrics.conversions_value
            FROM shopping_performance_view
            WHERE segments.date BETWEEN '{format_date_for_query(start_date)}' AND '{format_date_for_query(end_date)}'
            ORDER BY metrics.cost_micros DESC
        """
        response = ga_service.search(customer_id=customer_id, query=query)
        data = []
        for row in response:
            data.append({
                'campaign_name': row.campaign.name,
                'product_title': row.segments.product_title,
                'product_item_id': row.segments.product_item_id,
                'cost': row.metrics.cost_micros,
                'clicks': row.metrics.clicks,
                'impressions': row.metrics.impressions,
                'conversions': row.metrics.conversions,
                'conversions_value': row.metrics.conversions_value,
            })
        return pd.DataFrame(data)
    except GoogleAdsException as ex:
        st.error(f"Google Ads API error: {ex}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching product data: {str(e)}")
        return pd.DataFrame()

def fetch_daily_performance(client, customer_id, start_date, end_date):
    try:
        ga_service = client.get_service("GoogleAdsService")
        query = f"""
            SELECT segments.date, campaign.name,
                   metrics.cost_micros, metrics.clicks, metrics.impressions,
                   metrics.conversions, metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{format_date_for_query(start_date)}' AND '{format_date_for_query(end_date)}'
              AND campaign.status != 'REMOVED'
            ORDER BY segments.date
        """
        response = ga_service.search(customer_id=customer_id, query=query)
        data = []
        for row in response:
            data.append({
                'date': row.segments.date,
                'campaign_name': row.campaign.name,
                'cost': row.metrics.cost_micros / 1_000_000,
                'clicks': row.metrics.clicks,
                'impressions': row.metrics.impressions,
                'conversions': row.metrics.conversions,
                'conversions_value': row.metrics.conversions_value,
            })
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df = recalculate_metrics(df)
        return df
    except Exception as e:
        st.error(f"Error fetching daily data: {str(e)}")
        return pd.DataFrame()

def fetch_change_history(client, customer_id, start_date, end_date):
    try:
        ga_service = client.get_service("GoogleAdsService")
        start_datetime = f"{format_date_for_query(start_date)} 00:00:00"
        end_datetime   = f"{format_date_for_query(end_date)} 23:59:59"
        query = f"""
            SELECT change_event.change_date_time, change_event.change_resource_type,
                   change_event.resource_change_operation, change_event.change_resource_name,
                   change_event.old_resource, change_event.new_resource,
                   campaign.name, campaign.id
            FROM change_event
            WHERE change_event.change_date_time >= '{start_datetime}'
              AND change_event.change_date_time <= '{end_datetime}'
              AND change_event.change_resource_type IN ('CAMPAIGN','CAMPAIGN_BUDGET')
            ORDER BY change_event.change_date_time DESC
            LIMIT 1000
        """
        response = ga_service.search(customer_id=customer_id, query=query)
        data = []
        for row in response:
            resource_type_str = str(row.change_event.change_resource_type)
            resource_name     = str(row.change_event.change_resource_name).lower()
            old_resource = str(row.change_event.old_resource) if hasattr(row.change_event,'old_resource') else ''
            new_resource = str(row.change_event.new_resource) if hasattr(row.change_event,'new_resource') else ''
            change_content = f"{old_resource} {new_resource} {resource_name}".lower()
            is_budget = ('BUDGET' in resource_type_str.upper() or 'budget' in resource_name or
                         'amount_micros' in change_content or 'budget_amount' in change_content)
            is_bid = any(k in change_content for k in [
                'bidding_strategy','bid_strategy','maximize_conversions','maximize_conversion_value',
                'target_cpa','target_roas','manual_cpc','manual_cpm','target_spend',
                'target_impression_share','percent_cpc','commission'])
            if not (is_budget or is_bid):
                continue
            change_type = 'Budget Change' if is_budget else 'Bid Strategy Change'
            change_details = extract_change_details(old_resource, new_resource, is_budget, is_bid)
            operation_str = str(row.change_event.resource_change_operation)
            data.append({
                'change_datetime': row.change_event.change_date_time,
                'resource_type': resource_type_str,
                'operation': operation_str,
                'resource_name': row.change_event.change_resource_name,
                'campaign_name': row.campaign.name if hasattr(row,'campaign') and hasattr(row.campaign,'name') else 'Unknown',
                'campaign_id': str(row.campaign.id) if hasattr(row,'campaign') and hasattr(row.campaign,'id') else '',
                'change_type': change_type,
                'change_details': change_details,
                'old_resource': old_resource,
                'new_resource': new_resource,
            })
        df = pd.DataFrame(data)
        if not df.empty:
            df['change_datetime'] = pd.to_datetime(df['change_datetime'])
            df['date'] = df['change_datetime'].dt.date
            df['time'] = df['change_datetime'].dt.strftime('%H:%M:%S')
            df['operation'] = df['operation'].replace({'CREATE':'Created','UPDATE':'Updated','REMOVE':'Removed'})
        return df
    except GoogleAdsException as ex:
        st.error(f"Google Ads API error fetching change history: {ex}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching change history: {str(e)}")
        return pd.DataFrame()

def extract_change_details(old_resource, new_resource, is_budget, is_bid_strategy):
    try:
        details = []
        if is_budget:
            old_amount = extract_budget_amount(old_resource)
            new_amount = extract_budget_amount(new_resource)
            if old_amount and new_amount:
                old_val, new_val = old_amount/1_000_000, new_amount/1_000_000
                if old_val != new_val:
                    direction = "increased" if new_val > old_val else "decreased"
                    details.append(f"Budget {direction} from {old_val:.2f} to {new_val:.2f}")
            elif new_amount:
                details.append(f"Budget set to {new_amount/1_000_000:.2f}")
            elif old_amount:
                details.append("Budget removed")
        if is_bid_strategy:
            old_s = extract_bid_strategy(old_resource)
            new_s = extract_bid_strategy(new_resource)
            if old_s and new_s and old_s != new_s:
                details.append(f"Strategy changed from {old_s} to {new_s}")
            elif new_s:
                details.append(f"Strategy set to {new_s}")
            old_cpa = extract_target_cpa(old_resource)
            new_cpa = extract_target_cpa(new_resource)
            if old_cpa and new_cpa and old_cpa != new_cpa:
                old_v, new_v = old_cpa/1_000_000, new_cpa/1_000_000
                details.append(f"Target CPA {'increased' if new_v>old_v else 'decreased'} from {old_v:.2f} to {new_v:.2f}")
            old_roas = extract_target_roas(old_resource)
            new_roas = extract_target_roas(new_resource)
            if old_roas and new_roas and old_roas != new_roas:
                details.append(f"Target ROAS {'increased' if new_roas>old_roas else 'decreased'} from {old_roas*100:.0f}% to {new_roas*100:.0f}%")
        return " | ".join(details) if details else "Change detected"
    except:
        return "Change detected"

def extract_budget_amount(resource_str):
    try:
        m = re.search(r'amount_micros:\s*(\d+)', resource_str)
        if m: return int(m.group(1))
    except: pass
    return None

def extract_bid_strategy(resource_str):
    s = resource_str.lower()
    if 'maximize_conversion_value' in s: return 'Maximize Conversion Value'
    if 'maximize_conversions' in s:      return 'Maximize Conversions'
    if 'target_cpa' in s:                return 'Target CPA'
    if 'target_roas' in s:               return 'Target ROAS'
    if 'target_spend' in s:              return 'Target Spend'
    if 'manual_cpc' in s:                return 'Manual CPC'
    if 'manual_cpm' in s:                return 'Manual CPM'
    if 'percent_cpc' in s:               return 'Commission'
    return None

def extract_target_cpa(resource_str):
    try:
        m = re.search(r'target_cpa_micros:\s*(\d+)', resource_str)
        if m: return int(m.group(1))
    except: pass
    return None

def extract_target_roas(resource_str):
    try:
        m = re.search(r'target_roas:\s*([\d.]+)', resource_str)
        if m: return float(m.group(1))
    except: pass
    return None

def extract_percentage_change(details_str):
    try:
        m = re.search(r'from ([\d.]+)\D* to ([\d.]+)', details_str)
        if m:
            old_val, new_val = float(m.group(1)), float(m.group(2))
            if old_val > 0:
                return abs((new_val - old_val) / old_val * 100)
    except: pass
    return 0

def add_change_annotations(fig, df_changes, campaign_name, date_range, min_budget_pct=0, min_bid_pct=0):
    if df_changes is None or df_changes.empty:
        return fig
    required_cols = ['campaign_name','date','change_type','change_details']
    if not all(c in df_changes.columns for c in required_cols):
        return fig
    try:
        campaign_changes = df_changes[
            (df_changes['campaign_name'] == campaign_name) &
            (df_changes['date'] >= date_range[0]) &
            (df_changes['date'] <= date_range[1])
        ].copy()
        if campaign_changes.empty:
            return fig
        shapes, annotations = [], []
        for _, change in campaign_changes.iterrows():
            change_date  = change['date']
            change_type  = change['change_type']
            details      = change['change_details']
            show_ann     = False
            color        = '#6b7280'
            if change_type == 'Budget Change':
                pct = extract_percentage_change(details)
                if 'set to' in details or 'removed' in details or pct >= min_budget_pct:
                    show_ann = True
                color = '#f59e0b'
            elif change_type == 'Bid Strategy Change':
                if 'Strategy changed' in details:
                    show_ann = True
                else:
                    pct = extract_percentage_change(details)
                    if pct >= min_bid_pct:
                        show_ann = True
                color = '#8b5cf6'
            if not show_ann:
                continue
            shapes.append(dict(type="line",xref="x",yref="paper",x0=change_date,x1=change_date,
                               y0=0,y1=1,line=dict(color=color,width=2,dash="dot"),opacity=0.6))
            short_details = details[:40] + "..." if len(details) > 40 else details
            annotations.append(dict(x=change_date,y=1.02,xref="x",yref="paper",
                text=f"<b>{change_type.split()[0]}</b><br>{short_details}",
                showarrow=True,arrowhead=2,arrowsize=1,arrowwidth=2,arrowcolor=color,
                ax=0,ay=-50,bgcolor="rgba(255,255,255,0.9)",bordercolor=color,
                borderwidth=2,borderpad=4,font=dict(size=9,color="#111827"),align="center"))
        fig.update_layout(shapes=shapes, annotations=annotations)
    except:
        pass
    return fig

def create_time_series_chart(df, metric, metric_label):
    daily_agg = df.groupby('date')[metric].sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_agg['date'], y=daily_agg[metric], mode='lines',
        name=metric_label,
        line=dict(color='rgb(0,204,204)', width=3, shape='spline'),
        fill='tozeroy', fillcolor='rgba(0,204,204,0.1)'
    ))
    fig.update_layout(
        title=dict(text=f"{metric_label} Over Time", font=dict(size=20,color='#333')),
        xaxis=dict(showgrid=True, gridcolor='rgba(200,200,200,0.2)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(200,200,200,0.2)', showline=False),
        plot_bgcolor='white', paper_bgcolor='white', hovermode='x unified',
        height=400, margin=dict(l=50,r=50,t=80,b=50),
        legend=dict(orientation="h",yanchor="bottom",y=-0.2,xanchor="center",x=0.5)
    )
    return fig

def create_multi_metric_chart(df_current, df_comparison, selected_metrics, metric_labels, show_comparison=False):
    fig = go.Figure()
    colors = ['#1e88e5','#43a047','#e53935']
    comparison_colors = ['#90caf9','#81c784','#e57373']
    for idx, metric in enumerate(selected_metrics):
        daily_agg = df_current.groupby('date')[metric].sum().reset_index()
        yaxis_ref = 'y' if idx == 0 else f'y{idx+1}'
        fig.add_trace(go.Scatter(
            x=daily_agg['date'], y=daily_agg[metric], mode='lines+markers',
            name=metric_labels[metric],
            line=dict(color=colors[idx], width=3), marker=dict(size=6),
            yaxis=yaxis_ref
        ))
    if show_comparison and df_comparison is not None and not df_comparison.empty:
        for idx, metric in enumerate(selected_metrics):
            daily_agg_comp = df_comparison.groupby('date')[metric].sum().reset_index()
            yaxis_ref = 'y' if idx == 0 else f'y{idx+1}'
            fig.add_trace(go.Scatter(
                x=daily_agg_comp['date'], y=daily_agg_comp[metric], mode='lines',
                name=f"{metric_labels[metric]} (Comparison)",
                line=dict(color=comparison_colors[idx], width=2, dash='dash'),
                yaxis=yaxis_ref, opacity=0.7
            ))
    layout_config = dict(
        title=dict(text="Performance Over Time", font=dict(size=24,color='#111827',weight=700)),
        xaxis=dict(title=dict(text="Date",font=dict(size=16,color='#374151',weight=600)),
                   showgrid=True, gridcolor='rgba(200,200,200,0.2)', tickfont=dict(size=13)),
        yaxis=dict(title=dict(text=metric_labels[selected_metrics[0]],font=dict(size=16,color='#374151',weight=600)),
                   showgrid=True, gridcolor='rgba(200,200,200,0.2)', side='left', tickfont=dict(size=13)),
        plot_bgcolor='white', paper_bgcolor='white', hovermode='x unified', height=500,
        legend=dict(orientation="h",yanchor="bottom",y=-0.25,xanchor="center",x=0.5,font=dict(size=13,weight=600)),
        margin=dict(l=60,r=60,t=80,b=100)
    )
    if len(selected_metrics) > 1:
        layout_config['yaxis2'] = dict(title=dict(text=metric_labels[selected_metrics[1]],font=dict(size=16,color='#374151',weight=600)),
                                        overlaying='y', side='right', showgrid=False, tickfont=dict(size=13))
    if len(selected_metrics) > 2:
        layout_config['yaxis3'] = dict(title=dict(text=metric_labels[selected_metrics[2]],font=dict(size=16,color='#374151',weight=600)),
                                        overlaying='y', side='right', anchor='free', position=0.97,
                                        showgrid=False, tickfont=dict(size=13))
    fig.update_layout(**layout_config)
    return fig

def calculate_comparison(current_df, comparison_df):
    if comparison_df.empty:
        return current_df
    def totals(df):
        t = {
            'cost': df['cost'].sum(), 'clicks': df['clicks'].sum(),
            'impressions': df['impressions'].sum(), 'conversions': df['conversions'].sum(),
            'conversions_value': df['conversions_value'].sum()
        }
        t['cpc']            = t['cost'] / t['clicks'] if t['clicks'] > 0 else 0
        t['ctr']            = t['clicks'] / t['impressions'] * 100 if t['impressions'] > 0 else 0
        t['cost_per_conv']  = t['cost'] / t['conversions'] if t['conversions'] > 0 else 0
        t['conv_value_cost']= t['conversions_value'] / t['cost'] if t['cost'] > 0 else 0
        t['aov']            = t['conversions_value'] / t['conversions'] if t['conversions'] > 0 else 0
        return t
    cur = totals(current_df)
    comp = totals(comparison_df)
    changes = {f'{m}_change': ((cur[m]-comp[m])/comp[m]*100) if comp[m] != 0 else 0 for m in cur}
    return cur, comp, changes

def display_metric_card(label, value, change=None, metric_type='currency', inverse=False):
    def fmt(v, t):
        if t == 'currency':   return f"${v:,.2f}"
        if t == 'percentage': return f"{v:.2f}%"
        if t == 'number':     return f"{v:,.0f}"
        return f"{v:.2f}"
    formatted_value = fmt(value, metric_type)
    if change is not None and change != 0:
        arrow = "↑" if change > 0 else "↓"
        good = (change > 0 and not inverse) or (change < 0 and inverse)
        cls  = "positive-change" if good else "negative-change"
        change_text = f'<span class="{cls}">{arrow} {abs(change):.1f}%</span>'
        body = f"{formatted_value} {change_text}"
    else:
        body = formatted_value
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{body}</div></div>'

# ─────────────────────────────────────────────
# META HELPERS
# ─────────────────────────────────────────────

META_API_VERSION = "v20.0"
META_BASE_URL    = f"https://graph.facebook.com/{META_API_VERSION}"

def _meta_appsecret_proof(access_token: str, app_secret: str) -> str:
    """Generate appsecret_proof for added security on API calls."""
    return hmac.new(app_secret.encode('utf-8'), access_token.encode('utf-8'), hashlib.sha256).hexdigest()

def validate_meta_connection(access_token: str, ad_account_id: str, app_id: str = None, app_secret: str = None):
    """
    Validate Meta Ads API connection.
    Returns (success: bool, error_message: str | None, account_info: dict | None)
    """
    try:
        if not ad_account_id.startswith('act_'):
            ad_account_id = f'act_{ad_account_id}'

        params = {
            'access_token': access_token,
            'fields': 'name,id,currency,account_status,timezone_name,spend_cap,amount_spent'
        }
        # appsecret_proof is intentionally omitted during validation.
        # Only include it on data calls if the Meta app has "Require App Secret" enforced.

        resp = requests.get(f"{META_BASE_URL}/{ad_account_id}", params=params, timeout=15)
        result = resp.json()

        if 'error' in result:
            return False, result['error']['message'], None

        # Map account_status code to human-readable
        status_map = {1:'Active', 2:'Disabled', 3:'Unsettled', 7:'Pending Review',
                      8:'Pending Closure', 9:'In Grace Period', 100:'Temporarily Unavailable',
                      101:'Closed', 201:'Any Active', 202:'Any Closed'}
        raw_status = result.get('account_status', 0)
        result['account_status_label'] = status_map.get(raw_status, f"Unknown ({raw_status})")
        result['ad_account_id'] = ad_account_id

        return True, None, result

    except requests.exceptions.Timeout:
        return False, "Connection timed out. Check your internet connection.", None
    except Exception as e:
        return False, str(e), None

def _meta_extract_conversions(row_data: dict):
    """
    Extract purchase conversions and value from a Meta insights row.
    Priority: omni_purchase > purchase > offsite_conversion.fb_pixel_purchase
    """
    priority_types = ['omni_purchase', 'purchase', 'offsite_conversion.fb_pixel_purchase']

    conversions = 0.0
    conv_value  = 0.0

    actions       = row_data.get('actions', [])
    action_values = row_data.get('action_values', [])

    for at in priority_types:
        for action in actions:
            if action.get('action_type') == at:
                conversions = float(action.get('value', 0))
                break
        if conversions > 0:
            break

    for at in priority_types:
        for av in action_values:
            if av.get('action_type') == at:
                conv_value = float(av.get('value', 0))
                break
        if conv_value > 0:
            break

    return conversions, conv_value

def _meta_paginate(initial_url: str, initial_params: dict, timeout: int = 30):
    """
    Generator that yields rows from a Meta API paginated response.
    Handles cursor-based pagination automatically.
    """
    current_url    = initial_url
    current_params = initial_params

    while current_url:
        try:
            resp   = requests.get(current_url, params=current_params, timeout=timeout)
            result = resp.json()
        except requests.exceptions.Timeout:
            st.warning("Meta API request timed out during pagination. Returning partial data.")
            break
        except Exception as e:
            st.error(f"Meta API request error: {e}")
            break

        if 'error' in result:
            st.error(f"Meta API error: {result['error'].get('message','Unknown error')} "
                     f"(code {result['error'].get('code','')})")
            break

        for row in result.get('data', []):
            yield row

        paging   = result.get('paging', {})
        next_url = paging.get('next')
        if next_url and next_url != current_url:
            current_url    = next_url
            current_params = {}          # all params are already encoded in the next URL
        else:
            break

def fetch_meta_campaign_performance(access_token: str, ad_account_id: str,
                                    start_date, end_date,
                                    app_secret: str = None) -> pd.DataFrame:
    """Fetch Meta campaign-level performance data."""
    if not ad_account_id.startswith('act_'):
        ad_account_id = f'act_{ad_account_id}'

    params = {
        'access_token': access_token,
        'fields': ('campaign_name,campaign_id,adset_name,'
                   'spend,clicks,impressions,reach,frequency,'
                   'actions,action_values,cost_per_action_type'),
        'level': 'campaign',
        'time_range': json.dumps({
            'since': start_date.strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d')
        }),
        'limit': 500,
    }

    url  = f"{META_BASE_URL}/{ad_account_id}/insights"
    rows = []

    for row in _meta_paginate(url, params):
        conversions, conv_value = _meta_extract_conversions(row)
        rows.append({
            'campaign_name':     row.get('campaign_name', 'Unknown'),
            'campaign_id':       row.get('campaign_id', ''),
            'cost':              float(row.get('spend', 0)),
            'clicks':            int(row.get('clicks', 0)),
            'impressions':       int(row.get('impressions', 0)),
            'reach':             int(row.get('reach', 0)),
            'frequency':         float(row.get('frequency', 0)),
            'conversions':       conversions,
            'conversions_value': conv_value,
            'platform':          'Meta',
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df['cpc']             = df.apply(lambda x: x['cost']/x['clicks'] if x['clicks']>0 else 0, axis=1)
        df['ctr']             = df.apply(lambda x: x['clicks']/x['impressions']*100 if x['impressions']>0 else 0, axis=1)
        df['cost_per_conv']   = df.apply(lambda x: x['cost']/x['conversions'] if x['conversions']>0 else 0, axis=1)
        df['conv_value_cost'] = df.apply(lambda x: x['conversions_value']/x['cost'] if x['cost']>0 else 0, axis=1)
        df['aov']             = df.apply(lambda x: x['conversions_value']/x['conversions'] if x['conversions']>0 else 0, axis=1)
    return df

def fetch_meta_daily_performance(access_token: str, ad_account_id: str,
                                 start_date, end_date,
                                 app_secret: str = None) -> pd.DataFrame:
    """Fetch Meta daily performance data (time_increment=1)."""
    if not ad_account_id.startswith('act_'):
        ad_account_id = f'act_{ad_account_id}'

    params = {
        'access_token': access_token,
        'fields': 'campaign_name,campaign_id,spend,clicks,impressions,reach,actions,action_values',
        'level': 'campaign',
        'time_increment': '1',
        'time_range': json.dumps({
            'since': start_date.strftime('%Y-%m-%d'),
            'until': end_date.strftime('%Y-%m-%d')
        }),
        'limit': 500,
    }

    url  = f"{META_BASE_URL}/{ad_account_id}/insights"
    rows = []

    for row in _meta_paginate(url, params):
        conversions, conv_value = _meta_extract_conversions(row)
        rows.append({
            'date':              row.get('date_start', ''),
            'campaign_name':     row.get('campaign_name', 'Unknown'),
            'campaign_id':       row.get('campaign_id', ''),
            'cost':              float(row.get('spend', 0)),
            'clicks':            int(row.get('clicks', 0)),
            'impressions':       int(row.get('impressions', 0)),
            'reach':             int(row.get('reach', 0)),
            'conversions':       conversions,
            'conversions_value': conv_value,
            'platform':          'Meta',
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df['date']            = pd.to_datetime(df['date'])
        df['cpc']             = df.apply(lambda x: x['cost']/x['clicks'] if x['clicks']>0 else 0, axis=1)
        df['ctr']             = df.apply(lambda x: x['clicks']/x['impressions']*100 if x['impressions']>0 else 0, axis=1)
        df['cost_per_conv']   = df.apply(lambda x: x['cost']/x['conversions'] if x['conversions']>0 else 0, axis=1)
        df['conv_value_cost'] = df.apply(lambda x: x['conversions_value']/x['cost'] if x['cost']>0 else 0, axis=1)
        df['aov']             = df.apply(lambda x: x['conversions_value']/x['conversions'] if x['conversions']>0 else 0, axis=1)
    return df

# ─────────────────────────────────────────────
# SHARED CHART HELPERS
# ─────────────────────────────────────────────

def render_hero_kpi_cards(df, platform_label=""):
    """Render Top Revenue / Highest Spend / Best ROAS KPI cards."""
    name_col = 'campaign_name' if 'campaign_name' in df.columns else df.columns[0]
    top_rev   = df.nlargest(1, 'conversions_value').iloc[0]
    top_spend = df.nlargest(1, 'cost').iloc[0]
    best_roas = df.nlargest(1, 'conv_value_cost').iloc[0]
    col1, col2, col3 = st.columns(3)

    def _card(col, icon, title, big_val, sub1, sub2):
        col.markdown(f"""
        <div style="background:white;padding:20px;border-radius:8px;
                    box-shadow:0 1px 3px rgba(0,0,0,0.08);border:1px solid #e5e7eb;">
            <div style="font-size:13px;font-weight:500;color:#6b7280;
                        text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">
                {icon} {title}
            </div>
            <div style="font-size:28px;font-weight:700;color:#111827;margin-bottom:8px;">{big_val}</div>
            <div style="font-size:14px;color:#6b7280;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{sub1}</div>
            <div style="font-size:13px;color:#9ca3af;">{sub2}</div>
        </div>""", unsafe_allow_html=True)

    _card(col1, "🥇", f"Top Revenue{' — '+platform_label if platform_label else ''}",
          f"${top_rev['conversions_value']:,.2f}",
          top_rev[name_col][:50],
          f"{top_rev['conversions']:.0f} conversions")
    _card(col2, "💰", f"Highest Spend{' — '+platform_label if platform_label else ''}",
          f"${top_spend['cost']:,.2f}",
          top_spend[name_col][:50],
          f"{top_spend['clicks']:.0f} clicks")
    _card(col3, "🎯", f"Best ROAS{' — '+platform_label if platform_label else ''}",
          f"{best_roas['conv_value_cost']:.2f}x",
          best_roas[name_col][:50],
          f"${best_roas['conversions_value']:,.2f} revenue")

def render_top5_bar_chart(df, name_col, default_metrics, metric_options, key_suffix):
    """Render multi-metric grouped bar chart for top 5 rows."""
    top5 = df.nlargest(5, 'conversions_value')
    selected = st.multiselect(
        "Select metrics to compare",
        options=list(metric_options.keys()),
        default=default_metrics,
        max_selections=3,
        format_func=lambda x: metric_options[x],
        key=f"top5_metrics_{key_suffix}"
    )
    if not selected:
        st.info("Select at least one metric to visualize.")
        return
    colors = ['#1e88e5','#43a047','#e53935']
    names  = [n[:30]+'...' if len(n)>30 else n for n in top5[name_col]]
    fig    = go.Figure()
    for idx, metric in enumerate(selected):
        fig.add_trace(go.Bar(
            name=metric_options[metric], x=names, y=top5[metric],
            marker_color=colors[idx], text=top5[metric].round(2), textposition='auto'
        ))
    fig.update_layout(
        barmode='group', title="Top 5 by Revenue",
        height=430, plot_bgcolor='white', paper_bgcolor='white', hovermode='x unified',
        legend=dict(orientation="h",yanchor="bottom",y=-0.3,xanchor="center",x=0.5)
    )
    st.plotly_chart(fig, use_container_width=True)

def render_campaign_table(df, platform='Google'):
    """
    Render the styled campaign performance table.
    Works for both Google and Meta (Meta lacks 'budget' column).
    """
    df = calculate_share_metrics(df)

    if platform == 'Google':
        display_cols = ['campaign_name','budget','cost','soc','conversions_value','sor','soc_sor_ratio',
                        'conv_value_cost','cpc','ctr','clicks','impressions','conversions','cost_per_conv','aov']
        # Last 3-day metrics already merged upstream if available
        for col in ['cost_last3','budget_spent_3d_pct','spend_delta_3d','revenue_delta_3d','delta_ratio_3d']:
            if col in df.columns:
                display_cols.insert(display_cols.index('conv_value_cost'), col)
    else:  # Meta — no budget, add reach & frequency
        display_cols = ['campaign_name','cost','soc','conversions_value','sor','soc_sor_ratio',
                        'conv_value_cost','cpc','ctr','clicks','impressions','conversions','cost_per_conv','aov']
        if 'reach'     in df.columns: display_cols.append('reach')
        if 'frequency' in df.columns: display_cols.append('frequency')

    # Add change cols if present
    for m in ['cost','cpc','conv_value_cost','ctr','clicks','impressions',
              'conversions','conversions_value','cost_per_conv','aov']:
        if f'{m}_change' in df.columns and f'{m}_change' not in display_cols:
            display_cols.append(f'{m}_change')

    display_cols = [c for c in display_cols if c in df.columns]
    tbl = df[display_cols].copy()

    rename_map = {
        'campaign_name':'Campaign','budget':'Daily Budget','conv_value_cost':'ROAS',
        'conversions_value':'Revenue','cost_per_conv':'Cost/Conv',
        'soc':'SoC %','sor':'SoR %','soc_sor_ratio':'SoC/SoR',
        'cost_last3':'Last 3d Spend','budget_spent_3d_pct':'Budget % (3d)',
        'spend_delta_3d':'Δ Spend %','revenue_delta_3d':'Δ Revenue %','delta_ratio_3d':'Δ Ratio',
        'reach':'Reach','frequency':'Frequency',
    }
    tbl = tbl.rename(columns=rename_map)
    tbl.columns = [c.replace('_',' ').title() if c not in rename_map.values() else c for c in tbl.columns]

    def color_ratio(val):
        try:
            v = float(val)
            if v < 1.0: return 'background-color:#d1fae5;color:#065f46'
            if v > 1.0: return 'background-color:#fee2e2;color:#991b1b'
            return 'background-color:#f3f4f6;color:#6b7280'
        except: return ''

    styled = tbl.style.applymap(
        color_ratio, subset=['SoC/SoR'] if 'SoC/SoR' in tbl.columns else []
    ).set_table_styles([
        {'selector':'thead th','props':[('background-color','#1f2937'),('color','white'),
          ('font-weight','bold'),('font-size','14px'),('text-align','center'),
          ('padding','12px'),('border','1px solid #374151')]},
        {'selector':'tbody td','props':[('padding','10px'),('border','1px solid #e5e7eb'),('text-align','right')]},
        {'selector':'tbody tr:hover','props':[('background-color','#f3f4f6')]}
    ]).format({col:'{:.2f}' for col in tbl.select_dtypes(include=['float64']).columns})

    st.dataframe(styled, use_container_width=True, height=600)

# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def main():
    st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap:24px !important; padding:20px 0 !important; margin-bottom:30px !important; }
    .stTabs [data-baseweb="tab"]      { padding:20px 40px !important; font-size:20px !important; font-weight:600 !important; border-radius:10px !important; }
    .stTabs [aria-selected="true"]    { background-color:#eff6ff !important; color:#1e40af !important; font-size:22px !important; font-weight:700 !important; }
    </style>
    """, unsafe_allow_html=True)

    any_platform = (
        st.session_state.google_connected  or st.session_state.google_csv_uploaded  or
        st.session_state.meta_connected    or st.session_state.meta_csv_uploaded    or
        st.session_state.tiktok_connected  or st.session_state.tiktok_csv_uploaded  or
        st.session_state.shopify_connected or st.session_state.shopify_csv_uploaded
    )

    tab_list = ["🏠 Welcome & Setup"]
    if any_platform:
        tab_list += [
            "📊 Aggregate Overview",
            "📈 Campaign Breakdown",
            "🛍️ Product Breakdown",
            "📜 Change History",
            "🟢 Shopify Analytics",
            "🎯 Marketing Mix Modeling",
        ]

    tabs = st.tabs(tab_list)

    # ══════════════════════════════════════════
    # TAB 0 — WELCOME & SETUP
    # ══════════════════════════════════════════
    with tabs[0]:
        st.markdown('<p class="main-header">Multi-Platform Marketing Analytics Dashboard</p>', unsafe_allow_html=True)
        st.markdown("""
        ### Welcome! 🎉
        Connect your marketing platforms to get comprehensive analytics across **Google Ads**, **Meta Ads**, **TikTok**, and **Shopify**.
        - 🔗 **API Integration** (recommended): Real-time data, automatic updates
        - 📁 **CSV Upload**: Manual data upload, works offline
        """)
        st.markdown("---")

        # ── Connection status cards ──
        st.markdown("### 📊 Connection Status")
        c1, c2, c3, c4 = st.columns(4)

        def _status_card(col, emoji, label, connected, method):
            bg = '#d1fae5' if connected else '#f3f4f6'
            status = "✅ Connected" if connected else "⚪ Not Connected"
            col.markdown(f"""
            <div style="padding:15px;border-radius:10px;background:{bg};">
                <div style="font-size:24px;margin-bottom:5px;">{emoji}</div>
                <div style="font-weight:bold;">{label}</div>
                <div style="font-size:12px;color:#6b7280;">{status}</div>
                <div style="font-size:11px;color:#9ca3af;">{method}</div>
            </div>""", unsafe_allow_html=True)

        google_ok  = st.session_state.google_connected  or st.session_state.google_csv_uploaded
        meta_ok    = st.session_state.meta_connected    or st.session_state.meta_csv_uploaded
        tiktok_ok  = st.session_state.tiktok_connected  or st.session_state.tiktok_csv_uploaded
        shopify_ok = st.session_state.shopify_connected or st.session_state.shopify_csv_uploaded

        _status_card(c1, "🔵", "Google Ads",
                     google_ok,
                     "API" if st.session_state.google_connected else ("CSV" if st.session_state.google_csv_uploaded else "—"))
        _status_card(c2, "🔵", "Meta Ads",
                     meta_ok,
                     "API" if st.session_state.meta_connected else ("CSV" if st.session_state.meta_csv_uploaded else "—"))
        _status_card(c3, "⚫", "TikTok Ads",
                     tiktok_ok,
                     "API" if st.session_state.tiktok_connected else ("CSV" if st.session_state.tiktok_csv_uploaded else "—"))
        _status_card(c4, "🟢", "Shopify",
                     shopify_ok,
                     "API" if st.session_state.shopify_connected else ("CSV" if st.session_state.shopify_csv_uploaded else "—"))

        st.markdown("---")

        # Show Meta account info banner if connected
        if st.session_state.meta_connected and st.session_state.meta_account_info:
            info = st.session_state.meta_account_info
            st.success(
                f"✅ **Meta Ads connected** — Account: **{info.get('name','—')}** | "
                f"ID: `{info.get('id','—')}` | Currency: **{info.get('currency','—')}** | "
                f"Status: **{info.get('account_status_label','—')}** | "
                f"Timezone: {info.get('timezone_name','—')}"
            )

        # ── Platform selector ──
        setup_option = st.radio(
            "Choose platform to configure:",
            ["🔵 Google Ads", "🔵 Meta (Facebook) Ads", "⚫ TikTok Ads", "🟢 Shopify"],
            horizontal=True
        )

        # ──────────── GOOGLE SETUP ────────────
        if "Google" in setup_option:
            st.markdown("### 🔵 Google Ads Setup")
            google_method = st.radio("Connection method:", ["API Integration", "CSV Upload"], key="google_method")

            if google_method == "API Integration":
                with st.expander("📚 How to get credentials", expanded=False):
                    st.markdown("""
                    1. **Developer Token** → [Google Ads API Center](https://ads.google.com/aw/apicenter)
                    2. **OAuth (Client ID / Secret)** → [Google Cloud Console](https://console.cloud.google.com/) → Create OAuth Desktop app
                    3. **Refresh Token** → [OAuth Playground](https://developers.google.com/oauthplayground/)
                    4. **Customer ID** → Top-right in Google Ads (remove hyphens)
                    """)

                with st.form("google_api_form"):
                    dev_token = st.text_input("Developer Token", type="password")
                    col1, col2 = st.columns(2)
                    client_id     = col1.text_input("Client ID")
                    client_secret = col2.text_input("Client Secret", type="password")
                    refresh_token = st.text_input("Refresh Token", type="password")
                    col1, col2 = st.columns(2)
                    customer_id       = col1.text_input("Customer ID (no hyphens)")
                    login_customer_id = col2.text_input("Login Customer ID (optional MCC)")
                    submitted = st.form_submit_button("🚀 Connect Google Ads", type="primary")
                    if submitted:
                        if not all([dev_token, client_id, client_secret, refresh_token, customer_id]):
                            st.error("Please fill in all required fields.")
                        else:
                            with st.spinner("Connecting…"):
                                client = create_google_ads_client(
                                    dev_token, client_id, client_secret, refresh_token,
                                    login_customer_id or None
                                )
                                if client:
                                    st.session_state.client           = client
                                    st.session_state.customer_id      = customer_id
                                    st.session_state.google_connected = True
                                    st.session_state.authenticated    = True
                                    st.success("✅ Google Ads connected!")
                                    st.rerun()

                if st.session_state.google_connected:
                    st.success(f"✅ Connected (Customer ID: {st.session_state.customer_id})")
                    if st.button("🔓 Disconnect Google Ads"):
                        st.session_state.google_connected = False
                        st.session_state.authenticated    = False
                        st.session_state.client           = None
                        st.session_state.customer_id      = None
                        st.rerun()

            else:  # CSV
                st.info("Required columns: `date` (YYYY-MM-DD), `cost`. Optional: `clicks`, `impressions`, `conversions`, `revenue`.")
                f = st.file_uploader("Upload Google Ads CSV", type=['csv'], key="google_csv_uploader")
                if f:
                    try:
                        df = pd.read_csv(f)
                        df.columns = [c.lower().strip() for c in df.columns]
                        if 'date' not in df.columns or 'cost' not in df.columns:
                            st.error("CSV must contain 'date' and 'cost' columns.")
                        else:
                            df['date'] = pd.to_datetime(df['date'])
                            df['platform'] = 'Google Ads'
                            st.session_state.aggregate_data    = df
                            st.session_state.google_csv_uploaded = True
                            st.session_state.authenticated     = True
                            st.success(f"✅ Uploaded! ({len(df)} rows)")
                            st.dataframe(df.head(10))
                    except Exception as e:
                        st.error(f"Error reading CSV: {e}")

        # ──────────── META SETUP ────────────
        elif "Meta" in setup_option:
            st.markdown("### 🔵 Meta (Facebook) Ads Setup")
            meta_method = st.radio("Connection method:", ["API Integration", "CSV Upload"], key="meta_method")

            if meta_method == "API Integration":
                with st.expander("📚 How to get Meta API credentials", expanded=False):
                    st.markdown("""
                    **What you need (all 4 fields are recommended):**
                    | Field | Where to find it |
                    |---|---|
                    | **Access Token** | Meta Business Suite → Settings → System Users → Generate Token (scopes: `ads_read`, `ads_management`) |
                    | **App ID** | [developers.facebook.com](https://developers.facebook.com/) → Your App → Basic Settings |
                    | **App Secret** | Same page — click "Show" next to App Secret |
                    | **Ad Account ID** | Ads Manager URL or Business Settings → Ad Accounts (format: `act_XXXXXXXX`) |

                    💡 **Tip:** Use a **long-lived system user token** (never expires) rather than a short-lived user token.
                    Long-lived tokens can be generated via the Graph API Explorer or by calling:
                    ```
                    GET /oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={short_token}
                    ```

                    ⚠️ **Conversions tracked:** The dashboard looks for `purchase` / `omni_purchase` events.
                    Make sure your Meta pixel is firing purchase events on your store's order confirmation page.
                    """)

                with st.form("meta_api_form"):
                    st.subheader("Meta Ads API Credentials")
                    meta_access_token = st.text_input(
                        "Access Token *",
                        type="password",
                        help="Long-lived user or system user access token"
                    )
                    col1, col2 = st.columns(2)
                    meta_app_id     = col1.text_input("App ID",     help="Found in your Meta App settings")
                    meta_app_secret = col2.text_input("App Secret", type="password", help="Found in Meta App settings (used for appsecret_proof security)")
                    meta_account_id = st.text_input(
                        "Ad Account ID *",
                        placeholder="act_1234567890",
                        help="Your Meta Ad Account ID (add act_ prefix if not present)"
                    )
                    submitted = st.form_submit_button("🚀 Connect Meta Ads", type="primary")

                    if submitted:
                        if not meta_access_token or not meta_account_id:
                            st.error("Access Token and Ad Account ID are required.")
                        else:
                            with st.spinner("Validating Meta credentials…"):
                                ok, err, info = validate_meta_connection(
                                    meta_access_token,
                                    meta_account_id,
                                    meta_app_id or None,
                                    meta_app_secret or None
                                )
                            if ok:
                                st.session_state.meta_access_token  = meta_access_token
                                st.session_state.meta_ad_account_id = info['ad_account_id']
                                st.session_state.meta_app_id        = meta_app_id or None
                                st.session_state.meta_app_secret    = meta_app_secret or None
                                st.session_state.meta_account_info  = info
                                st.session_state.meta_connected     = True
                                st.success(f"✅ Connected to Meta Ads — **{info.get('name','—')}** "
                                           f"({info.get('currency','')}, {info.get('account_status_label','')})")
                                st.rerun()
                            else:
                                st.error(f"❌ Connection failed: {err}")
                                st.info("**Common fixes:**\n"
                                        "- Make sure the access token has `ads_read` permission\n"
                                        "- Ad Account ID should start with `act_`\n"
                                        "- Check that the token hasn't expired\n"
                                        "- For short-lived tokens: exchange for a long-lived token")

                if st.session_state.meta_connected:
                    info = st.session_state.meta_account_info or {}
                    st.success(f"✅ Meta Ads connected — {info.get('name','—')} ({info.get('id','—')})")
                    if st.button("🔓 Disconnect Meta Ads"):
                        for k in ['meta_connected','meta_access_token','meta_ad_account_id',
                                  'meta_app_id','meta_app_secret','meta_account_info',
                                  'meta_campaign_data','meta_daily_data']:
                            st.session_state[k] = None if 'data' in k or 'info' in k or 'token' in k or 'id' in k or 'secret' in k else False
                        st.rerun()

            else:  # CSV
                st.info("Required columns: `date` (YYYY-MM-DD), `cost`. Optional: `clicks`, `impressions`, `conversions`, `revenue`.")
                f = st.file_uploader("Upload Meta Ads CSV", type=['csv'], key="meta_csv_uploader")
                if f:
                    try:
                        df = pd.read_csv(f)
                        df.columns = [c.lower().strip() for c in df.columns]
                        if 'date' not in df.columns or 'cost' not in df.columns:
                            st.error("CSV must contain 'date' and 'cost' columns.")
                        else:
                            df['date'] = pd.to_datetime(df['date'])
                            df['platform'] = 'Meta'
                            st.session_state.meta_data        = df
                            st.session_state.meta_csv_uploaded = True
                            st.success(f"✅ Uploaded! ({len(df)} rows)")
                            st.dataframe(df.head(10))
                    except Exception as e:
                        st.error(f"Error reading CSV: {e}")

        # ──────────── TIKTOK SETUP ────────────
        elif "TikTok" in setup_option:
            st.markdown("### ⚫ TikTok Ads Setup")
            tiktok_method = st.radio("Connection method:", ["API Integration", "CSV Upload"], key="tiktok_method")
            if tiktok_method == "API Integration":
                with st.form("tiktok_api_form"):
                    st.subheader("TikTok Ads API Credentials")
                    st.text_input("Access Token", type="password", key="tiktok_access_token")
                    st.text_input("Advertiser ID", key="tiktok_advertiser_id")
                    if st.form_submit_button("🚀 Connect TikTok Ads", type="primary"):
                        st.warning("⚠️ TikTok API coming in Phase 3. Please use CSV upload for now.")
            else:
                f = st.file_uploader("Upload TikTok Ads CSV", type=['csv'], key="tiktok_csv_uploader")
                if f:
                    try:
                        df = pd.read_csv(f)
                        df.columns = [c.lower().strip() for c in df.columns]
                        if 'date' not in df.columns or 'cost' not in df.columns:
                            st.error("CSV must contain 'date' and 'cost' columns.")
                        else:
                            df['date'] = pd.to_datetime(df['date'])
                            df['platform'] = 'TikTok'
                            st.session_state.tiktok_data        = df
                            st.session_state.tiktok_csv_uploaded = True
                            st.success(f"✅ Uploaded! ({len(df)} rows)")
                    except Exception as e:
                        st.error(f"Error: {e}")

        # ──────────── SHOPIFY SETUP ────────────
        elif "Shopify" in setup_option:
            st.markdown("### 🟢 Shopify Setup")
            shopify_method = st.radio("Connection method:", ["API Integration", "CSV Upload"], key="shopify_method")
            if shopify_method == "API Integration":
                with st.form("shopify_api_form"):
                    st.subheader("Shopify API Credentials")
                    st.text_input("Store URL", placeholder="mystore.myshopify.com", key="shopify_store_url")
                    st.text_input("Admin API Access Token", type="password", key="shopify_access_token")
                    if st.form_submit_button("🚀 Connect Shopify", type="primary"):
                        st.warning("⚠️ Shopify API coming in Phase 3. Please use CSV upload for now.")
            else:
                f = st.file_uploader("Upload Shopify CSV", type=['csv'], key="shopify_csv_uploader")
                if f:
                    try:
                        df = pd.read_csv(f)
                        df.columns = [c.lower().strip() for c in df.columns]
                        has_rev = 'revenue' in df.columns or 'sales' in df.columns
                        if 'date' not in df.columns or not has_rev or 'orders' not in df.columns:
                            st.error("CSV must contain 'date', 'revenue' (or 'sales'), and 'orders' columns.")
                        else:
                            df['date'] = pd.to_datetime(df['date'])
                            if 'sales' in df.columns and 'revenue' not in df.columns:
                                df['revenue'] = df['sales']
                            st.session_state.shopify_data        = df
                            st.session_state.shopify_csv_uploaded = True
                            st.success(f"✅ Uploaded! ({len(df)} rows)")
                    except Exception as e:
                        st.error(f"Error: {e}")

        # Validation footer
        st.markdown("---")
        any_ads = (google_ok or meta_ok or tiktok_ok)
        if not any_ads:
            st.warning("⚠️ At least one advertising platform must be connected to use the dashboard.")
        else:
            st.success("✅ Dashboard ready! Navigate to the other tabs to view your data.")

    # ══════════════════════════════════════════
    # REMAINING TABS (only shown when connected)
    # ══════════════════════════════════════════
    if not any_platform:
        return

    # ══════════════════════════════════════════
    # TAB 1 — AGGREGATE OVERVIEW
    # ══════════════════════════════════════════
    with tabs[1]:
        st.header("📊 Aggregate Overview")

        google_avail = st.session_state.google_connected or st.session_state.google_csv_uploaded
        meta_avail   = st.session_state.meta_connected   or st.session_state.meta_csv_uploaded

        if not google_avail and not meta_avail:
            st.warning("Connect Google Ads or Meta Ads to view aggregate data.")
            st.stop()

        # Platform selector
        platform_options = []
        if google_avail: platform_options.append("Google Ads")
        if meta_avail:   platform_options.append("Meta Ads")
        if google_avail and meta_avail: platform_options.append("All Platforms")

        selected_platform_agg = st.radio("Select platform:", platform_options, horizontal=True, key="agg_platform")

        # ── Cross-platform combined view ──
        if selected_platform_agg == "All Platforms":
            st.subheader("🌐 Cross-Platform Summary")
            st.info("Load data from each platform individually (via their respective Campaign Breakdown tabs) to compare here. "
                    "This section shows the most recently loaded data from each platform.")

            g_data = st.session_state.campaign_data
            m_data = st.session_state.meta_campaign_data

            if g_data is None and m_data is None:
                st.warning("No data loaded yet. Go to **Campaign Breakdown → Google Ads** and **Campaign Breakdown → Meta Ads** "
                           "to load data first.")
            else:
                rows = []
                if g_data is not None and not g_data.empty:
                    rows.append({'Platform':'Google Ads',
                                 'Spend': g_data['cost'].sum(),
                                 'Clicks': g_data['clicks'].sum(),
                                 'Impressions': g_data['impressions'].sum(),
                                 'Conversions': g_data['conversions'].sum(),
                                 'Revenue': g_data['conversions_value'].sum()})
                if m_data is not None and not m_data.empty:
                    rows.append({'Platform':'Meta Ads',
                                 'Spend': m_data['cost'].sum(),
                                 'Clicks': m_data['clicks'].sum(),
                                 'Impressions': m_data['impressions'].sum(),
                                 'Conversions': m_data['conversions'].sum(),
                                 'Revenue': m_data['conversions_value'].sum()})

                if rows:
                    summary_df = pd.DataFrame(rows)
                    summary_df['ROAS']   = summary_df.apply(lambda x: x['Revenue']/x['Spend'] if x['Spend']>0 else 0, axis=1)
                    summary_df['CPC']    = summary_df.apply(lambda x: x['Spend']/x['Clicks'] if x['Clicks']>0 else 0, axis=1)
                    summary_df['CTR %']  = summary_df.apply(lambda x: x['Clicks']/x['Impressions']*100 if x['Impressions']>0 else 0, axis=1)

                    # KPI tiles
                    total_spend   = summary_df['Spend'].sum()
                    total_revenue = summary_df['Revenue'].sum()
                    total_roas    = total_revenue / total_spend if total_spend > 0 else 0
                    k1, k2, k3 = st.columns(3)
                    k1.metric("Total Spend (All Platforms)", f"${total_spend:,.2f}")
                    k2.metric("Total Revenue (All Platforms)", f"${total_revenue:,.2f}")
                    k3.metric("Blended ROAS", f"{total_roas:.2f}x")

                    st.markdown("### Platform Comparison")

                    # Bar chart
                    metric_to_plot = st.selectbox("Metric to compare:", ['Spend','Revenue','Conversions','Clicks','ROAS','CTR %'], key="xplat_metric")
                    colors_map = {'Google Ads':'#4285f4', 'Meta Ads':'#1877f2'}
                    fig = go.Figure(go.Bar(
                        x=summary_df['Platform'],
                        y=summary_df[metric_to_plot],
                        marker_color=[colors_map.get(p,'#888') for p in summary_df['Platform']],
                        text=summary_df[metric_to_plot].round(2),
                        textposition='auto'
                    ))
                    fig.update_layout(
                        title=f"{metric_to_plot} by Platform",
                        height=380, plot_bgcolor='white', paper_bgcolor='white'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Summary table
                    st.dataframe(
                        summary_df.style.format({
                            'Spend':'${:,.2f}','Revenue':'${:,.2f}',
                            'Clicks':'{:,.0f}','Impressions':'{:,.0f}','Conversions':'{:,.0f}',
                            'ROAS':'{:.2f}x','CPC':'${:.2f}','CTR %':'{:.2f}%'
                        }),
                        use_container_width=True
                    )
            return  # Don't show per-platform section for "All Platforms"

        # ── Google Aggregate ──
        if selected_platform_agg == "Google Ads":
            if not google_avail:
                st.warning("Connect Google Ads first.")
                st.stop()

            col1, col2, col3 = st.columns([2,2,1])
            start_date   = col1.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="agg_start")
            end_date     = col2.date_input("End Date", value=datetime.now(), key="agg_end")
            compare_opt  = col3.selectbox("Compare to", ["None","Previous Period","Previous Week","Previous Month","Previous Year","Custom"], key="agg_compare")

            if compare_opt == "Custom":
                c1, c2 = st.columns(2)
                compare_start = c1.date_input("Compare Start", key="agg_comp_start")
                compare_end   = c2.date_input("Compare End",   key="agg_comp_end")

            st.markdown("---")
            c1, c2 = st.columns([4,1])
            camp_filter  = c1.text_input("Filter by Campaign Name", placeholder="Type campaign name…", key="agg_camp_filter")
            exact_match  = c2.checkbox("Exact", key="agg_exact")

            if st.button("📥 Load Google Data", key="load_agg_data", type="primary"):
                with st.spinner("Fetching from Google Ads…"):
                    camp_df  = fetch_campaign_performance(st.session_state.client, st.session_state.customer_id, start_date, end_date)
                    daily_df = fetch_daily_performance(st.session_state.client, st.session_state.customer_id, start_date, end_date)
                    if not camp_df.empty:
                        camp_df = process_dataframe(camp_df)
                        st.session_state.campaign_data = camp_df
                        st.session_state.daily_data    = daily_df
                        comp_df = pd.DataFrame()
                        if compare_opt != "None":
                            days_diff = (end_date - start_date).days
                            if compare_opt == "Previous Period":  comp_start_d = start_date-timedelta(days=days_diff+1); comp_end_d = start_date-timedelta(days=1)
                            elif compare_opt == "Previous Week":  comp_start_d = start_date-timedelta(days=7); comp_end_d = start_date-timedelta(days=1)
                            elif compare_opt == "Previous Month": comp_start_d = start_date-timedelta(days=30); comp_end_d = start_date-timedelta(days=1)
                            elif compare_opt == "Previous Year":  comp_start_d = start_date-timedelta(days=365); comp_end_d = end_date-timedelta(days=365)
                            else:                                  comp_start_d = compare_start; comp_end_d = compare_end
                            comp_df = fetch_campaign_performance(st.session_state.client, st.session_state.customer_id, comp_start_d, comp_end_d)
                            if not comp_df.empty: comp_df = process_dataframe(comp_df)
                        st.session_state.aggregate_data = {'current':camp_df,'comparison':comp_df,'compare_option':compare_opt}
                        st.session_state.data_loaded = True
                        st.success("✅ Data loaded!")
                    else:
                        st.warning("No data found.")

            if st.session_state.data_loaded and st.session_state.aggregate_data and isinstance(st.session_state.aggregate_data, dict):
                cur_df  = st.session_state.aggregate_data['current'].copy()
                comp_df = st.session_state.aggregate_data['comparison']
                if camp_filter:
                    cur_df  = cur_df[cur_df['campaign_name'] == camp_filter] if exact_match else cur_df[cur_df['campaign_name'].str.contains(camp_filter, case=False, na=False)]
                    if not comp_df.empty:
                        comp_df = comp_df[comp_df['campaign_name'] == camp_filter] if exact_match else comp_df[comp_df['campaign_name'].str.contains(camp_filter, case=False, na=False)]
                if cur_df.empty:
                    st.warning("No matching campaigns.")
                    st.stop()

                if not comp_df.empty:
                    cur_t, comp_t, chg = calculate_comparison(cur_df, comp_df)
                else:
                    cur_t = {'cost':cur_df['cost'].sum(),'clicks':cur_df['clicks'].sum(),
                             'impressions':cur_df['impressions'].sum(),'conversions':cur_df['conversions'].sum(),
                             'conversions_value':cur_df['conversions_value'].sum()}
                    cur_t['cpc']            = cur_t['cost']/cur_t['clicks'] if cur_t['clicks']>0 else 0
                    cur_t['ctr']            = cur_t['clicks']/cur_t['impressions']*100 if cur_t['impressions']>0 else 0
                    cur_t['cost_per_conv']  = cur_t['cost']/cur_t['conversions'] if cur_t['conversions']>0 else 0
                    cur_t['conv_value_cost']= cur_t['conversions_value']/cur_t['cost'] if cur_t['cost']>0 else 0
                    cur_t['aov']            = cur_t['conversions_value']/cur_t['conversions'] if cur_t['conversions']>0 else 0
                    chg = {f'{m}_change':0 for m in cur_t}

                st.subheader("Key Performance Metrics — Google Ads")
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("Cost", cur_t['cost'], chg['cost_change'], 'currency', inverse=True), unsafe_allow_html=True)
                c2.markdown(display_metric_card("CPC",  cur_t['cpc'],  chg['cpc_change'],  'currency', inverse=True), unsafe_allow_html=True)
                c3.markdown(display_metric_card("ROAS", cur_t['conv_value_cost'], chg['conv_value_cost_change'], 'number'), unsafe_allow_html=True)
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("CTR",         cur_t['ctr'],             chg['ctr_change'],             'percentage'), unsafe_allow_html=True)
                c2.markdown(display_metric_card("Clicks",      cur_t['clicks'],           chg['clicks_change'],           'number'), unsafe_allow_html=True)
                c3.markdown(display_metric_card("Impressions",  cur_t['impressions'],      chg['impressions_change'],      'number'), unsafe_allow_html=True)
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("Conv Value",   cur_t['conversions_value'], chg['conversions_value_change'], 'currency'), unsafe_allow_html=True)
                c2.markdown(display_metric_card("Cost/Conv",    cur_t['cost_per_conv'],    chg['cost_per_conv_change'],    'currency', inverse=True), unsafe_allow_html=True)
                c3.markdown(display_metric_card("AOV",          cur_t['aov'],              chg['aov_change'],              'currency'), unsafe_allow_html=True)

                if st.session_state.daily_data is not None and not st.session_state.daily_data.empty:
                    st.markdown("---")
                    st.subheader("📈 Performance Over Time")
                    daily_data = st.session_state.daily_data.copy()
                    if camp_filter:
                        daily_data = daily_data[daily_data['campaign_name']==camp_filter] if exact_match else daily_data[daily_data['campaign_name'].str.contains(camp_filter, case=False, na=False)]
                    metric_opts = {'cost':'Cost','clicks':'Clicks','impressions':'Impressions',
                                   'conversions':'Conversions','conversions_value':'Conversion Value',
                                   'ctr':'CTR (%)','cpc':'CPC','conv_value_cost':'ROAS',
                                   'cost_per_conv':'Cost per Conv','aov':'AOV'}
                    sel_m = st.selectbox("Metric:", list(metric_opts.keys()), format_func=lambda x: metric_opts[x], key="agg_metric")
                    st.plotly_chart(create_time_series_chart(daily_data, sel_m, metric_opts[sel_m]), use_container_width=True)

        # ── Meta Aggregate ──
        elif selected_platform_agg == "Meta Ads":
            if not meta_avail:
                st.warning("Connect Meta Ads first.")
                st.stop()

            col1, col2 = st.columns(2)
            start_date_m = col1.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="agg_meta_start")
            end_date_m   = col2.date_input("End Date",   value=datetime.now(),                   key="agg_meta_end")

            st.markdown("---")
            c1, c2 = st.columns([4,1])
            meta_camp_filter = c1.text_input("Filter by Campaign Name", placeholder="Type campaign name…", key="agg_meta_camp_filter")
            meta_exact       = c2.checkbox("Exact", key="agg_meta_exact")

            if st.button("📥 Load Meta Data", key="load_agg_meta", type="primary"):
                if st.session_state.meta_connected:
                    with st.spinner("Fetching from Meta Ads API…"):
                        m_camp  = fetch_meta_campaign_performance(
                            st.session_state.meta_access_token,
                            st.session_state.meta_ad_account_id,
                            start_date_m, end_date_m,
                            st.session_state.meta_app_secret
                        )
                        m_daily = fetch_meta_daily_performance(
                            st.session_state.meta_access_token,
                            st.session_state.meta_ad_account_id,
                            start_date_m, end_date_m,
                            st.session_state.meta_app_secret
                        )
                        if not m_camp.empty:
                            st.session_state.meta_campaign_data = m_camp
                            st.session_state.meta_daily_data    = m_daily
                            st.success(f"✅ Loaded {len(m_camp)} Meta campaigns!")
                        else:
                            st.warning("No Meta campaign data found for this date range.")
                elif st.session_state.meta_csv_uploaded and st.session_state.meta_data is not None:
                    st.session_state.meta_campaign_data = st.session_state.meta_data
                    st.success("✅ Using uploaded Meta CSV data.")
                else:
                    st.warning("Please connect Meta Ads first.")

            if st.session_state.meta_campaign_data is not None and not st.session_state.meta_campaign_data.empty:
                m_df = st.session_state.meta_campaign_data.copy()
                if meta_camp_filter:
                    m_df = m_df[m_df['campaign_name']==meta_camp_filter] if meta_exact else m_df[m_df['campaign_name'].str.contains(meta_camp_filter, case=False, na=False)]
                if m_df.empty:
                    st.warning("No matching campaigns.")
                    st.stop()

                total_spend = m_df['cost'].sum()
                total_rev   = m_df['conversions_value'].sum()
                total_roas  = total_rev/total_spend if total_spend>0 else 0
                total_clicks= m_df['clicks'].sum()
                total_impr  = m_df['impressions'].sum()
                total_conv  = m_df['conversions'].sum()
                total_cpc   = total_spend/total_clicks if total_clicks>0 else 0
                total_ctr   = total_clicks/total_impr*100 if total_impr>0 else 0

                st.subheader("Key Performance Metrics — Meta Ads")
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("Spend",       total_spend,  None, 'currency'), unsafe_allow_html=True)
                c2.markdown(display_metric_card("CPC",         total_cpc,    None, 'currency'), unsafe_allow_html=True)
                c3.markdown(display_metric_card("ROAS",        total_roas,   None, 'number'),   unsafe_allow_html=True)
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("CTR",         total_ctr,    None, 'percentage'), unsafe_allow_html=True)
                c2.markdown(display_metric_card("Clicks",      total_clicks, None, 'number'),   unsafe_allow_html=True)
                c3.markdown(display_metric_card("Impressions", total_impr,   None, 'number'),   unsafe_allow_html=True)
                c1,c2,c3 = st.columns(3)
                c1.markdown(display_metric_card("Revenue",     total_rev,    None, 'currency'), unsafe_allow_html=True)
                c2.markdown(display_metric_card("Conversions", total_conv,   None, 'number'),   unsafe_allow_html=True)

                if 'reach' in m_df.columns:
                    total_reach = m_df['reach'].sum()
                    c3.markdown(display_metric_card("Total Reach", total_reach, None, 'number'), unsafe_allow_html=True)

                # Time-series
                if st.session_state.meta_daily_data is not None and not st.session_state.meta_daily_data.empty:
                    st.markdown("---")
                    st.subheader("📈 Meta Performance Over Time")
                    daily_m = st.session_state.meta_daily_data.copy()
                    if meta_camp_filter:
                        daily_m = daily_m[daily_m['campaign_name']==meta_camp_filter] if meta_exact else daily_m[daily_m['campaign_name'].str.contains(meta_camp_filter, case=False, na=False)]
                    metric_opts_m = {'cost':'Spend','clicks':'Clicks','impressions':'Impressions',
                                     'conversions':'Conversions','conversions_value':'Revenue',
                                     'ctr':'CTR (%)','cpc':'CPC','conv_value_cost':'ROAS','aov':'AOV'}
                    sel_m = st.selectbox("Metric:", list(metric_opts_m.keys()), format_func=lambda x: metric_opts_m[x], key="agg_meta_metric")
                    st.plotly_chart(create_time_series_chart(daily_m, sel_m, metric_opts_m[sel_m]), use_container_width=True)

    # ══════════════════════════════════════════
    # TAB 2 — CAMPAIGN BREAKDOWN
    # ══════════════════════════════════════════
    with tabs[2]:
        st.header("📈 Campaign Breakdown")

        google_avail = st.session_state.google_connected or st.session_state.google_csv_uploaded
        meta_avail   = st.session_state.meta_connected   or st.session_state.meta_csv_uploaded

        if not google_avail and not meta_avail:
            st.warning("Connect Google Ads or Meta Ads to view campaign data.")
            st.stop()

        plat_options_camp = []
        if google_avail: plat_options_camp.append("Google Ads")
        if meta_avail:   plat_options_camp.append("Meta Ads")

        selected_platform_camp = st.radio("Platform:", plat_options_camp, horizontal=True, key="camp_platform")

        # ── GOOGLE CAMPAIGNS ──
        if selected_platform_camp == "Google Ads":
            if not google_avail:
                st.warning("Connect Google Ads first.")
                st.stop()

            col1,col2,col3 = st.columns([2,2,1])
            start_date_camp  = col1.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="camp_start")
            end_date_camp    = col2.date_input("End Date",   value=datetime.now(),                    key="camp_end")
            compare_opt_camp = col3.selectbox("Compare to", ["None","Previous Period","Previous Week","Previous Month","Previous Year","Custom"], key="camp_compare")

            if compare_opt_camp == "Custom":
                c1,c2 = st.columns(2)
                comp_start_camp = c1.date_input("Compare Start", key="camp_comp_start")
                comp_end_camp   = c2.date_input("Compare End",   key="camp_comp_end")

            st.markdown("---")
            c1,c2 = st.columns([4,1])
            camp_filter_g = c1.text_input("Filter by Campaign Name", placeholder="Type campaign name…", key="camp_filter_g")
            exact_g       = c2.checkbox("Exact", key="camp_exact_g")

            if st.button("📥 Load Google Campaign Data", key="load_camp_g", type="primary"):
                with st.spinner("Fetching…"):
                    camp_df  = fetch_campaign_performance(st.session_state.client, st.session_state.customer_id, start_date_camp, end_date_camp)
                    daily_df = fetch_daily_performance(st.session_state.client, st.session_state.customer_id, start_date_camp, end_date_camp)
                    try:
                        chg_df = fetch_change_history(st.session_state.client, st.session_state.customer_id, start_date_camp, end_date_camp)
                        st.session_state.change_history_data = chg_df if not chg_df.empty else None
                    except:
                        st.session_state.change_history_data = None
                    if not camp_df.empty:
                        camp_df = process_dataframe(camp_df)
                        comp_df = pd.DataFrame()
                        daily_comp_df = pd.DataFrame()
                        if compare_opt_camp != "None":
                            days_diff = (end_date_camp - start_date_camp).days
                            if compare_opt_camp == "Previous Period":  cs = start_date_camp-timedelta(days=days_diff+1); ce = start_date_camp-timedelta(days=1)
                            elif compare_opt_camp == "Previous Week":  cs = start_date_camp-timedelta(days=7); ce = start_date_camp-timedelta(days=1)
                            elif compare_opt_camp == "Previous Month": cs = start_date_camp-timedelta(days=30); ce = start_date_camp-timedelta(days=1)
                            elif compare_opt_camp == "Previous Year":  cs = start_date_camp-timedelta(days=365); ce = end_date_camp-timedelta(days=365)
                            else:                                        cs = comp_start_camp; ce = comp_end_camp
                            comp_df      = fetch_campaign_performance(st.session_state.client, st.session_state.customer_id, cs, ce)
                            daily_comp_df= fetch_daily_performance(st.session_state.client, st.session_state.customer_id, cs, ce)
                            if not comp_df.empty: comp_df = process_dataframe(comp_df)
                        if not comp_df.empty:
                            merged = camp_df.merge(
                                comp_df[['campaign_name','cost','cpc','ctr','clicks','impressions',
                                         'conversions','conversions_value','cost_per_conv','conv_value_cost','aov']],
                                on='campaign_name', how='left', suffixes=('','_comp')
                            )
                            for m in ['cost','cpc','ctr','clicks','impressions','conversions',
                                      'conversions_value','cost_per_conv','conv_value_cost','aov']:
                                merged[f'{m}_change'] = merged.apply(
                                    lambda x: ((x[m]-x[f'{m}_comp'])/x[f'{m}_comp']*100) if pd.notna(x.get(f'{m}_comp')) and x.get(f'{m}_comp',0)!=0 else 0, axis=1)
                            camp_df = merged
                        st.session_state.campaign_data            = camp_df
                        st.session_state.daily_data_camp          = daily_df
                        st.session_state.daily_data_camp_comparison = daily_comp_df
                        st.success("✅ Google campaign data loaded!")
                    else:
                        st.warning("No data found.")

            if st.session_state.campaign_data is not None and not st.session_state.campaign_data.empty:
                st.markdown("---")
                st.subheader("🏆 Campaign Performance Insights — Google Ads")
                df_g = st.session_state.campaign_data.copy()
                if camp_filter_g:
                    df_g = df_g[df_g['campaign_name']==camp_filter_g] if exact_g else df_g[df_g['campaign_name'].str.contains(camp_filter_g, case=False, na=False)]
                if df_g.empty:
                    st.warning(f"No campaigns matching '{camp_filter_g}'")
                    st.stop()

                render_hero_kpi_cards(df_g, "Google Ads")
                st.markdown("### 📊 Top 5 Campaigns")
                render_top5_bar_chart(df_g, 'campaign_name',
                                      ['conversions_value','cost'],
                                      {'cost':'Cost','conversions':'Conversions','conversions_value':'Revenue',
                                       'conv_value_cost':'ROAS','clicks':'Clicks','cpc':'CPC'},
                                      key_suffix="google_camp")

                st.markdown("---")
                # Last 3 days metrics
                if st.session_state.daily_data_camp is not None:
                    budget_df = df_g[['campaign_name','budget']].copy() if 'budget' in df_g.columns else None
                    last3 = calculate_last_3_days_metrics(st.session_state.daily_data_camp, budget_df)
                    if not last3.empty:
                        df_g = df_g.merge(last3, on='campaign_name', how='left')
                        for col in ['cost_last3','spend_delta_3d','revenue_delta_3d','delta_ratio_3d','budget_spent_3d_pct']:
                            if col in df_g.columns: df_g[col] = df_g[col].fillna(0)

                render_campaign_table(df_g, platform='Google')

                # Time-series
                if st.session_state.daily_data_camp is not None and not st.session_state.daily_data_camp.empty:
                    st.markdown("---")
                    st.subheader("📈 Google Campaign Performance Over Time")
                    daily_g = st.session_state.daily_data_camp.copy()
                    if camp_filter_g:
                        daily_g = daily_g[daily_g['campaign_name']==camp_filter_g] if exact_g else daily_g[daily_g['campaign_name'].str.contains(camp_filter_g, case=False, na=False)]
                    if not daily_g.empty:
                        unique_camps = daily_g['campaign_name'].unique()
                        is_single    = len(unique_camps) == 1
                        if is_single:
                            st.info(f"📍 Single campaign: **{unique_camps[0]}**. Change history markers will appear on the chart.")
                            c1,c2 = st.columns(2)
                            min_bud = c1.slider("Min Budget Change %", 0,100,10,5, key="g_min_bud")
                            min_bid = c2.slider("Min Bid Change %",    0,100,10,5, key="g_min_bid")
                        else:
                            min_bud = min_bid = 0

                        metric_opts = {'cost':'Cost','clicks':'Clicks','impressions':'Impressions',
                                       'conversions':'Conversions','conversions_value':'Revenue',
                                       'ctr':'CTR (%)','cpc':'CPC','conv_value_cost':'ROAS',
                                       'cost_per_conv':'Cost/Conv','aov':'AOV'}
                        sel_metrics = st.multiselect("Select up to 3 metrics:", list(metric_opts.keys()),
                                                     default=['cost','conversions'], max_selections=3,
                                                     format_func=lambda x: metric_opts[x], key="g_camp_metrics")
                        if sel_metrics:
                            daily_comp = st.session_state.daily_data_camp_comparison
                            if daily_comp is not None and not daily_comp.empty:
                                show_comp = st.checkbox("Show comparison period", value=True, key="g_show_comp")
                                if camp_filter_g:
                                    daily_comp = daily_comp[daily_comp['campaign_name']==camp_filter_g] if exact_g else daily_comp[daily_comp['campaign_name'].str.contains(camp_filter_g, case=False, na=False)]
                            else:
                                show_comp  = False
                                daily_comp = None
                            fig = create_multi_metric_chart(daily_g, daily_comp if show_comp else None,
                                                            sel_metrics, metric_opts, show_comp)
                            if is_single and st.session_state.change_history_data is not None:
                                try:
                                    fig = add_change_annotations(fig, st.session_state.change_history_data,
                                                                 unique_camps[0], (start_date_camp, end_date_camp),
                                                                 min_bud, min_bid)
                                except: pass
                            st.plotly_chart(fig, use_container_width=True)

        # ── META CAMPAIGNS ──
        elif selected_platform_camp == "Meta Ads":
            if not meta_avail:
                st.warning("Connect Meta Ads first.")
                st.stop()

            col1,col2 = st.columns(2)
            start_date_meta = col1.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="meta_camp_start")
            end_date_meta   = col2.date_input("End Date",   value=datetime.now(),                    key="meta_camp_end")

            st.markdown("---")
            c1,c2 = st.columns([4,1])
            camp_filter_m = c1.text_input("Filter by Campaign Name", placeholder="Type campaign name…", key="meta_camp_filter")
            exact_m       = c2.checkbox("Exact", key="meta_camp_exact")

            if st.button("📥 Load Meta Campaign Data", key="load_meta_camp", type="primary"):
                if st.session_state.meta_connected:
                    with st.spinner("Fetching Meta campaign data…"):
                        m_camp  = fetch_meta_campaign_performance(
                            st.session_state.meta_access_token,
                            st.session_state.meta_ad_account_id,
                            start_date_meta, end_date_meta,
                            st.session_state.meta_app_secret
                        )
                        m_daily = fetch_meta_daily_performance(
                            st.session_state.meta_access_token,
                            st.session_state.meta_ad_account_id,
                            start_date_meta, end_date_meta,
                            st.session_state.meta_app_secret
                        )
                        if not m_camp.empty:
                            st.session_state.meta_campaign_data = m_camp
                            st.session_state.meta_daily_data    = m_daily
                            st.success(f"✅ Loaded {len(m_camp)} Meta campaigns!")
                        else:
                            st.warning("No data found for this date range. Check that your campaigns have spend.")
                elif st.session_state.meta_csv_uploaded and st.session_state.meta_data is not None:
                    st.session_state.meta_campaign_data = st.session_state.meta_data
                    st.success("✅ Using uploaded Meta CSV.")
                else:
                    st.warning("Please connect Meta Ads first.")

            if st.session_state.meta_campaign_data is not None and not st.session_state.meta_campaign_data.empty:
                st.markdown("---")
                df_m = st.session_state.meta_campaign_data.copy()
                if camp_filter_m:
                    df_m = df_m[df_m['campaign_name']==camp_filter_m] if exact_m else df_m[df_m['campaign_name'].str.contains(camp_filter_m, case=False, na=False)]
                if df_m.empty:
                    st.warning(f"No campaigns matching '{camp_filter_m}'")
                    st.stop()

                st.subheader("🏆 Campaign Performance Insights — Meta Ads")
                render_hero_kpi_cards(df_m, "Meta Ads")

                st.markdown("### 📊 Top 5 Meta Campaigns")
                render_top5_bar_chart(df_m, 'campaign_name',
                                      ['conversions_value','cost'],
                                      {'cost':'Spend','conversions':'Conversions','conversions_value':'Revenue',
                                       'conv_value_cost':'ROAS','clicks':'Clicks','cpc':'CPC'},
                                      key_suffix="meta_camp")

                st.markdown("---")

                # Portfolio-level 3-day summary (using daily data)
                if st.session_state.meta_daily_data is not None and not st.session_state.meta_daily_data.empty:
                    last3_m = calculate_last_3_days_metrics(st.session_state.meta_daily_data)
                    if not last3_m.empty:
                        df_m = df_m.merge(last3_m, on='campaign_name', how='left')
                        for col in ['cost_last3','spend_delta_3d','revenue_delta_3d','delta_ratio_3d']:
                            if col in df_m.columns: df_m[col] = df_m[col].fillna(0)
                        # Show 3-day summary cards
                        max_d   = st.session_state.meta_daily_data['date'].max()
                        l3_st   = max_d - timedelta(days=2)
                        p3_en   = l3_st - timedelta(days=1)
                        p3_st   = p3_en - timedelta(days=2)
                        st.info(f"📅 **Last 3 Days:** {l3_st.strftime('%b %d')}–{max_d.strftime('%b %d, %Y')} vs Previous: {p3_st.strftime('%b %d')}–{p3_en.strftime('%b %d, %Y')}")

                        avg_spend_d   = df_m['spend_delta_3d'].mean() if 'spend_delta_3d' in df_m.columns else 0
                        avg_rev_d     = df_m['revenue_delta_3d'].mean() if 'revenue_delta_3d' in df_m.columns else 0
                        avg_delta_r   = avg_rev_d/avg_spend_d if abs(avg_spend_d)>0.1 else 0

                        k1,k2,k3 = st.columns(3)
                        sa,sc = ("▲","#dc2626") if avg_spend_d>0 else ("▼","#059669")
                        ra,rc = ("▲","#059669") if avg_rev_d>0   else ("▼","#dc2626")
                        perf  = avg_delta_r>=1 or (avg_spend_d<0 and avg_rev_d>0)
                        dc    = "#059669" if perf else "#dc2626"

                        k1.markdown(f'<div style="background:white;padding:16px;border-radius:8px;border:1px solid #e5e7eb;"><div style="font-size:12px;color:#6b7280;">AVG SPEND CHANGE (3D)</div><div style="font-size:24px;font-weight:700;color:{sc};">{sa} {abs(avg_spend_d):.1f}%</div></div>', unsafe_allow_html=True)
                        k2.markdown(f'<div style="background:white;padding:16px;border-radius:8px;border:1px solid #e5e7eb;"><div style="font-size:12px;color:#6b7280;">AVG REVENUE CHANGE (3D)</div><div style="font-size:24px;font-weight:700;color:{rc};">{ra} {abs(avg_rev_d):.1f}%</div></div>', unsafe_allow_html=True)
                        k3.markdown(f'<div style="background:white;padding:16px;border-radius:8px;border:1px solid #e5e7eb;"><div style="font-size:12px;color:#6b7280;">AVG DELTA RATIO (3D)</div><div style="font-size:24px;font-weight:700;color:{dc};">{abs(avg_delta_r):.2f}x</div></div>', unsafe_allow_html=True)
                        st.markdown("---")

                render_campaign_table(df_m, platform='Meta')

                # Time-series for Meta
                if st.session_state.meta_daily_data is not None and not st.session_state.meta_daily_data.empty:
                    st.markdown("---")
                    st.subheader("📈 Meta Campaign Performance Over Time")
                    daily_m = st.session_state.meta_daily_data.copy()
                    if camp_filter_m:
                        daily_m = daily_m[daily_m['campaign_name']==camp_filter_m] if exact_m else daily_m[daily_m['campaign_name'].str.contains(camp_filter_m, case=False, na=False)]
                    if not daily_m.empty:
                        metric_opts_m = {'cost':'Spend','clicks':'Clicks','impressions':'Impressions',
                                         'conversions':'Conversions','conversions_value':'Revenue',
                                         'ctr':'CTR (%)','cpc':'CPC','conv_value_cost':'ROAS',
                                         'cost_per_conv':'Cost/Conv','aov':'AOV'}
                        sel_m = st.multiselect("Select up to 3 metrics:", list(metric_opts_m.keys()),
                                               default=['cost','conversions_value'], max_selections=3,
                                               format_func=lambda x: metric_opts_m[x], key="meta_camp_metrics")
                        if sel_m:
                            fig_m = create_multi_metric_chart(daily_m, None, sel_m, metric_opts_m, False)
                            st.plotly_chart(fig_m, use_container_width=True)
                        else:
                            st.info("Select at least one metric.")

                # Download
                csv_m = df_m.to_csv(index=False)
                st.download_button("📥 Download Meta Campaign CSV", csv_m,
                                   f"meta_campaigns_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

    # ══════════════════════════════════════════
    # TAB 3 — PRODUCT BREAKDOWN (Google only)
    # ══════════════════════════════════════════
    with tabs[3]:
        st.header("🛍️ Product Breakdown")
        if not (st.session_state.google_connected or st.session_state.google_csv_uploaded):
            st.warning("⚠️ Connect Google Ads to view product data.")
            st.info("Product-level data comes from Google Shopping campaigns.")
            st.stop()

        col1,col2,col3 = st.columns([2,2,1])
        start_date_prod  = col1.date_input("Start Date", value=datetime.now()-timedelta(days=30), key="prod_start")
        end_date_prod    = col2.date_input("End Date",   value=datetime.now(), key="prod_end")
        compare_opt_prod = col3.selectbox("Compare to", ["None","Previous Period","Previous Week","Previous Month","Previous Year","Custom"], key="prod_compare")
        if compare_opt_prod == "Custom":
            c1,c2 = st.columns(2)
            comp_start_prod = c1.date_input("Compare Start", key="prod_comp_start")
            comp_end_prod   = c2.date_input("Compare End", key="prod_comp_end")

        st.markdown("---")
        c1,c2 = st.columns([4,1])
        camp_filter_prod = c1.text_input("Filter by Campaign (before loading)", placeholder="Campaign name…", key="prod_camp_filter")
        exact_prod       = c2.checkbox("Exact", key="prod_exact")

        if st.button("📥 Load Product Data", key="load_prod", type="primary"):
            with st.spinner("Fetching product data…"):
                prod_df = fetch_product_performance(st.session_state.client, st.session_state.customer_id, start_date_prod, end_date_prod)
                if not prod_df.empty:
                    prod_df = process_dataframe(prod_df)
                    if camp_filter_prod:
                        prod_df = prod_df[prod_df['campaign_name']==camp_filter_prod] if exact_prod else prod_df[prod_df['campaign_name'].str.contains(camp_filter_prod, case=False, na=False)]
                    agg_prod = prod_df.groupby('product_title').agg({'cost':'sum','clicks':'sum','impressions':'sum','conversions':'sum','conversions_value':'sum'}).reset_index()
                    agg_prod = recalculate_metrics(agg_prod).sort_values('cost', ascending=False)
                    st.session_state.product_data = agg_prod
                    st.success("✅ Product data loaded!")
                else:
                    st.warning("No product data found.")

        if st.session_state.product_data is not None and not st.session_state.product_data.empty:
            st.markdown("---")
            st.subheader("🏆 Product Performance Insights")
            render_hero_kpi_cards(st.session_state.product_data.rename(columns={'product_title':'campaign_name'}))
            st.markdown("### 📊 Top 5 Products")
            df_prod_renamed = st.session_state.product_data.rename(columns={'product_title':'campaign_name'})
            render_top5_bar_chart(df_prod_renamed, 'campaign_name',
                                  ['conversions_value','cost'],
                                  {'cost':'Cost','conversions':'Conversions','conversions_value':'Revenue',
                                   'conv_value_cost':'ROAS','clicks':'Clicks','cpc':'CPC'},
                                  key_suffix="prod")
            st.markdown("---")
            c1,c2,c3 = st.columns(3)
            prod_title_filter = c1.text_input("Filter by Product Title", key="prod_title_filter")
            min_spend_prod    = c2.number_input("Min Spend ($)", min_value=0.0, value=0.0, key="min_spend_prod")
            min_aov_prod      = c3.number_input("Min AOV ($)",   min_value=0.0, value=0.0, key="min_aov_prod")
            df_pfilt = st.session_state.product_data.copy()
            if prod_title_filter: df_pfilt = df_pfilt[df_pfilt['product_title'].str.contains(prod_title_filter, case=False, na=False)]
            if min_spend_prod>0:  df_pfilt = df_pfilt[df_pfilt['cost']>=min_spend_prod]
            if min_aov_prod>0:    df_pfilt = df_pfilt[df_pfilt['aov']>=min_aov_prod]
            show_all_prod = st.checkbox("Show all products", value=False, key="show_all_prod")
            df_show = df_pfilt if show_all_prod else df_pfilt.head(50)
            if not show_all_prod:
                st.info(f"Showing top 50 of {len(df_pfilt)} products. Check 'Show all' to expand.")
            df_show = calculate_share_metrics(df_show)
            # Table
            tbl_cols = ['product_title','cost','soc','conversions_value','sor','soc_sor_ratio',
                        'conv_value_cost','cpc','ctr','clicks','impressions','conversions','cost_per_conv','aov']
            tbl_cols = [c for c in tbl_cols if c in df_show.columns]
            prod_tbl = df_show[tbl_cols].copy().rename(columns={
                'product_title':'Product','conv_value_cost':'ROAS','conversions_value':'Revenue',
                'cost_per_conv':'Cost/Conv','soc':'SoC %','sor':'SoR %','soc_sor_ratio':'SoC/SoR'
            })
            def _clr(v):
                try:
                    f=float(v)
                    if f<1: return 'background-color:#d1fae5;color:#065f46'
                    if f>1: return 'background-color:#fee2e2;color:#991b1b'
                    return 'background-color:#f3f4f6;color:#6b7280'
                except: return ''
            styled_prod = prod_tbl.style.applymap(_clr, subset=['SoC/SoR'] if 'SoC/SoR' in prod_tbl.columns else []).format({c:'{:.2f}' for c in prod_tbl.select_dtypes(include=['float64']).columns})
            st.dataframe(styled_prod, use_container_width=True, height=600)
            st.download_button("📥 Download Product CSV", df_pfilt.to_csv(index=False),
                               f"products_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

    # ══════════════════════════════════════════
    # TAB 4 — CHANGE HISTORY (Google only)
    # ══════════════════════════════════════════
    with tabs[4]:
        st.header("📜 Change History")
        if not (st.session_state.google_connected or st.session_state.google_csv_uploaded):
            st.warning("⚠️ Connect Google Ads to view change history.")
            st.stop()

        col1,col2 = st.columns(2)
        hist_start = col1.date_input("Start Date", value=datetime.now()-timedelta(days=7), key="hist_start")
        hist_end   = col2.date_input("End Date",   value=datetime.now(), key="hist_end")

        st.markdown("---")
        c1,c2,c3 = st.columns([3,2,1])
        camp_filter_hist = c1.text_input("Filter by Campaign", placeholder="Campaign name…", key="hist_camp_filter")
        chg_type_filter  = c2.selectbox("Change Type", ["All Changes","Budget Changes Only","Bid Strategy Changes Only"], key="hist_chg_type")
        exact_hist       = c3.checkbox("Exact", key="hist_exact")

        if st.button("📥 Load Change History", key="load_hist", type="primary"):
            with st.spinner("Fetching change history…"):
                hist_df = fetch_change_history(st.session_state.client, st.session_state.customer_id, hist_start, hist_end)
                if not hist_df.empty:
                    st.session_state.change_history_data = hist_df
                    st.success(f"✅ Found {len(hist_df)} change(s)!")
                else:
                    st.session_state.change_history_data = None
                    st.info("No budget or bid strategy changes found.")

        if st.session_state.change_history_data is not None and not st.session_state.change_history_data.empty:
            df_hist = st.session_state.change_history_data.copy()
            if camp_filter_hist:
                df_hist = df_hist[df_hist['campaign_name']==camp_filter_hist] if exact_hist else df_hist[df_hist['campaign_name'].str.contains(camp_filter_hist, case=False, na=False)]
            if chg_type_filter == "Budget Changes Only":        df_hist = df_hist[df_hist['change_type']=='Budget Change']
            elif chg_type_filter == "Bid Strategy Changes Only": df_hist = df_hist[df_hist['change_type']=='Bid Strategy Change']
            if not df_hist.empty:
                k1,k2,k3 = st.columns(3)
                k1.metric("Total Changes",        len(df_hist))
                k2.metric("Budget Changes",       len(df_hist[df_hist['change_type']=='Budget Change']))
                k3.metric("Bid Strategy Changes", len(df_hist[df_hist['change_type']=='Bid Strategy Change']))
                st.markdown("---")
                display_hist = df_hist[['date','time','campaign_name','change_type','change_details']].copy()
                display_hist.columns = ['Date','Time','Campaign','Change Type','Details']
                display_hist = display_hist.sort_values('Date', ascending=False)
                st.dataframe(display_hist, use_container_width=True, height=600)
                st.download_button("📥 Download Change History CSV", df_hist.to_csv(index=False),
                                   f"change_history_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
            else:
                st.info("No changes match the selected filters.")

    # ══════════════════════════════════════════
    # TAB 5 — SHOPIFY ANALYTICS (placeholder)
    # ══════════════════════════════════════════
    with tabs[5]:
        st.header("🟢 Shopify Analytics")
        if not (st.session_state.shopify_connected or st.session_state.shopify_csv_uploaded):
            st.warning("⚠️ Connect Shopify to view analytics.")
            st.info("Go to **Welcome & Setup → Shopify** to upload your CSV or connect via API.")
        else:
            st.success("✅ Shopify data connected.")
            st.info("📊 **Full Shopify analytics coming in Phase 3!**")
            st.markdown("""
            **Planned features:**
            - Total orders, revenue & AOV metrics
            - New vs returning customer split
            - Retention curves and cohort analysis
            - Revenue time-series with trend detection
            - Customer lifetime value (CLV) estimates
            """)

    # ══════════════════════════════════════════
    # TAB 6 — MMM (placeholder)
    # ══════════════════════════════════════════
    with tabs[6]:
        st.header("🎯 Marketing Mix Modeling (MMM)")
        st.info("📊 **Full MMM coming in Phase 3-5!**")
        st.markdown("""
        **Connected data sources:**
        """)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Advertising Platforms:**")
            for flag, label in [('google_connected','Google Ads (API)'), ('google_csv_uploaded','Google Ads (CSV)'),
                                  ('meta_connected','Meta Ads (API)'),   ('meta_csv_uploaded','Meta Ads (CSV)'),
                                  ('tiktok_csv_uploaded','TikTok (CSV)')]:
                if st.session_state[flag]:
                    st.markdown(f"- ✅ {label}")
        with col2:
            st.markdown("**Revenue Data:**")
            for flag, label in [('shopify_connected','Shopify (API)'), ('shopify_csv_uploaded','Shopify (CSV)')]:
                if st.session_state[flag]:
                    st.markdown(f"- ✅ {label}")

        st.markdown("""
        **Planned MMM features:**
        - Contribution analysis per channel
        - iROAS & saturation curves
        - Adstock & carryover modelling
        - Budget scenario optimisation
        - Downloadable PDF report
        """)

if __name__ == "__main__":
    main()
