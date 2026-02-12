import os
import sys
import json
import logging
from datetime import datetime, timedelta, timezone
import dateutil.parser
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from azure.monitor.query import LogsQueryClient
from azure.identity import DefaultAzureCredential

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO ---
WORKSPACE_ID = os.environ.get("AZURE_WORKSPACE_ID", "bd026a2c-7612-4892-8249-c8190aeaa5d0")

app = FastAPI(title="Leahpar Code Analytics")

# Inicialização Azure
logs_client = None
try:
    credential = DefaultAzureCredential()
    logs_client = LogsQueryClient(credential)
    logger.info("✅ Credenciais Azure carregadas.")
except Exception as e:
    logger.error(f"❌ Erro ao carregar Azure: {e}")

def parse_rows(result):
    if result is None or not result.tables or len(result.tables) == 0: 
        return []
    table = result.tables[0]
    cols = [c.name if hasattr(c, 'name') else str(c) for c in table.columns]
    return [dict(zip(cols, row)) for row in table.rows]

def fill_time_gaps_dual(raw_rows, start_date, end_date):
    map_in = {}
    map_out = {}
    for row in raw_rows:
        try:
            t_str = row.get('TimeGenerated') or row.get('bin')
            if not t_str: continue
            dt = dateutil.parser.isoparse(str(t_str))
            dt_hour = dt.replace(minute=0, second=0, microsecond=0)
            map_in[dt_hour] = row.get('count_in', 0)
            map_out[dt_hour] = row.get('count_out', 0)
        except: pass

    final_times, final_in, final_out = [], [], []
    current = start_date.replace(minute=0, second=0, microsecond=0)
    while current <= end_date:
        final_times.append(current.isoformat())
        final_in.append(map_in.get(current, 0))
        final_out.append(map_out.get(current, 0))
        current += timedelta(hours=1)
    return final_times, final_in, final_out

@app.get("/api/metrics")
async def get_metrics(date: str = None):
    # Fallback seguro
    fallback_data = {
        "msgs_total": 0, "msgs_received": 0, "msgs_sent": 0,
        "sql_success_rate": 100.0, "latency_p95": 0, "error_count": 0,
        "volume_times": [], "received_data": [], "sent_data": [],
        "status_labels": [], "status_values": [],
        "funnel_data": [0, 0, 0],
        "top_users_labels": [], "top_users_totals": []
    }

    if not logs_client:
        return fallback_data

    try:
        now_utc = datetime.now(timezone.utc)
        if date:
            start_dt = dateutil.parser.parse(f"{date}T03:00:00Z")
            end_dt = start_dt + timedelta(hours=23, minutes=59, seconds=59)
        else:
            today_brt = (now_utc - timedelta(hours=3)).replace(hour=0, minute=0, second=0, microsecond=0)
            start_dt = today_brt + timedelta(hours=3)
            end_dt = now_utc

        kql_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        kql_end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        time_filter = f"| where TimeGenerated between (todatetime('{kql_start}') .. todatetime('{kql_end}'))"
        query_span = timedelta(days=90)

        # --- QUERY KPI ESTRITA (AUDIT) ---
        q_kpis = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') 
        | extend parsed = iff(json_start >= 0, parse_json(substring(Log_s, json_start)), dynamic(null))
        | extend event_type = tostring(parsed.event)
        
        | extend is_in = (event_type == "audit.webhook_message_in")
        | extend is_out = (event_type == "audit.webhook_enqueued" and parsed.io == "output")
        | extend is_dump = (event_type == "audit.trace_dump")
        | extend is_fail = (parsed.status == "ERROR")
        | extend duration = toint(parsed.total_duration_ms)
        
        | summarize 
            msgs_received = countif(is_in),
            msgs_sent = countif(is_out),
            median_lat = percentile(iff(is_dump and duration > 0, duration, int(null)), 50),
            errors = countif(is_fail)
        """
        
        # --- QUERY TOP USUÁRIOS (DIA) ---
        q_top_users = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') | where json_start >= 0
        | extend parsed = parse_json(substring(Log_s, json_start))
        | extend event_type = tostring(parsed.event)
        | where event_type == "audit.webhook_message_in" or (event_type == "audit.webhook_enqueued" and parsed.io == "output")
        | extend user_id = coalesce(tostring(parsed.from_phone), tostring(parsed.phone_suffix))
        | where isnotempty(user_id)
        | summarize total = count() by user_id
        | top 4 by total
        """

        # --- QUERY STATUS (LOGS GERAIS DO DIA) ---
        # AQUI MUDOU: Removemos filtros restritivos para pegar "Tudo do Dia"
        q_status = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') 
        | extend parsed = iff(json_start >= 0, parse_json(substring(Log_s, json_start)), dynamic(null))
        | extend status_label = case(
            // 1. Falhas (Vermelho)
            parsed.status == "ERROR" or Log_s has "[ERROR]" or Log_s has "Exception", "Falhas",
            
            // 2. Logs de Negócio (Azul)
            parsed.event == "audit.webhook_message_in", "Recebidas",
            parsed.event == "audit.webhook_enqueued" and parsed.io == "output", "Enviadas",
            
            // 3. Todo o Resto (Azul - Logs Operacionais)
            "Logs Operacionais"
        )
        | summarize count() by status = status_label
        """

        # Volume Hora a Hora
        q_volume = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') | where json_start >= 0
        | extend parsed = parse_json(substring(Log_s, json_start))
        | extend event_type = tostring(parsed.event)
        | extend is_in = (event_type == "audit.webhook_message_in")
        | extend is_out = (event_type == "audit.webhook_enqueued" and parsed.io == "output")
        | where is_in or is_out
        | summarize count_in = countif(is_in), count_out = countif(is_out) by bin(TimeGenerated, 1h)
        | order by TimeGenerated asc
        """

        # Execução das Queries
        res_kpis = logs_client.query_workspace(WORKSPACE_ID, q_kpis, timespan=query_span)
        res_users = logs_client.query_workspace(WORKSPACE_ID, q_top_users, timespan=query_span)
        res_stat = logs_client.query_workspace(WORKSPACE_ID, q_status, timespan=query_span)
        res_vol = logs_client.query_workspace(WORKSPACE_ID, q_volume, timespan=query_span)

        kpis = parse_rows(res_kpis)[0] if (res_kpis.tables and len(res_kpis.tables) > 0 and res_kpis.tables[0].rows) else {}
        top_users_rows = parse_rows(res_users)
        status_rows = parse_rows(res_stat)
        vol_rows = parse_rows(res_vol)
        
        times_clean, in_clean, out_clean = fill_time_gaps_dual(vol_rows, start_dt, end_dt)

        rec = kpis.get('msgs_received', 0) or 0
        sent = kpis.get('msgs_sent', 0) or 0
        total_vol = rec + sent
        err_count = kpis.get('errors', 0) or 0

        if total_vol > 0:
            success_rate = round(((total_vol - err_count) / total_vol * 100), 1)
            if success_rate < 0: success_rate = 0.0
        else:
            success_rate = 100.0

        raw_lat = kpis.get('median_lat')
        try:
            final_lat = int(float(raw_lat)) if raw_lat is not None else 0   
        except:
            final_lat = 0

        return {
            "msgs_total": total_vol,
            "msgs_received": rec,
            "msgs_sent": sent,
            "sql_success_rate": success_rate,
            "latency_p95": final_lat,
            "error_count": err_count,
            "volume_times": times_clean,
            "received_data": in_clean,
            "sent_data": out_clean,
            "status_labels": [row['status'] for row in status_rows],
            "status_values": [row['count_'] for row in status_rows],
            "funnel_data": [rec, sent, 0], 
            "top_users_labels": [row['user_id'] for row in top_users_rows],
            "top_users_totals": [row['total'] for row in top_users_rows]
        }
    except Exception as e:
        logger.error(f"❌ Erro API: {e}")
        return fallback_data

@app.get("/")
async def serve_frontend():
    return FileResponse('index.html') if os.path.exists('index.html') else HTMLResponse("index.html missing")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8002))
    uvicorn.run(app, host="0.0.0.0", port=port)
