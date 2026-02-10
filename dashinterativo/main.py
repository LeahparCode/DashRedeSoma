import os
import logging
from datetime import datetime, timedelta, timezone
import dateutil.parser
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from azure.monitor.query import LogsQueryClient
from azure.identity import DefaultAzureCredential

# Configuração de Logs para depuração
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
    if not result or not result.tables or len(result.tables) == 0: 
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
    # ESTRUTURA DE FALLBACK
    fallback_data = {
        "msgs_total": 0, "msgs_received": 0, "msgs_sent": 0,
        "sql_success_rate": 100, "latency_p95": 0, "error_count": 0,
        "volume_times": [], "received_data": [], "sent_data": [],
        "status_labels": [], "status_values": [],
        "funnel_data": [0, 0, 0],
        "top_users_labels": [], "top_users_totals": []
    }

    if not logs_client:
        logger.error("Azure client não inicializado")
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

        # QUERY 1: KPIs (Correção Latência: Remove dependência estrita de tipo de evento)
        q_kpis = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') 
        | extend parsed = iff(json_start >= 0, parse_json(substring(Log_s, json_start)), dynamic(null))
        | extend is_real_msg_in = (parsed.event == "webhook_message_parsed")
        | extend is_real_msg_out = (parsed.event == "whatsapp_status" and parsed.status == "sent")
        | extend is_text_success = (Log_s has "[OK]" and Log_s has "SUCCESS")
        | extend is_text_error = (Log_s has "[ERROR]") or (Log_s has "Exception")
        | extend duration_val = coalesce(toint(parsed.duration_ms), toint(extract(@"(\\d+)ms", 1, Log_s)), toint(todouble(extract(@"(\\d+\\.\\d+)s", 1, Log_s)) * 1000))
        | summarize 
            msgs_received = countif(is_real_msg_in),
            msgs_sent = countif(is_real_msg_out),
            msgs_delivered = countif(parsed.status == "delivered"),
            msgs_read = countif(parsed.status == "read"),
            sql_total_ops = countif(is_real_msg_out or is_text_success or is_text_error),
            sql_success_ops = countif(is_text_success),
            median_lat = percentile(iff(duration_val > 100, duration_val, int(null)), 50), 
            errors = countif(is_text_error)
        """
        
        # QUERY 2: Top 4 Usuários
        q_top_users = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') | where json_start >= 0
        | extend parsed = parse_json(substring(Log_s, json_start))
        | extend user_id = coalesce(tostring(parsed.phone_suffix), tostring(parsed.recipient_id), tostring(parsed.sender_id))
        | where isnotempty(user_id)
        | extend is_in = (parsed.event == "webhook_message_parsed")
        | extend is_out = (parsed.event == "whatsapp_status" and parsed.status == "sent")
        | where is_in or is_out
        | summarize total = count() by user_id
        | top 4 by total
        """

        # QUERY 3: Status
        q_status = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') 
        | extend parsed = iff(json_start >= 0, parse_json(substring(Log_s, json_start)), dynamic(null))
        | extend status_label = case(
            parsed.event == "webhook_message_parsed", "Mensagem Recebida",
            parsed.event == "whatsapp_status" and parsed.status == "sent", "Resposta Enviada",
            Log_s has "ERROR" or Log_s has "Exception", "Erros de Log",
            isnotempty(parsed.event), "Logs Técnicos",
            "Outros"
        )
        | where status_label != "Outros"
        | summarize count() by status = status_label
        """

        # QUERY 4: Volume
        q_volume = f"""
        ContainerAppConsoleLogs_CL 
        {time_filter}
        | extend json_start = indexof(Log_s, '{{') | where json_start >= 0
        | extend parsed = parse_json(substring(Log_s, json_start))
        | extend is_in = (parsed.event == "webhook_message_parsed")
        | extend is_out = (parsed.event == "whatsapp_status" and parsed.status == "sent")
        | where is_in or is_out
        | summarize count_in = countif(is_in), count_out = countif(is_out) by bin(TimeGenerated, 1h)
        | order by TimeGenerated asc
        """

        # Execução segura
        res_kpis = logs_client.query_workspace(WORKSPACE_ID, q_kpis, timespan=query_span)
        res_users = logs_client.query_workspace(WORKSPACE_ID, q_top_users, timespan=query_span)
        res_stat = logs_client.query_workspace(WORKSPACE_ID, q_status, timespan=query_span)
        res_vol = logs_client.query_workspace(WORKSPACE_ID, q_volume, timespan=query_span)

        kpis = parse_rows(res_kpis)[0] if (res_kpis.tables and res_kpis.tables[0].rows) else {}
        top_users_rows = parse_rows(res_users)
        status_rows = parse_rows(res_stat)
        vol_rows = parse_rows(res_vol)
        
        times_clean, in_clean, out_clean = fill_time_gaps_dual(vol_rows, start_dt, end_dt)

        total_ops = kpis.get('sql_total_ops', 0) or 0
        rate = round((kpis.get('sql_success_ops', 0) / total_ops * 100), 1) if total_ops > 0 else 100.0
        
        # Tratamento da latência (Usando Median com fallback seguro)
        raw_lat = kpis.get('median_lat')
        try:
            final_lat = int(float(raw_lat)) if raw_lat is not None else 0
        except:
            final_lat = 0

        return {
            "msgs_total": (kpis.get('msgs_received') or 0) + (kpis.get('msgs_sent') or 0),
            "msgs_received": kpis.get('msgs_received') or 0,
            "msgs_sent": kpis.get('msgs_sent') or 0,
            "sql_success_rate": rate,
            "latency_p95": final_lat,
            "error_count": kpis.get('errors') or 0,
            "volume_times": times_clean,
            "received_data": in_clean,
            "sent_data": out_clean,
            "status_labels": [row['status'] for row in status_rows] if status_rows else [],
            "status_values": [row['count_'] for row in status_rows] if status_rows else [],
            "funnel_data": [kpis.get('msgs_sent') or 0, kpis.get('msgs_delivered') or 0, kpis.get('msgs_read') or 0],
            "top_users_labels": [row['user_id'] for row in top_users_rows] if top_users_rows else [],
            "top_users_totals": [row['total'] for row in top_users_rows] if top_users_rows else []
        }
    except Exception as e:
        logger.error(f"❌ Erro ao processar métricas: {e}")
        return fallback_data

@app.get("/")
async def serve_frontend():
    return FileResponse('index.html') if os.path.exists('index.html') else HTMLResponse("index.html missing")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8002))
    uvicorn.run(app, host="0.0.0.0", port=port)
