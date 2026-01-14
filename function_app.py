from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import azure.functions as func
import httpx
import logging
import psycopg

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="0 */15 11,14-19 * * 1-5",
              arg_name="mytimer",
              run_on_startup=False)
def sync_crm_to_postgres(mytimer: func.TimerRequest) -> None:
    logging.info("Sync CRM to Postgres function started at %s", datetime.utcnow().isoformat())
    def get_deals():
        deals = []
        with httpx.Client(timeout=30.0) as client:
            try:
                funnels = client.get("https://crm.rdstation.com/api/v1/deal_pipelines", params={"token": os.getenv("CRM_TOKEN")}).json()
            except Exception as e:
                raise Exception("Failed to fetch funnels") from e

            def fetch_deals(params):
                page = 1
                while True:
                    try:
                        print(f"Fetching deals with params: {params}, page: {page}")
                        data = client.get("https://crm.rdstation.com/api/v1/deals", params={
                            **params, 
                            "page": page,
                            "limit": 200,
                            "token": os.getenv("CRM_TOKEN")
                        }).json()
                        print(f"Fetched {len(data['deals'])} deals")
                        deals.extend(data["deals"])
                        if not data["has_more"]:
                            break
                        page += 1
                    except Exception as e:
                        raise Exception(f"Failed to fetch deals on page {page} with params {params}") from e

            fetch_deals({"win": "null"})
            fetch_deals({
                "win": "true",
                "closed_at_period": "true",
                "start_date": (datetime.now() - relativedelta(months=11)).replace(day=1).strftime("%Y-%m-%dT00:00:00"),
            })

        stage_to_funnel_name = {stage["id"]: funnel["name"] for funnel in funnels for stage in funnel["deal_stages"]}
        stage_to_funnel_order = {stage["id"]: funnel["order"] for funnel in funnels for stage in funnel["deal_stages"]}
        stage_to_stage_order = {stage["id"]: stage["order"] for funnel in funnels for stage in funnel["deal_stages"]}

        return [{
            "crm_id": deal["id"],
            "criada_em": deal["created_at"],
            "valor_recorrente": deal["amount_montly"],  # typo in API
            "valor_nao_recorrente": deal["amount_unique"],
            "previsao_fechamento": deal["prediction_date"], 
            "status": "Em andamento" if deal["win"] is None else "Ganha" if deal["win"] else "Perdida",
            "funil": stage_to_funnel_name[deal["deal_stage"]["id"]],
            "ordem_funil": stage_to_funnel_order[deal["deal_stage"]["id"]],
            "estagio": deal["deal_stage"]["name"],
            "ordem_estagio": stage_to_stage_order[deal["deal_stage"]["id"]],
            "data_fechamento": deal["closed_at"]
        } for deal in deals]

    try:
        with psycopg.connect() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                deals = get_deals()

                logging.info("Total deals to sync: %s", len(deals))
                print(f"Total deals to sync: {len(deals)}")
                
                # Nuke and pave approach: delete all and insert fresh
                cur.execute('DELETE FROM "comercial"."negociacoes"')
                logging.info("Deleted all existing records")
                
                insert_sql = """
                    INSERT INTO "comercial"."negociacoes" 
                    (crm_id, criada_em, valor_recorrente, valor_nao_recorrente, previsao_fechamento, status, funil, ordem_funil, estagio, ordem_estagio, data_fechamento)
                    VALUES (%(crm_id)s, %(criada_em)s, %(valor_recorrente)s, %(valor_nao_recorrente)s, %(previsao_fechamento)s, %(status)s, %(funil)s, %(ordem_funil)s, %(estagio)s, %(ordem_estagio)s, %(data_fechamento)s)
                """
                if deals:
                    logging.debug("Insert sample: %s", deals[0])
                    cur.executemany(insert_sql, deals)

                conn.commit()

    except Exception as e:
        raise Exception("Database operation failed") from e

sync_crm_to_postgres(None)