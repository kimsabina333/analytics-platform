import asyncio
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

# MUST be called before any PyMC/matplotlib import
import matplotlib
matplotlib.use("Agg")

from fastapi import FastAPI

from backend.core.config import VENDOR_DIR, settings

_prediction_service_first = None
_prediction_service_recur = None
_ai_service_first = None
_ai_service_recur = None
_ai_service_ltv = None
_ltv_service = None
_risk_service = None
_ai_service_risk = None
_marketing_service = None
_ai_service_marketing = None


def get_prediction_service(model: str = "first"):
    if model == "recurring":
        return _prediction_service_recur
    return _prediction_service_first


def get_ai_service(model: str = "first"):
    if model == "recurring":
        return _ai_service_recur
    if model == "ltv":
        return _ai_service_ltv
    return _ai_service_first


def get_ltv_service():
    return _ltv_service

def get_risk_service():
    return _risk_service

def get_ai_risk_service():
    return _ai_service_risk

def get_marketing_service():
    return _marketing_service

def get_ai_marketing_service():
    return _ai_service_marketing


def _load_all_services():
    global _prediction_service_first, _prediction_service_recur
    global _ai_service_first, _ai_service_recur, _ai_service_ltv
    global _ltv_service, _marketing_service, _ai_service_marketing

    if settings.google_credentials_base64:
        import base64, tempfile
        try:
            cleaned = settings.google_credentials_base64.strip()
            creds_json = base64.b64decode(cleaned)
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            tmp.write(creds_json); tmp.flush(); tmp.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
            print(f"GCP credentials loaded from base64 ({len(creds_json)} bytes)")
        except Exception as e:
            print(f"ERROR: failed to decode GOOGLE_CREDENTIALS_BASE64 — {e}")
    elif settings.google_application_credentials:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.google_application_credentials
    local_cache_dir = Path(__file__).parents[2] / ".runtime_cache"
    local_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ["ARVIZ_CACHE_DIR"] = str(local_cache_dir / "arviz")
    os.environ["MPLCONFIGDIR"] = str(local_cache_dir / "matplotlib")
    os.environ["LOCALAPPDATA"] = str(local_cache_dir / "localappdata")
    for cache_name in ("arviz", "matplotlib", "localappdata"):
        (local_cache_dir / cache_name).mkdir(parents=True, exist_ok=True)
    try:
        import platformdirs
        platformdirs.user_cache_dir = lambda *args, **kwargs: str(local_cache_dir / "platformdirs")
        (local_cache_dir / "platformdirs").mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    vendor_str = str(VENDOR_DIR)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)

    import joblib
    import yaml

    from backend.services.db_service import DBService, DB_PATH
    from backend.services.cache_service import CacheService
    from backend.services.model_service import ModelService
    from backend.services.data_service import DataService
    from backend.services.prediction_service import PredictionService
    from backend.services.ai_service import AIService

    encoder_path = VENDOR_DIR / "idata" / "ordinal_encoder.joblib"
    encoder = joblib.load(str(encoder_path))

    db_svc = DBService(DB_PATH)
    cache_svc = CacheService(settings.redis_url, settings.cache_ttl_seconds, db=db_svc)
    print("DB: SQLite persistent cache ready at", DB_PATH)

    # ── First model ───────────────────────────────────────────────────────────
    q_map_path_first = VENDOR_DIR / "idata" / "sr_alert_q_map.yaml"
    with open(str(q_map_path_first)) as f:
        q_map_first = yaml.safe_load(f)

    model_svc_first = ModelService(
        encoder=encoder,
        q_map=q_map_first,
        vendor_dir=VENDOR_DIR,
        model_path="idata/idata_sr_ts.nc",
    )
    data_svc_first = DataService(vendor_dir=VENDOR_DIR)
    pred_svc_first = PredictionService(
        model_svc_first,
        data_svc_first,
        cache_svc,
        cache_prefix="first:",
        db_svc=db_svc,
        model_key="first",
    )
    ai_svc_first = AIService(pred_svc_first, settings.openrouter_api_key, model_label="first")

    # ── Recurring model ───────────────────────────────────────────────────────
    pred_svc_recur = None
    ai_svc_recur = None
    try:
        q_map_path_recur = VENDOR_DIR / "idata" / "sr_alert_q_map_recur.yaml"
        recur_sql_path = str(VENDOR_DIR / "sql_templates_recur") + "/"

        if q_map_path_recur.exists():
            with open(str(q_map_path_recur)) as f:
                q_map_recur = yaml.safe_load(f)
        else:
            print("WARNING: sr_alert_q_map_recur.yaml not found — recurring model using default q=0.05")
            q_map_recur = {}

        model_svc_recur = ModelService(
            encoder=encoder,
            q_map=q_map_recur,
            vendor_dir=VENDOR_DIR,
            model_path="idata/idata_sr_rec_ts.nc",
        )
        data_svc_recur = DataService(vendor_dir=VENDOR_DIR, sql_path=recur_sql_path)
        pred_svc_recur = PredictionService(
            model_svc_recur,
            data_svc_recur,
            cache_svc,
            cache_prefix="recur:",
            db_svc=db_svc,
            model_key="recurring",
        )
        ai_svc_recur = AIService(pred_svc_recur, settings.openrouter_api_key, model_label="recurring")
        print("Recurring SR model ready.")
    except Exception as e:
        print(f"WARNING: Recurring SR model failed to load — {e}")

    # ── LTV model ─────────────────────────────────────────────────────────────
    ltv_svc = None
    ai_svc_ltv = None
    risk_svc = None
    marketing_svc = None
    ai_svc_marketing = None
    try:
        from google.cloud import bigquery
        from backend.services.ltv_service import LTVService
        from backend.services.risk_service import RiskService
        from backend.services.marketing_service import MarketingService
        bq_client = bigquery.Client()
        ltv_svc = LTVService(bq_client=bq_client)
        ai_svc_ltv = AIService(ltv_svc=ltv_svc, api_key=settings.openrouter_api_key, model_label="ltv")
        print("LTV service ready.")
        risk_svc = RiskService(bq_client=bigquery.Client(), db_svc=db_svc)
        ai_svc_risk = AIService(risk_svc=risk_svc, api_key=settings.openrouter_api_key, model_label="risk")
        print("Risk service ready.")
        marketing_svc = MarketingService(bq_client=bigquery.Client(), db_svc=db_svc)
        ai_svc_marketing = AIService(marketing_svc=marketing_svc, api_key=settings.openrouter_api_key, model_label="marketing")
        print("Marketing service ready.")
    except Exception as e:
        print(f"WARNING: BQ services failed to load — {e}")
        ai_svc_risk = None

    return pred_svc_first, pred_svc_recur, ai_svc_first, ai_svc_recur, ltv_svc, ai_svc_ltv, risk_svc, ai_svc_risk, marketing_svc, ai_svc_marketing


_startup_done = False

def is_ready() -> bool:
    return _startup_done


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _prediction_service_first, _prediction_service_recur
    global _ai_service_first, _ai_service_recur, _ai_service_ltv
    global _ltv_service, _risk_service, _ai_service_risk, _marketing_service, _ai_service_marketing
    global _startup_done

    async def _background_load():
        global _prediction_service_first, _prediction_service_recur
        global _ai_service_first, _ai_service_recur, _ai_service_ltv
        global _ltv_service, _risk_service, _ai_service_risk, _marketing_service, _ai_service_marketing
        global _startup_done
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _load_all_services)
            (
                _prediction_service_first, _prediction_service_recur,
                _ai_service_first, _ai_service_recur,
                _ltv_service, _ai_service_ltv, _risk_service, _ai_service_risk, _marketing_service, _ai_service_marketing,
            ) = result
            _startup_done = True
            print("All services loaded and ready.")
        except Exception as e:
            import traceback
            print(f"FATAL: service loading failed — {e}")
            traceback.print_exc()
            _startup_done = True

    asyncio.create_task(_background_load())
    yield
