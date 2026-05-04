from fastapi import FastAPI, Request, HTTPException
import pandas as pd
from model_prediction import predict_ltv

app = FastAPI()


@app.post("/predict_ltv/")
async def predict_ltv_endpoint(request: Request):
    try:
        payload = await request.json()
        if "data" not in payload:
            raise HTTPException(
                status_code=400, detail="'data' key not found in JSON body")

        df = pd.DataFrame(payload["data"])
        result_df = predict_ltv(df)  # <- передаёшь DataFrame напрямую

        # Сериализуем результат в список словарей
        return {"result": result_df.reset_index().to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
