import { OverviewResponse, SegmentPredictionResult } from "../types/api";
import { apiClient } from "./client";

export async function fetchOverview(model = "first"): Promise<OverviewResponse> {
  const res = await apiClient.get<OverviewResponse>("/dashboard/overview", {
    params: { model },
  });
  return res.data;
}

export async function fetchAlerts(model = "first"): Promise<SegmentPredictionResult[]> {
  const res = await apiClient.get<SegmentPredictionResult[]>("/dashboard/alerts", {
    params: { model },
  });
  return res.data;
}
