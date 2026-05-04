import { SegmentPredictionResult, TopSegment } from "../types/api";
import { apiClient } from "./client";

export async function fetchDimensions(model = "first"): Promise<{
  dimensions: string[];
  categories: Record<string, string[]>;
}> {
  const res = await apiClient.get("/segments/dimensions", { params: { model } });
  return res.data;
}

export async function fetchSegmentSR(
  dimension: string,
  value: string,
  q = 0.05,
  model = "first"
): Promise<SegmentPredictionResult> {
  const res = await apiClient.get<SegmentPredictionResult>("/segments/sr", {
    params: { dimension, value, q, model },
  });
  return res.data;
}

export async function fetchComboSR(
  filters: Record<string, string>,
  q = 0.05,
  model = "first"
): Promise<SegmentPredictionResult> {
  const res = await apiClient.post<SegmentPredictionResult>("/segments/sr/combo", {
    filters,
    q,
    model,
  });
  return res.data;
}

export async function fetchTopSegments(
  dimension: string,
  n = 5,
  order: "best" | "worst" = "best",
  model = "first"
): Promise<TopSegment[]> {
  const res = await apiClient.get<TopSegment[]>("/segments/top", {
    params: { dimension, n, order, model },
  });
  return res.data;
}
