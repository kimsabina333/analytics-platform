import { useEffect, useState } from "react";
import { FilterPanel } from "../components/segments/FilterPanel";
import { SegmentTable } from "../components/segments/SegmentTable";
import { SRTimeSeriesChart } from "../components/dashboard/SRTimeSeriesChart";
import { fetchDimensions, fetchSegmentSR, fetchComboSR } from "../api/segments";
import { SegmentPredictionResult } from "../types/api";

interface Props {
  model?: "first" | "recurring";
  title?: string;
}

export function SegmentExplorerPage({ model = "first", title = "SEGMENT EXPLORER" }: Props) {
  const [dimensions, setDimensions] = useState<string[]>([]);
  const [categories, setCategories] = useState<Record<string, string[]>>({});
  const [result, setResult] = useState<SegmentPredictionResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchDimensions(model).then(({ dimensions, categories }) => {
      setDimensions(dimensions);
      setCategories(categories);
    });
    setResult(null);
  }, [model]);

  const handleApply = async (filters: Record<string, string>, q: number) => {
    if (Object.keys(filters).length === 0) return;
    setLoading(true);
    setError(null);
    try {
      const keys = Object.keys(filters);
      let res: SegmentPredictionResult;
      if (keys.length === 1) {
        res = await fetchSegmentSR(keys[0], filters[keys[0]], q, model);
      } else {
        res = await fetchComboSR(filters, q, model);
      }
      setResult(res);
    } catch (e: unknown) {
      setError(String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <h1 className="text-sm font-bold text-cyan-400 tracking-widest">{title}</h1>

      <FilterPanel dimensions={dimensions} categories={categories} onApply={handleApply} />

      {loading && (
        <div className="text-center py-8 text-cyan-400 animate-pulse text-sm">
          Running Bayesian inference...
        </div>
      )}

      {error && (
        <div className="border border-red-600 text-red-400 rounded p-3 text-sm">{error}</div>
      )}

      {result && !loading && (
        <div className="space-y-4">
          <SRTimeSeriesChart result={result} />
          <SegmentTable result={result} />
        </div>
      )}
    </div>
  );
}
