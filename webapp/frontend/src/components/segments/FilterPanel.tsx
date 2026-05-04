import { useState } from "react";

interface Props {
  dimensions: string[];
  categories: Record<string, string[]>;
  onApply: (filters: Record<string, string>, q: number) => void;
}

export function FilterPanel({ dimensions, categories, onApply }: Props) {
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [q, setQ] = useState(0.05);

  const handleSelect = (dim: string, val: string) => {
    setFilters((prev) => {
      if (prev[dim] === val) {
        const next = { ...prev };
        delete next[dim];
        return next;
      }
      return { ...prev, [dim]: val };
    });
  };

  const activeFilters = Object.keys(filters);

  return (
    <div className="border border-gray-700 rounded p-4 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-bold text-cyan-400">Filters</h3>
        <div className="flex items-center gap-3">
          <label className="text-xs text-gray-400">
            CI:
            <select
              value={q}
              onChange={(e) => setQ(Number(e.target.value))}
              className="ml-1 bg-gray-800 border border-gray-600 rounded text-white text-xs px-1"
            >
              <option value={0.10}>80%</option>
              <option value={0.05}>90%</option>
              <option value={0.025}>95%</option>
            </select>
          </label>
          <button
            onClick={() => onApply(filters, q)}
            disabled={activeFilters.length === 0}
            className="text-xs bg-cyan-700 hover:bg-cyan-600 disabled:opacity-40 text-white px-3 py-1 rounded"
          >
            Apply
          </button>
          {activeFilters.length > 0 && (
            <button
              onClick={() => setFilters({})}
              className="text-xs text-gray-400 hover:text-white"
            >
              Clear
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
        {dimensions.map((dim) => (
          <div key={dim}>
            <p className="text-xs text-gray-400 mb-1">{dim}</p>
            <div className="flex flex-wrap gap-1">
              {(categories[dim] || []).map((val) => {
                const active = filters[dim] === val;
                return (
                  <button
                    key={val}
                    onClick={() => handleSelect(dim, val)}
                    className={`text-xs px-2 py-0.5 rounded border transition-colors ${
                      active
                        ? "border-cyan-400 bg-cyan-900 text-cyan-300"
                        : "border-gray-600 text-gray-300 hover:border-gray-400"
                    }`}
                  >
                    {val}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
