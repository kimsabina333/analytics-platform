import json
import re
from pathlib import Path

import pandas as pd

try:
    from rapidfuzz import process, fuzz
except ImportError:
    raise ImportError("pip install rapidfuzz")


class BankNameNormalizer:
    PLACEHOLDERS = {"UNKNOWN", "NA", "N/A", "NONE", "NULL", "-", ""}

    _CARD_TYPE_RE = re.compile(
        r"\s*-?\s*(PREPAID\s+DEBIT|CONSUMER\s+DEBIT|CONSUMER\s+CREDIT"
        r"|COMMERCIAL\s+CREDIT|COMMERCIAL\s+DEBIT|DEBIT|CREDIT|PREPAID)\s*$"
    )

    _LEGAL_SUFFIXES = [
        r",?\s+NATIONAL\s+ASSOCIATION",
        r",?\s+CLOSED\s+JOINT[\s-]*STOCK\s+COMPANY",
        r",?\s+OPEN\s+JOINT[\s-]*STOCK\s+COMPANY",
        r",?\s+JOINT[\s-]+STOCK\s+COMPANY",
        r",?\s+PUBLIC\s+LIMITED\s+COMPANY",
        r",?\s+PUBLIC\s+COMPANY\s+LIMITED",
        r",?\s+SHARE\s+COMPANY",

        r",?\s+S\.?A\.?\s+DE\s+C\.?V\.?\s*,?\s*SOFOM\s+E\.?R\.?",
        r",?\s+S\.?A\.?\s+DE\s+C\.?V\.?",
        r",?\s+SA\s+DE\s+CV\s*,?\s*SOFOM\s+E\.?R\.?",
        r",?\s+SA\s+DE\s+CV",

        r",?\s+LIMITED",
        r",?\s+LTD\.?",
        r",?\s+INC\.?",
        r",?\s+LLC\.?",
        r",?\s+L\.?L\.?C\.?",
        r",?\s+INCORPORATED",
        r",?\s+CORPORATION",
        r",?\s+CORP\.?",
        r",?\s+CO\.?",
        r",?\s+P\.?L\.?C\.?",

        r",?\s+P\.?J\.?S\.?C\.?",
        r",?\s+C\.?J\.?S\.?C\.?",
        r",?\s+O\.?J\.?S\.?C\.?",
        r",?\s+J\.?S\.?C\.?",
        r",?\s+A\.?O\.?",
        r",?\s+TOO",
        r",?\s+SHA\.?",

        r",?\s+S\.?P\.?A\.?",
        r",?\s+S\.?A\.?S\.?",
        r",?\s+S\.?A\.?R\.?L\.?",
        r",?\s+S\.?A\.?K\.?",
        r",?\s+S\.?A\.?",
        r",?\s+A\.?G\.?",
        r",?\s+A\.?S\.?",
        r",?\s+S\.?E\.?",
        r",?\s+G\.?M\.?B\.?H\.?",
        r",?\s+B\.?S\.?C\.?",
        r",?\s+B\.?V\.?",
        r",?\s+N\.?V\.?",
        r",?\s+D\.?D\.?",
        r",?\s+E\.?G\.?",
        r",?\s+H\.?F\.?",
        r",?\s+K\.?S\.?C\.?",
        r",?\s+Q\.?S\.?C\.?",
        r",?\s+P\.?S\.?C\.?",

        r",?\s+N\.?A\.?",
        r",?\s+F\.?S\.?B\.?",
        r",?\s+S\.?S\.?B\.?",

        r",?\s+PTY\.?",
        r",?\s+PTE\.?",

        r",?\s+C\s*\.?\s*R\s*\.?\s*L\s*\.?",
        r",?\s+B\.?\s*M\.?",
    ]
    _SUFFIX_PATTERNS = [re.compile(p + r"\s*$", re.IGNORECASE) for p in _LEGAL_SUFFIXES]
    _ACRONYM_RE = re.compile(r"^[A-Z]{2,7}$")

    def __init__(
        self,
        file_path: str,
        json_path: str,
        bank_column: str = "card_bank",
        fuzzy_threshold: int = 90,
        scorer=None,
        sheet_name=0,
    ):
        self.file_path = Path(file_path)
        self.json_path = Path(json_path)
        self.bank_column = bank_column
        self.fuzzy_threshold = fuzzy_threshold
        self.scorer = scorer or fuzz.token_sort_ratio
        self.sheet_name = sheet_name

        self.df = None
        self.normalized_dict = None
        self.reverse_map = None

    @staticmethod
    def normalize_column_name(col: str) -> str:
        col = str(col).strip().lower()
        col = re.sub(r"[^\w\s]", " ", col)
        col = re.sub(r"\s+", "_", col)
        col = re.sub(r"_+", "_", col)
        return col.strip("_")

    @classmethod
    def normalize_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [cls.normalize_column_name(c) for c in df.columns]
        return df

    @staticmethod
    def load_json(json_path: Path) -> dict:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def _strip_trailing_acronym(cls, s: str) -> str:
        """Strip a trailing token that looks like an acronym of the preceding words.

        E.g. 'BANQUE MAROCAINE POUR LE COMMERCE ET LINDUSTRIE BMCI' → 'BANQUE MAROCAINE POUR LE COMMERCE ET LINDUSTRIE'
        Only strips when ≥60% of the acronym's letters appear as word initials in the rest of the name.
        """
        tokens = s.split()
        if len(tokens) < 3:
            return s
        last = tokens[-1]
        if not cls._ACRONYM_RE.match(last):
            return s
        preceding = tokens[:-1]
        initials = {t[0] for t in preceding if t}
        if sum(1 for c in last if c in initials) / len(last) >= 0.6:
            return " ".join(preceding).strip()
        return s

    @staticmethod
    def build_reverse_map(normalized_dict: dict) -> dict:
        reverse_map = {}
        for group, names in normalized_dict.items():
            if not isinstance(names, list):
                continue
            group_clean = BankNameNormalizer.normalize_bank_name(group)
            if group_clean:
                reverse_map[group_clean] = group_clean
            for name in names:
                name_clean = BankNameNormalizer.normalize_bank_name(name)
                if name_clean:
                    reverse_map[name_clean] = group_clean
        return reverse_map

    @staticmethod
    def clean_text(name: str) -> str:
        if pd.isna(name) or not isinstance(name, str):
            return ""
        s = name.strip().upper()
        s = s.replace(",", "")
        s = s.replace('"', "").replace("'", "")
        s = re.sub(r"(?<=\w)\s*-\s*(?=\w)", " ", s)
        s = re.sub(r"\s{2,}", " ", s)
        s = re.sub(r"\s*,\s*", ", ", s)
        s = re.sub(r"\b([A-Z])\.\s+([A-Z])\b", r"\1\2", s)
        s = re.sub(r"\s*-\s*$", "", s)
        s = re.sub(r"\s*-\s*(?=\()", " ", s)
        s = re.sub(r"\s*\([^)]*\)", "", s)
        s = re.sub(r"\bN\s*\.?\s*A\s*\.?\b", "", s)
        s = re.sub(r"\bCO\.?\s*LTD\.?\b", "", s)
        s = re.sub(r"[.]", "", s)
        s = re.sub(r"\s*-\s*", " ", s)
        s = re.sub(r"\s*&\s*", " AND ", s)
        s = re.sub(r"\s{2,}", " ", s).strip()
        return s.strip()

    @classmethod
    def normalize_bank_name(cls, name: str) -> str:
        s = cls.clean_text(name)
        if not s:
            return ""
        if s in cls.PLACEHOLDERS:
            return ""
        s = re.sub(r"\bB\s*\.?\s*M\s*\.?\b", "", s)
        s = cls._CARD_TYPE_RE.sub("", s).strip(" -")
        s = re.sub(r"NATIONAL\s+ASSO\b(?!CIATION)", "NATIONAL ASSOCIATION", s)
        for pat in cls._SUFFIX_PATTERNS:
            s = pat.sub("", s).strip()
        s = cls._strip_trailing_acronym(s)
        s = re.sub(r"^THE\s+", "", s)
        s = re.sub(r",?\s+THE\s*$", "", s)
        s = re.sub(r"[.,;:\-]+$", "", s)
        s = re.sub(r"\s{2,}", " ", s)
        return s.strip()

    def _read_excel_with_detected_header(self) -> pd.DataFrame:
        raw = pd.read_excel(self.file_path, sheet_name=self.sheet_name, header=None)

        header_row_idx = None
        for i in range(len(raw)):
            row_values = [
                self.normalize_column_name(x) for x in raw.iloc[i].tolist()
                if pd.notna(x)
            ]
            if "card_bank" in row_values and "tier" in row_values:
                header_row_idx = i
                break

        if header_row_idx is None:
            raise ValueError(
                "Could not find header row with 'card_bank' and 'tier'. "
                "Check the Excel file."
            )

        df = pd.read_excel(
            self.file_path,
            sheet_name=self.sheet_name,
            header=header_row_idx,
        )
        df = self.normalize_columns(df)
        df = df.dropna(axis=1, how="all")
        df = df.loc[:, ~df.columns.str.startswith("unnamed")]
        return df

    def load(self):
        suffix = self.file_path.suffix.lower()

        if suffix == ".csv":
            self.df = pd.read_csv(self.file_path)
            self.df = self.normalize_columns(self.df)
        elif suffix in [".xlsx", ".xls"]:
            self.df = self._read_excel_with_detected_header()
        else:
            raise ValueError(f"Unsupported file format: {suffix}. Use .csv, .xlsx or .xls")

        self.normalized_dict = self.load_json(self.json_path)
        self.reverse_map = self.build_reverse_map(self.normalized_dict)

        if self.bank_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.bank_column}' not found. "
                f"Available columns: {list(self.df.columns)}"
            )
        return self

    def create_dynamic_bank_groups(self, series: pd.Series, initial_normalized_dict: dict, threshold: int):
        dynamic_reverse_map = self.build_reverse_map(initial_normalized_dict)
        final_mapping = {}

        unique_cleaned_names = series.dropna().unique()

        for cleaned_name in unique_cleaned_names:
            if not str(cleaned_name).strip():
                final_mapping[cleaned_name] = ("unknown", "unknown", "missing", 0.0)
                continue

            if cleaned_name in final_mapping:
                continue

            if cleaned_name in dynamic_reverse_map:
                group = dynamic_reverse_map[cleaned_name]
                canonical = cleaned_name
                match_type = "exact"
                score = 100.0
            else:
                current_choices = list(dynamic_reverse_map.keys())
                match = process.extractOne(
                    cleaned_name,
                    current_choices,
                    scorer=self.scorer,
                    score_cutoff=threshold,
                )

                if match:
                    matched_canonical_name, score, _ = match
                    group = dynamic_reverse_map[matched_canonical_name]
                    canonical = matched_canonical_name
                    match_type = "fuzzy"
                else:
                    group = cleaned_name
                    canonical = cleaned_name
                    match_type = "unmatched_new_group"
                    score = 0.0
                    dynamic_reverse_map[cleaned_name] = cleaned_name

            final_mapping[cleaned_name] = (group, canonical, match_type, score)

        mapped_results = series.apply(
            lambda x: ("unknown", "unknown", "missing", 0.0)
            if pd.isna(x)
            else final_mapping.get(x, (x, x, "unmatched_new_group", 0.0))
        )
        return mapped_results

    def transform(self) -> pd.DataFrame:
        if self.df is None:
            self.load()

        self.df["issuing_bank_clean"] = self.df[self.bank_column].apply(self.normalize_bank_name)

        self.df[
            ["bank_group", "canonical_bank_name", "match_type", "match_score"]
        ] = self.create_dynamic_bank_groups(
            self.df["issuing_bank_clean"],
            self.normalized_dict,
            self.fuzzy_threshold,
        ).apply(pd.Series)

        return self.df

    def get_grouped_bank_names(self) -> pd.DataFrame:
        if self.df is None or "bank_group" not in self.df.columns:
            self.transform()

        grouped = (
            self.df.groupby([self.bank_column, "bank_group"])
            .size()
            .reset_index(name="count")
            .sort_values(by="count", ascending=False)
            .reset_index(drop=True)
        )
        return grouped

    def fit_transform(self):
        df = self.transform()
        grouped = self.get_grouped_bank_names()
        return df, grouped


if __name__ == "__main__":
    

    normalizer = BankNameNormalizer(
        file_path="Mapping (1).xlsx",
        json_path="normalized_dict (1).json",
        bank_column="card_bank",
        fuzzy_threshold=95,
        sheet_name=0,
    )

    df_normalized, grouped_banks = normalizer.fit_transform()

    df_normalized.to_excel("normalized_bank_file.xlsx", index=False)
    grouped_banks.to_excel("grouped_bank_names.xlsx", index=False)

    print("Done.")
    print(df_normalized.head())
    print(grouped_banks.head())
