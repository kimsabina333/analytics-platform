import os

import torch
from torch.utils.data import Dataset

import pandas as pd
import numpy as np
import scipy.stats as sps
import json

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

import jinja2
import yaml

dag_path = os.path.dirname(os.path.abspath(__file__))
sql_path = os.path.join(dag_path, 'sql_templates/')

import re
from pathlib import Path
try:
    from rapidfuzz import process, fuzz
except ImportError:
    raise ImportError(
        "Установи rapidfuzz: pip install rapidfuzz"
    )

json_path = Path(__file__).parent / "normalized_dict.json"

mappers_path = Path(__file__).parent / "processing_mappers.json"

class BankNameNormalizer:
    PLACEHOLDERS = {"UNKNOWN", "NA", "N/A", "NONE", "NULL", "-", ""}

    _CARD_TYPE_RE = re.compile(
        r"\s*-?\s*(PREPAID\s+DEBIT|CONSUMER\s+DEBIT|CONSUMER\s+CREDIT"
        r"|COMMERCIAL\s+CREDIT|COMMERCIAL\s+DEBIT|DEBIT|CREDIT|PREPAID)\s*$"
    )

    _LEGAL_SUFFIXES = [
       # Multi-word / long forms
    r",?\s+NATIONAL\s+ASSOCIATION",
    r",?\s+CLOSED\s+JOINT[\s-]*STOCK\s+COMPANY",
    r",?\s+OPEN\s+JOINT[\s-]*STOCK\s+COMPANY",
    r",?\s+JOINT[\s-]+STOCK\s+COMPANY",
    r",?\s+PUBLIC\s+LIMITED\s+COMPANY",
    r",?\s+PUBLIC\s+COMPANY\s+LIMITED",
    r",?\s+SHARE\s+COMPANY",

    # International compound forms
    r",?\s+S\.?A\.?\s+DE\s+C\.?V\.?\s*,?\s*SOFOM\s+E\.?R\.?",
    r",?\s+S\.?A\.?\s+DE\s+C\.?V\.?",
    r",?\s+SA\s+DE\s+CV\s*,?\s*SOFOM\s+E\.?R\.?",
    r",?\s+SA\s+DE\s+CV",

    # Common corporate suffixes
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

    # CIS / Eastern Europe
    r",?\s+P\.?J\.?S\.?C\.?",
    r",?\s+C\.?J\.?S\.?C\.?",
    r",?\s+O\.?J\.?S\.?C\.?",
    r",?\s+J\.?S\.?C\.?",
    r",?\s+A\.?O\.?",
    r",?\s+TOO",
    r",?\s+SHA\.?",

    # Europe
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

    # US specific
    r",?\s+N\.?A\.?",
    r",?\s+F\.?S\.?B\.?",
    r",?\s+S\.?S\.?B\.?",

    # AU / Asia
    r",?\s+PTY\.?",
    r",?\s+PTE\.?",

    # Other
    r",?\s+C\s*\.?\s*R\s*\.?\s*L\s*\.?",
    r",?\s+B\.?\s*M\.?",
    ]
    _SUFFIX_PATTERNS = [re.compile(p + r"\s*$", re.IGNORECASE) for p in _LEGAL_SUFFIXES]

    def __init__(
        self,
        df: pd.DataFrame,
        json_path: str = json_path,
        bank_column: str = "issuing_bank",
        fuzzy_threshold: int = 90,
        scorer=None,
    ):
        self.df = df
        self.json_path = Path(json_path)
        self.bank_column = bank_column
        self.fuzzy_threshold = fuzzy_threshold
        self.scorer = scorer or fuzz.token_sort_ratio

        # self.df = None
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

        # remove card type
        s = re.sub(r"\bB\s*\.?\s*M\s*\.?\b", "", s)
        s = cls._CARD_TYPE_RE.sub("", s).strip(" -")

        # fix truncated NATIONAL ASSO
        s = re.sub(r"NATIONAL\s+ASSO\b(?!CIATION)", "NATIONAL ASSOCIATION", s)

        # remove legal suffixes
        for pat in cls._SUFFIX_PATTERNS:
            s = pat.sub("", s).strip()

        # remove THE
        s = re.sub(r"^THE\s+", "", s)
        s = re.sub(r",?\s+THE\s*$", "", s)

        # final cleanup
        s = re.sub(r"[.,;:\-]+$", "", s)
        s = re.sub(r"\s{2,}", " ", s)

        return s.strip()

    def load(self):
        # self.df = pd.read_csv(self.csv_path)
        self.df = self.normalize_columns(self.df)

        self.normalized_dict = self.load_json(self.json_path)
        self.reverse_map = self.build_reverse_map(self.normalized_dict)

        if self.bank_column not in self.df.columns:
            raise ValueError(
                f"Колонка '{self.bank_column}' не найдена. "
                f"Доступные колонки: {list(self.df.columns)}"
            )

        return self

    def create_dynamic_bank_groups(self, series: pd.Series, initial_normalized_dict: dict, threshold: int):
        # Start with the initial mapping
        dynamic_reverse_map = self.build_reverse_map(initial_normalized_dict)

        # Dictionary to store the final mapping for each unique cleaned name
        final_mapping = {}

        unique_cleaned_names = series.dropna().unique()

        for cleaned_name in unique_cleaned_names:
            # Handle empty strings specifically (these result from original 'Unknown' or NaN after cleaning)
            if not cleaned_name.strip(): # Handles ""
                final_mapping[cleaned_name] = ("unknown", "unknown", "missing", 0.0)
                continue

            # Check if we've already processed this cleaned_name
            if cleaned_name in final_mapping:
                continue

            # 1. Exact match against the dynamic reverse map (includes original and new canonical names)
            if cleaned_name in dynamic_reverse_map:
                group = dynamic_reverse_map[cleaned_name]
                canonical = cleaned_name
                match_type = "exact"
                score = 100.0
            else:
                # 2. Fuzzy match against current choices (includes original and new canonical names)
                current_choices = list(dynamic_reverse_map.keys())
                match = process.extractOne(
                    cleaned_name,
                    current_choices,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=threshold,
                )

                if match:
                    matched_canonical_name, score, _ = match
                    group = dynamic_reverse_map[matched_canonical_name]
                    canonical = matched_canonical_name
                    match_type = "fuzzy"
                else:
                    # 3. No match: create a new group for this cleaned_name
                    group = cleaned_name
                    canonical = cleaned_name
                    match_type = "unmatched_new_group"
                    score = 0.0
                    # Add this new canonical name to the dynamic reverse map for future checks
                    dynamic_reverse_map[cleaned_name] = cleaned_name

            # Store the result for this specific cleaned_name
            final_mapping[cleaned_name] = (group, canonical, match_type, score)

        # Apply the final mapping to the original series
        # Handle NaN values explicitly: if pd.isna(x) is true, return the desired tuple
        # Otherwise, get the mapping from final_mapping.
        mapped_results = series.apply(
            lambda x: ("unknown", "unknown", "missing", 0.0) if pd.isna(x) else final_mapping.get(x, (x, x, "unmatched_new_group", 0.0))
        )

        return mapped_results

    def transform(self) -> pd.DataFrame:
        self.load()

        self.df["issuing_bank_clean"] = self.df[self.bank_column].apply(self.normalize_bank_name)

        dynamic_bank_groups = self.create_dynamic_bank_groups(self.df["issuing_bank_clean"], self.normalized_dict, self.fuzzy_threshold).apply(pd.Series)
        dynamic_bank_groups.rename(columns={0: "bank_group", 1: "canonical_bank_name", 2: "match_type", 3: "match_score"}, inplace=True)
        return dynamic_bank_groups

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

    def fit_transform(self, get_bank_names=False):
        dynamic_bank_groups = self.transform()
        if get_bank_names:
            grouped = self.get_grouped_bank_names()
            return dynamic_bank_groups, grouped
        return dynamic_bank_groups

class LTVForecaster:
    # Class for LTV Forecasting
    def __init__(self, task_type='fast', mode='online', training=False, 
                 sql_path=sql_path):
        self.task_type = task_type # 'extended' or 'fast'
        self.mode = mode
        self.time_filter = None
        self.training = training
        self.sql_path = sql_path
        # self.pop_countries = ['US', 'AU', 'GB', 'AE', 'CA', 'NZ', 'SG']

        
        self.feature_cols = ['offer',
                            'channel',
                            'utm_source',
                            # 'utm_keyword',
                            'gender',
                            'age',
                            'device',
                            'payment_method',
                            'first_amount',
                            'geo',
                            # 'geo_gdp',
                            'issuing_bank',
                            'upsell_amount',
                            'card_type',
                            'card_brand'
                            ]
        with  open(mappers_path) as f: 
            self.mappers = json.load(f)

        if task_type != 'fast': self.feature_cols.extend(['session_time_3d_min'])


    def load_data(self, bq_hook, start_date=None, end_date=None):

        # if self.task_type == 'fast':
        #     query = self.get_query_fast(start_date, end_date)
        # else:
        #     query = self.get_query_extended(start_date, end_date)

        self.time_filter = self.build_time_filter(start_date, end_date)
        query = self.generate_final_query()
        if self.mode == 'online':
            df = bq_hook.get_records_as_df(sql=query)
        else:
            df = bq_hook.query(query).to_dataframe()
        print('Dataset is loaded')
        print('Shape:', df.shape, ', Task type:', self.task_type)
        return df

    def fit(self, model, bq_hook, start_date, end_date, split_date, directory_path, 
            model_path='bdw_weights.pth', config_path='config.joblib'):
        df = self.load_data(bq_hook, start_date, end_date)

        with open('gender_dict.json') as f:
            gender_dict = json.load(f)

        with open('age_dict.json') as f:
            age_dict = json.load(f)

        df['age'] = df.apply(self.assign_age, axis=1, args=(age_dict, ))
        df['gender'] = df.apply(self.assign_gender, axis=1, args=(gender_dict, ))
        
        df = self.preprocess(df)

        print(f"Nan observations: ", df[self.feature_cols].isna().sum())
        print(f"Total observations: ", len(df))
        print(f"Last subscription cohort date: ", df.subscription_cohort_date.max())

        df.dropna(inplace=True)

        if self.mode == 'online':
            df_amount = bq_hook.get_records_as_df(sql=self.query_amount())
        else:
            df_amount = bq_hook.query(self.query_amount()).to_dataframe()

        df_amount = df_amount.sort_values(['customer_account_id', 'paid_count'])

        df_amount.set_index('customer_account_id', inplace=True)

        train_mask = (df.subscription_cohort_date < split_date)
        mask = ~((df.churned==0)&(df.paid_count==0))
        C = 1- df.churned
        T = df.paid_count
        X = df[self.feature_cols]

   
        self.X_train, self.X_test = X[train_mask & mask], X[(~train_mask)& mask]
        self.T_train, self.T_test = T[train_mask & mask], T[(~train_mask)& mask]
        self.C_train, self.C_test = C[train_mask & mask], C[(~train_mask)& mask]

        amount_mask = df_amount.index.isin(self.X_train.index)
        payment_data  = df_amount[amount_mask][['paid_count','amount']]

        model.fit(self.X_train, self.T_train, self.C_train, payment_data, self.X_test, self.T_test, self.C_test)
        if (directory_path is not None) & (model_path is not None) &  (config_path is not None):
            model.save(directory_path=directory_path, model_path=model_path, config_path=config_path)
        
        pc_mask = X.offer.apply(lambda x: 12 if x!='12Week' else 4).to_numpy()
        df_final = model.ltv_calc(X, pc_mask)
        df_final['arppu'] = df_final['first_amount'] + df_final['upsell_amount']
        df_final['ltv'] = df_final['arppu'] + df_final['ltv_recurring']
        join_mask = ['subscription_cohort_date', 'churned','paid_count']
        df_final = df_final.merge(df[join_mask], on='customer_account_id', how='inner')
        return df_final


    def forecast(self, model, bq_hook, directory_path, model_path='bdw_weights.pth', config_path='config.joblib', start_date=None, end_date=None):

        df = self.load_data(bq_hook, start_date=start_date, end_date=end_date)

        df = self.preprocess(df)
        
        # log data with nan values to prevent errors
        print(f"Nan observations: ", df[self.feature_cols].isna().sum())
        print(f"Total observations: ", len(df))

        print(f"Last subscription cohort date: ", df.subscription_cohort_date.max())

        df.dropna(inplace=True)

        X = df[self.feature_cols]


        pc_mask = X.offer.apply(lambda x: 12 if x!='12Week' else 4).to_numpy()

        model = model.load(directory_path=directory_path, model_path=model_path, config_path=config_path)

        df_final = model.ltv_calc(X, pc_mask)

        df_final['arppu'] = df_final['first_amount'] + df_final['upsell_amount']
        df_final['ltv'] = df_final['arppu'] + df_final['ltv_recurring']

        join_mask = ['subscription_cohort_date', 'churned', 'paid_count']

        df_final = df_final.merge(df[join_mask], on='customer_account_id', how='inner')

        return df_final

    def preprocess(self, df):

        self.bank_normalizer = BankNameNormalizer(df=df[['issuing_bank']], json_path=json_path)

        df['age'] = df.apply(self.process_age, axis=1)

        df['gender'] = df.apply(self.process_gender, axis=1)

        df['channel'] = df.apply(self.process_channel, axis=1)
        df['utm_source'] = df.apply(self.process_utm_source, axis=1)
        # df['geo_gdp'] = df['geo_country'].apply(self.process_geo_gdp)
        df['geo'] = df.apply(self.process_geo, axis=1)

        bank_matching = self.bank_normalizer.fit_transform()
        df[bank_matching.columns] = bank_matching.to_numpy()
        df['issuing_bank'] = df.apply(self.process_issuing_bank, axis=1)

        df['card_type'] = df['card_type'].apply(self.process_card_type)
        # df['utm_keyword'] = df['utm_keyword'].apply(self.process_keywords)
        if self.task_type != 'fast':
            df['session_time_3d_min'] = df['session_time_3d_min'].apply(self.process_session_time)
        df = df.sort_values(['customer_account_id', 'subscription_cohort_date']).drop_duplicates('customer_account_id', keep='last')
        df.set_index('customer_account_id', inplace=True)
        return df 
    
    def process_gender(self, x):
        if x.gender in ('Male →', 'Female →'):
            return x.gender.split(' ')[0].lower()
        if x.gender in ('Hombre', 'Homme', 'Männlich', 'Uomo', 'Masculino'):
            return 'male'
        if x.gender in ('Mujer', 'Femme', 'Weiblich', 'Donna', 'Feminino'):
            return 'female'
        if x.gender == 'No binario':
            return 'non-binary'
        if x.gender  in ("I'd rather skip this one", "Prefer not to say", "Prefiero no especificar"):
            return 'prefer not to say'
        if x.gender in ('yes', 'other', 'no'):
            return 'none'
        return x.gender.lower()

    def process_age(self, x):
        if x.age in ('45', '45+'):
            return '45+'
        elif x.age == '36-45':
            return '35-44'
        elif x.age == '18-25':
            return '18-24'
        elif x.age == '26-35':
            return '25-34'
        return x.age

    def process_channel(self, x):
        if x.mid is None or pd.isna(x.mid): return 'unknown'
        elif x.mid == 'esquire': return np.nan
        return x.mid

    # def process_geo_gdp(self, x):
    #     if x in self.geo_dict:
    #         return np.log1p(self.geo_dict[x])
    #     else:
    #         return np.nan
    
    def process_geo(self, x):
        return x.geo_country if x.geo_country in self.mappers['geo_country'] else x.geo

    def process_utm_source(self, x):
        return x.utm_source if x.utm_source!='adq' else np.nan
    
    def process_issuing_bank(self, x):
        return x.bank_group if x.bank_group in self.mappers['bank_group'] else self.mappers['tier'].get(x.bank_group, 'unknown')
    
    def process_card_type(self, x):
        if x == 'DEFFERED_DEBIT':
            return 'deferred debit'
        elif x == 'CREDIT/DEBIT':
            return 'credit'
        elif str(x).lower() == 'none' or pd.isna(x):
            return 'unknown'
        return str(x).lower()

    def process_session_time(self, x):
        return np.log1p(x)
    
    # def process_keywords(self, keyword):
    #     kw = str(keyword).lower().strip()
        
    #     # 1. Brand (Самый высокий приоритет)
    #     if any(x in kw for x in ['jobescape', 'jobscape', 'job escape']):
    #         return 'Brand'
        
    #     # 2. AI Tools & Income
    #     if any(x in kw for x in ['ai ', ' ai', 'gpt', 'automation', 'artificial intelligence']):
    #         return 'AI & Automation'
        
    #     # 3. Freelance Platforms
    #     if any(x in kw for x in ['upwork', 'fiverr', 'freelanc', 'textbroker', 'snagajob']):
    #         return 'Freelance Platforms'
        
    #     # 4. Specific Skills (Professional)
    #     if any(x in kw for x in ['marketing', 'copywriting', 'dropshipping', 'design', 'typing', 'data entry', 'coding']):
    #         return 'Professional Skills'
        
    #     # 5. Micro-tasks & Surveys
    #     if any(x in kw for x in ['survey', 'swagbucks', 'clickworker', 'inboxdollars', 'mypoints', 'watch videos', 'games']):
    #         return 'Micro-tasks & Rewards'
        
    #     # 6. Remote / Work from Home (General)
    #     if any(x in kw for x in ['remote', 'work from home', 'wfh', 'at home jobs', 'online jobs']):
    #         return 'Remote Work'
        
    #     # 7. Make Money Online (Broad)
    #     if any(x in kw for x in ['make money', 'money', 'earn', 'hustle', 'income', 'cash', 'rich', 'verdienen']):
    #         return 'General Earning'
        
    #     return 'Other / Unclassified'
    
    def assign_gender(self, x, gender_dict):
        if x.gender == 'None':
            if x.customer_account_id in gender_dict:
                return gender_dict[x.customer_account_id]['gender']
        return x.gender
    
    def assign_age(self, x, age_dict):
        if x.age == 'None':
            if x.customer_account_id in age_dict:
                return age_dict[x.customer_account_id]['age']
        return x.age
    
    def build_time_filter(self, start_date=None, end_date=None):
        if start_date is not None and end_date is not None:
            time_filter = f"timestamp>='{start_date}' and timestamp < '{end_date}'"
        elif start_date is not None:
            time_filter = f"timestamp>='{start_date}'"
        elif end_date is not None:
            time_filter = f"timestamp<'{end_date}'"
        else:
            time_filter = f'timestamp >= timestamp_sub(current_timestamp(), interval 6 day)'
        return time_filter
    
    
    def generate_final_query(self, path=None):
        if path is None:
            path = self.sql_path
        with open(path+'sql_config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(path))
        
        template = env.get_template('main.sql')
        return template.render(
            mappings=config['mappings'],
            is_training=self.training,
            time_filter=self.time_filter,
            task_type=self.task_type
        )
    
    def query_amount(self):
        query = """
        select 
            customer_account_id,
            rebill_count as paid_count,
            avg(amount * exchange_rate/ 100) as amount,
        from `payments.all_payments_prod` as app
        left join `analytics_draft.exchange_rate` as fx
        on date(timestamp_micros(app.created_at)) = fx.date
        and app.currency = fx.currency
        where 1=1
        and status='settled'
        and payment_type = 'recurring'
        and amount is not null
        and rebill_count is not null
        group by 1, 2
        """
        return query


class SurvivalDataset(Dataset):
    """
    Custom dataset for survival analysis. This class is designed to hold the features (X),
    survival times (T), and censoring information (C) in a format compatible with PyTorch.

    Parameters
    ----------
    X : pd.DataFrame or np.ndarray
        The input feature matrix containing the covariates (independent variables) for each observation.

    T : np.ndarray or list
        The survival times for each observation. These represent the time until the event or censoring occurs.

    C : np.ndarray or list
        The censoring indicators for each observation. A value of 1 indicates that the event occurred, 
        while 0 indicates that the observation was censored.

    Attributes
    ----------
    X : torch.Tensor
        The features of the dataset as a tensor of type `float32`.

    T : torch.Tensor
        The survival times as a tensor of type `float32`.

    c : torch.Tensor
        The censoring indicators as a tensor of type `float32`.

    Methods
    -------
    __len__() :
        Returns the number of samples in the dataset.

    __getitem__(idx) :
        Returns the features, survival time, and censoring indicator for a given index.
    """

    def __init__(self, X, T=None, C=None):
        super().__init__()

        X = X.to_numpy()

        self.X = torch.tensor(X).to(torch.float32)
        if (T is not None) and (C is not None):
            self.T = torch.tensor(T).to(torch.float32)
            self.C = torch.tensor(C).to(torch.float32)
        else:
            self.T, self.C = None, None

    def __len__(self):
        """
        Returns the number of samples in the dataset.

        Returns
        -------
        int
            The total number of samples in the dataset.
        """
        return len(self.X)

    def __getitem__(self, idx):
        """
        Fetches a single sample from the dataset.

        Parameters
        ----------
        idx : int
            The index of the sample to retrieve.

        Returns
        -------
        tuple
            A tuple containing the features (X), survival time (T),
            and censoring indicator (C) for the given index.
        """
        if (self.T is not None) and (self.C is not None):
            return self.X[idx], self.T[idx], self.C[idx]
        else:
            return self.X[idx]


def log_beta(alpha, beta):
    """
    Computes the log of the Beta function using the gamma function
    """
    return torch.lgamma(alpha) + torch.lgamma(beta) - torch.lgamma(alpha + beta)

# Class for handling survival data


class SurvivalDataset(Dataset):
    """
    Custom dataset for survival analysis. This class is designed to hold the features (X),
    survival times (T), and censoring information (C) in a format compatible with PyTorch.

    Parameters
    ----------
    X : pd.DataFrame or np.ndarray
        The input feature matrix containing the covariates (independent variables) for each observation.

    T : np.ndarray or list
        The survival times for each observation. These represent the time until the event or censoring occurs.

    C : np.ndarray or list
        The censoring indicators for each observation. A value of 1 indicates that the event occurred, 
        while 0 indicates that the observation was censored.

    Attributes
    ----------
    X : torch.Tensor
        The features of the dataset as a tensor of type `float32`.

    T : torch.Tensor
        The survival times as a tensor of type `float32`.

    c : torch.Tensor
        The censoring indicators as a tensor of type `float32`.

    Methods
    -------
    __len__() :
        Returns the number of samples in the dataset.

    __getitem__(idx) :
        Returns the features, survival time, and censoring indicator for a given index.
    """

    def __init__(self, X, T=None, C=None):
        super().__init__()

        X = X.to_numpy()

        self.X = torch.tensor(X).to(torch.float32)
        if (T is not None) and (C is not None):
            self.T = torch.tensor(T).to(torch.float32)
            self.C = torch.tensor(C).to(torch.float32)
        else:
            self.T, self.C = None, None

    def __len__(self):
        """
        Returns the number of samples in the dataset.

        Returns
        -------
        int
            The total number of samples in the dataset.
        """
        return len(self.X)

    def __getitem__(self, idx):
        """
        Fetches a single sample from the dataset.

        Parameters
        ----------
        idx : int
            The index of the sample to retrieve.

        Returns
        -------
        tuple
            A tuple containing the features (X), survival time (T),
            and censoring indicator (C) for the given index.
        """
        if (self.T is not None) and (self.C is not None):
            return self.X[idx], self.T[idx], self.C[idx]
        else:
            return self.X[idx]

