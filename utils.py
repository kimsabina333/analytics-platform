import os
import re
import jinja2
import yaml
import json
import pandas as pd
import numpy as np
import pymc as pm
from sklearn.preprocessing import OrdinalEncoder
import arviz as az
import matplotlib.pyplot as plt
from alert.banks_mapping import BankNameNormalizer


BG_COLOR = "#000000"
GRID_COLOR = "#222222"
TEXT_COLOR = "#FFFFFF"
FACT_COLOR = "#FF0000"
CI_COLOR = "#00FFFF" 
FORECAST_LINE = "#00FFFF"

DECLINE_CATEGORIES = [
    "INSUFFICIENT_FUNDS",
    "FRAUD_RISK",
    "DO_NOT_HONOR",
    "CARD_ISSUE",
    "BANK_DECLINE",
    "TECH_ERROR",
    "OTHER",
]


plt.rcParams.update({
    "figure.facecolor": BG_COLOR,
    "axes.facecolor": BG_COLOR,
    "axes.edgecolor": GRID_COLOR,
    "axes.labelcolor": TEXT_COLOR,
    "xtick.color": TEXT_COLOR,
    "ytick.color": TEXT_COLOR,
    "grid.color": GRID_COLOR,
    "text.color": TEXT_COLOR,
    "font.family": "monospace"
})

def make_plot(sr_dist, sr_fact, q=0.01, fig_path=None, save=True):
    fig, ax = plt.subplots(figsize=(12, 6))

    mean_vals = sr_dist.mean(axis=1).astype(float)
    q01 = np.quantile(sr_dist, q, axis=1).astype(float)
    q99 = np.quantile(sr_dist, 1-q, axis=1).astype(float)
    dates = pd.to_datetime(sr_fact.index)

    # динамический label для CI
    ci_label = f'CI {int((1 - 2*q) * 100)}%'  # q=0.05 → CI 90%, q=0.10 → CI 80%

    ax.fill_between(dates, q01, q99, color=CI_COLOR, alpha=0.2, label=ci_label)
    ax.plot(dates, mean_vals, color=FORECAST_LINE, linestyle='--', linewidth=1.5, label='Bayesian Mean')
    ax.plot(dates, sr_fact.values, color=FACT_COLOR, linewidth=2, marker='o', markersize=3, label='LIVE FACT')

    ax.set_title("SYSTEM MONITORING: SUCCESS RATE FIRST", color=FACT_COLOR, fontsize=16, fontweight='bold', pad=20)
    ax.grid(True, linestyle=':', alpha=1)
    ax.legend(facecolor=BG_COLOR, edgecolor=GRID_COLOR, loc='upper right', bbox_to_anchor=(1, 1), ncol=3)

    plt.tight_layout()
    if save == True: plt.savefig(fig_path)
    plt.close(fig)


    # query = """

dag_path = os.path.dirname(os.path.abspath(__file__))
sql_path = os.path.join(dag_path, 'sql_templates/')
banks_mapping_path = os.path.join(dag_path, 'banks_mapping.json')


class DataLoader:

    def __init__(self, sql_path=sql_path, mode='online'):
        self.sql_path = sql_path
        self.mode = mode
        
        tier_path = os.path.join(dag_path, 'idata/matched_groups_final.csv')
        tier_df = pd.read_csv(tier_path)
        self.bank_tier_map = dict(
            zip(
                tier_df['bank_group'].str.strip().str.upper(),
                tier_df['tier'].fillna(0.0).apply(
                    lambda x: 'T' + str(int(x)) if x > 0 else 'unknown'
                )
            )
        )
        # инициализируем нормализатор с fuzzy matching
        self.bank_normalizer = BankNameNormalizer(
            file_path=tier_path,
            json_path=os.path.join(dag_path, 'banks_mapping.json'),
            bank_column='bank_group',
            fuzzy_threshold=85)
        self.bank_normalizer.load()
        
        self.features = ['utm_source', 'geo', 'device', 'age', 'gender', 'payment_method', 
             'card_type', 'mid', 'offer', 'card_brand', 'weekday','bank_tier']
    
        
       

    def render_query(self, path=None):
        if path is None:
            path = self.sql_path
        with open(path+'sql_config.yaml', 'r') as f:
            config = yaml.safe_load(f)

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(path))
        
        template = env.get_template('main.sql')
        return template.render(
            mappings=config['mappings'],
        )
    
    def process(self, df):
        df['card_type'] = df.card_type.apply(self.process_card_type)
        df['device'] = df.device.str.lower().apply(self.process_device)
        df['mid'] = df.mid.apply(self.process_mid)
        df['age'] = df.age.apply(lambda x: '45+' if x == '45' else x)
        df['gender'] = df.gender.str.lower().apply(self.process_gender)
        df['date'] = pd.to_datetime(df.date)
        df['weekday'] = df.date.dt.day_name()
        bank_series = df['issuing_bank'].apply(BankNameNormalizer.normalize_bank_name)
        matched = self.bank_normalizer.create_dynamic_bank_groups(bank_series,
                                                                  self.bank_normalizer.normalized_dict,
                                                                  self.bank_normalizer.fuzzy_threshold)
        df['bank_tier'] = matched.apply(
            lambda x: self.bank_tier_map.get(x[0].upper(), 'unknown')
        )
        if 'decline_message' not in df.columns:
            df['decline_message'] = ''
        df['decline_category'] = df.apply(
            lambda row: self.categorize_decline(row.get('decline_message'))
            if row.get('status') != 1 else None,
            axis=1,
        )
        decline_cols = []
        for category in DECLINE_CATEGORIES:
            col = f"decline_{category.lower()}"
            decline_cols.append(col)
            df[col] = (
                (df['status'] != 1) & (df['decline_category'] == category)
            ).astype(int)

        df.dropna(subset=['customer_account_id', 'status', 'date'] + self.features, inplace=True)
        df = df.sort_values(['customer_account_id', 'status']).drop_duplicates(
            subset=['customer_account_id'], 
            keep='last'
            )
        agg_spec = {
            'success': ('status', 'sum'),
            'cnt': ('status', 'count'),
            **{col: (col, 'sum') for col in decline_cols},
        }
        df = df.groupby(['date']+self.features, as_index=False).agg(**agg_spec).sort_values(['date', 'cnt'])
        return df
    
    def __call__(self, client, *args, **kwds):
        query = self.render_query()
        if self.mode == 'offline':
            df = client.query(query).to_dataframe()
        else:
            df = client.get_records_as_df(sql=query)
        print('Data loaded, processing...', df.head())
        df = self.process(df)
        return df

    def process_device(self, x):
        if x is None: return 'unknown'
        if 'android' in x or 'windows phone' in x:
            return 'android'
        elif 'ipad' in x or 'iphone' in x: return 'ios'
        elif 'macintosh' in x: return 'mac'
        elif 'windows nt' in x: return 'windows'
        elif 'linux' in x: return 'linux'
        elif 'x11' in x: return 'x11' 
        else: return 'other'
    
    def process_card_type(self, x):
        if x is None:
            return 'unknown'
        if x == 'DEFERRED DEBIT':
            return 'DEFFERED_DEBIT'.lower()
        elif x == 'CREDIT/DEBIT':
            return 'credit'
        else:
            return x.lower()
    
    def process_mid(self, x):
        if x in ['adyen_us', 'checkout', 'adyen', 'adyen US', 'paypal', 'esquire','airwallex']:
            return x
        else: 
            return np.nan

    def _get_bank_tier(self, bank_name: str) -> str:
        if not isinstance(bank_name, str) or not bank_name.strip():
            return 'unknown'
        normalized = BankNameNormalizer.normalize_bank_name(bank_name)
        if not normalized:
            return 'unknown'
        return self.bank_tier_map.get(normalized.upper(), 'unknown')
    
    def process_gender(self, x):
        if x in ('male', 'female', 'non-binary'): return x
        elif x in ('female →', 'male →'): return x.split(' ')[0]
        elif x == 'prefer not to say' or x == 'prefiero no especificar': return 'unknown'
        elif x == 'mujer' or x == 'hombre': return 'male'
        elif x == 'no binario': return 'non-binary'
        else: return 'unknown'

    def normalize_decline_message(self, msg):
        if not isinstance(msg, str):
            return ''
        msg = re.sub(r'^\d+\s*:\s*', '', msg.strip().lower())
        return re.sub(r'[^a-z0-9\s]', '', msg).strip()

    def categorize_decline(self, msg):
        normalized = self.normalize_decline_message(msg)
        if any(token in normalized for token in (
            'insufficient', 'not enough', 'no funds', 'low balance',
            'exceeds limit', 'limit exceeded',
        )):
            return 'INSUFFICIENT_FUNDS'
        if any(token in normalized for token in (
            'fraud', 'security', 'suspected', 'risk', 'blocked',
        )):
            return 'FRAUD_RISK'
        if 'do not honor' in normalized:
            return 'DO_NOT_HONOR'
        if any(token in normalized for token in (
            'expired', 'invalid card', 'restricted', 'lost', 'stolen',
        )):
            return 'CARD_ISSUE'
        if 'issuer' in normalized or 'bank declined' in normalized:
            return 'BANK_DECLINE'
        if any(token in normalized for token in ('timeout', 'error', 'technical')):
            return 'TECH_ERROR'
        return 'OTHER'


     
class ModelLoader:
    def __init__(self, coords, model_path='idata/idata_sr_ts.nc'):

        if 'obs_id' not in coords or 'feature_dim' not in coords:
            raise ValueError("Coords must contain 'obs_id' and 'feature_dim' keys.")
        
        self.coords = coords
        self.model_path = model_path
        with pm.Model(coords=coords) as self.sr_model:
            covariates = pm.Data('covariates', np.zeros((len(coords['obs_id']), len(coords['feature_dim']))).astype(int), dims=('obs_id', 'feature_dim'))
            success = pm.Data('success', np.zeros(len(coords['obs_id'])).astype(int), dims=('obs_id',))
            cnt = pm.Data('cnt', np.zeros(len(coords['obs_id'])).astype(int),  dims=('obs_id',))


            bias = pm.Normal('bias', mu=0, sigma=0.2)

            effects = []
            for i, col in enumerate(coords['feature_dim']):

                std = pm.HalfNormal(f'{col}_sigma', sigma = 0.2)
                group_std = pm.HalfNormal(f'{col}_group_std', sigma=0.2)

                avg = pm.Normal(f'{col}_avg', mu=0, sigma=1)*std
                coefs = pm.Normal(f'{col}_group_avg', mu=0, sigma=1, dims=col)*group_std + (avg + 0.05) 

                effects.append( coefs[covariates[:, i]] )
                
            prob = pm.math.sigmoid(bias + pm.math.sum(effects, axis=0))

            pm.Binomial("Likelihood", p=prob, n=cnt, observed=success, dims=("obs_id",))
        
        self.idata = az.from_netcdf(os.path.join(dag_path, model_path), engine='netcdf4')

    def __call__(self, cnt, covariates, *args, **kwds):
        
        with self.sr_model:
            pm.set_data(
                new_data={
                    "covariates": covariates.astype(int), 
                    "cnt": cnt.astype(int),
                    "success": np.zeros_like(cnt).astype(int)
                    },
                coords = {"obs_id": np.arange(cnt.shape[0])}
                )
            
            post_pred = pm.sample_posterior_predictive(self.idata, var_names=["Likelihood"], progressbar=False)
        return post_pred.posterior_predictive['Likelihood'].stack(sample=('chain', 'draw')).values.T
