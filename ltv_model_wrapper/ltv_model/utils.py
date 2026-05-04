import torch
from torch.utils.data import Dataset

import pandas as pd
import numpy as np
import scipy.stats as sps

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator


from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


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


class KaplanMeierEstimator:
    def __init__(self, alpha=0.05, n_periods=6):
        self.alpha = alpha
        self.n_periods = n_periods

    def fit(self, T, C):

        z = sps.norm.ppf(1-self.alpha/2)

        times = np.arange(0, self.n_periods)

        masks = [~((T == t) & (C == 1)) for t in times]
        d = np.array([((T == t) & masks[t]).sum() for t in times])
        n = np.array([((T >= t) & masks[t]).sum() for t in times])

        self.c = np.array([1-d[k]/n[k] for k in times])
        self.S = np.cumprod([self.c[k] for k in times])
        self.h = 1-self.c

        var_log_p = [d[k]/(n[k]*(n[k]-d[k])) for k in times]

        V = np.sqrt(np.cumsum(var_log_p))/np.log(self.S)

        log_log_S = np.log(-np.log(self.S))

        self.ci_survival = np.exp(-np.exp(log_log_S + z*V)
                                  ), np.exp(-np.exp(log_log_S - z*V))
        self.ci_c = self.c-z * \
            np.sqrt(self.c*(1-self.c)/n), self.c+z*np.sqrt(self.c*(1-self.c)/n)
        self.ci_h = self.h-z * \
            np.sqrt(self.h*(1-self.h)/n), self.h+z*np.sqrt(self.h*(1-self.h)/n)
        return self

    def plot_summary(self, h_size=18, w_size=5, tick_size=0.05):

        fig, axs = plt.subplots(1, 3, figsize=(h_size, w_size))

        x = np.arange(self.n_periods)

        xticks_labels_c = [f'c{i}{i+1}' for i in range(self.n_periods)]
        xticks_labels_h = [f'h({i})' for i in range(self.n_periods)]
        xticks_labels_surv = [
            f'S({i})' if i < 10 else f'c{i}' for i in range(self.n_periods)]

        # Survival plot
        axs[0].plot(self.S, color='k', linewidth=1, label='Survival S(t)')
        axs[0].fill_between(x, self.ci_survival[0],
                            self.ci_survival[1], color='royalblue', alpha=0.5)
        axs[0].set_title('Survival Function')
        axs[0].set_xlabel('Time intervals')
        axs[0].set_ylabel('Survival Probability')
        axs[0].set_xticks(x)
        axs[0].set_xticklabels(xticks_labels_surv, rotation=45)
        axs[0].yaxis.set_major_locator(MultipleLocator(tick_size))
        axs[0].grid(True)

        # Hazard plot
        axs[1].plot(self.h, color='k', linewidth=1, label='Hazard h(t)')
        axs[1].fill_between(x, self.ci_h[0], self.ci_h[1],
                            color='royalblue', alpha=0.5)
        axs[1].set_title('Hazard Function')
        axs[1].set_xlabel('Time intervals')
        axs[1].set_ylabel('Probability')
        axs[1].set_xticks(x)
        axs[1].set_xticklabels(xticks_labels_h, rotation=45)
        axs[1].yaxis.set_major_locator(MultipleLocator(tick_size))
        axs[1].grid(True)

        # Inverse Hazard plot
        axs[2].plot(self.c, color='k', linewidth=1,
                    label='Inverse Hazard c(t) = 1 - h(t)')
        axs[2].fill_between(x, self.ci_c[0], self.ci_c[1],
                            color='royalblue', alpha=0.5)
        axs[2].set_title('Inverse Hazard Function')
        axs[2].set_xlabel('Time intervals')
        axs[2].set_ylabel('Probability')
        axs[2].set_xticks(x)
        axs[2].set_xticklabels(xticks_labels_c, rotation=45)
        axs[2].yaxis.set_major_locator(MultipleLocator(tick_size))
        axs[2].grid(True)

        plt.tight_layout()
        plt.show()


def Preprocessor(X_train, X_test, cat_features, num_features):

    cat_pipeline = Pipeline(steps=[
        ('ohe', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=False))
    ])

    num_pipeline = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='mean'))
    ])

    column_transformer = ColumnTransformer(transformers=[
        ('cat', cat_pipeline, cat_features),
        ('num', num_pipeline, num_features)
    ])

    preprocessor = Pipeline(steps=[
        ('transformers', column_transformer),
        ('scaler', StandardScaler())
    ])
    preprocessor.fit(X_train)

    features_train = pd.DataFrame(
        preprocessor.transform(X_train),
        columns=preprocessor.get_feature_names_out(),
        index=X_train.index)

    features_test = pd.DataFrame(
        preprocessor.transform(X_test),
        columns=preprocessor.get_feature_names_out(),
        index=X_test.index)
    return features_train, features_test


class KaplanMeierFitter:
    def __init__(self, ):
        pass

    def fit(self, X_train, T_train, C_train):
        X = X_train.copy()
        X['paid_count'] = T_train
        X['censor'] = C_train
        self.sf = X.groupby(['offer', 'geo', 'utm_source', 'payment_method'], as_index=False).apply(
            self.apply_KaplanMeier).replace(0, np.nan)

        return self

    def apply_KaplanMeier(self, x):
        km = KaplanMeierEstimator()
        km.fit(x.paid_count, x.censor)
        return pd.Series(km.S, index=[f'S({i})' for i in range(len(km.S))])

    def predict(self, X_test):
        return X_test.merge(self.sf, on=['offer', 'geo', 'utm_source', 'payment_method'], how='left')[[f'S({i})' for i in range(6)]]
