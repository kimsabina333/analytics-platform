import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import torch
from torch.utils.data import DataLoader

from scipy.stats import norm
import matplotlib.pyplot as plt

from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    accuracy_score, f1_score, confusion_matrix, classification_report,
    roc_auc_score, brier_score_loss
)

from IPython.display import clear_output
from tqdm import tqdm
import joblib


from ltv_model.utils import *
from ltv_model.prior_model import *

from typing import Optional, Union


def BdWLoss(alpha, beta, t, c, gamma, eps=1e-6):
    """
    Compute the negative log-likelihood loss for the Beta-Discrete-Weibull (BdW) survival model.

    This loss function handles both censored and uncensored survival data. It computes the
    likelihood based on a Beta-Discrete-Weibull distribution, where the survival function is
    modeled using the beta function evaluated at transformed time points. The final loss is
    the negative mean log-likelihood, optionally weighted.

    Parameters
    ----------
    alpha : torch.Tensor
        First shape parameter of the Beta distribution. Shape: (batch_size,).
    beta : torch.Tensor
        Second shape parameter of the Beta distribution. Shape: (batch_size,).
    t : torch.Tensor
        Discrete time to event or censoring. Shape: (batch_size,).
    c : torch.Tensor
        Censoring indicator (0 = uncensored, 1 = right-censored). Shape: (batch_size,).
    gamma : torch.Tensor, optional
        Shape parameter of the Weibull transformation. Default is `torch.tensor([1.])`.
    eps : float, optional
        Small constant for numerical stability. Default is 1e-6.

    Returns
    -------
    torch.Tensor
        Scalar tensor representing the negative mean log-likelihood across the batch.

    Notes
    -----
    - For uncensored data (`c == 0`), the log-likelihood is computed using the difference
      between log-beta functions evaluated at `t` and `t+1`.
    - For censored data (`c == 1`), the log-likelihood uses the log-beta function at `t+1` only.
    - Subtracting `log_beta(alpha, beta)` ensures the likelihood is properly normalized.
    - The function supports optional per-time weights (e.g., inverse-frequency weights).
    """
    gamma = gamma.squeeze()

    a = log_beta(alpha + torch.pow(t, gamma), beta)
    b = log_beta(alpha + torch.pow(t + 1, gamma), beta)

    # Ensure a > b
    max_val = torch.maximum(a, b)
    min_val = torch.minimum(a, b)

    l_uncensored = max_val + torch.log1p(-torch.exp(min_val - max_val) + eps)

    l_censored = log_beta(alpha + torch.pow(t, gamma), beta)

    l = torch.where(c == 0, l_uncensored, l_censored) - log_beta(alpha, beta)

    return -torch.mean(l)

class Preprocessor:
    def __init__(self, num_features=('upsell_amount', 'first_amount')):
        self.num_features = list(num_features)
        self.ohe = OneHotEncoder(drop='if_binary', handle_unknown='ignore', sparse_output=False)
        self.scaler  = StandardScaler()
    def fit(self, X):
        self.cat_features = [x for x in X.columns if x not in self.num_features]
        X_cat = pd.DataFrame(
            self.ohe.fit_transform(X[self.cat_features]),
            index=X.index, 
            columns=[ x.split('_')[0] if ('unsub' in x or 'upsell' in x) else x for x in self.ohe.get_feature_names_out()]
            )
        X = pd.concat([X_cat, X[self.num_features]], axis=1)
        self.scaler.fit(X)
        return self
    
    def transform(self, X):
        X_cat = pd.DataFrame(
            self.ohe.transform(X[self.cat_features]),
            index=X.index, 
            columns=[ x.split('_')[0] if ('unsub' in x or 'upsell' in x) else x for x in self.ohe.get_feature_names_out()]
            )
        X = pd.concat([X_cat, X[self.num_features]], axis=1)
        return pd.DataFrame(self.scaler.transform(X), index=X.index, columns=X.columns)
    
    def fit_transform(self, X):
        self.fit(X)
        return self.transform(X)

    def save(self, path):
        """
        Saves the fitted Preprocessor object to a file using joblib.
        """
        if self.cat_features is None:
            raise ValueError("Preprocessor must be fitted before calling save.")
        joblib.dump(self, path)
        print(f"Preprocessor saved successfully to {path}")

class SurvivalModel:
    """
    A survival model class that supports 'weibull' and 'beta_logistic' models for survival analysis.

    This class is designed to handle survival data, where the goal is to predict the time-to-event
    along with accounting for censoring. It supports two different model types: 'weibull' and 'beta_logistic',
    and it can handle various ways of treating the gamma parameter.

    Parameters
    ----------
        The shape of the input features (e.g., `(batch_size, num_features)`).
    model_type : str
        Type of the survival model. Options are:
        - 'weibull': A model based on the Weibull distribution.
        - 'beta_logistic': A model based on a Beta distribution and logistic regression.
    device : str
        The device to run the model on. Either 'cuda' (GPU) or 'cpu'.

    Attributes
    ----------
    model : torch.nn.Module
        The neural network architecture for the survival model, either Weibull or Beta Logistic regression.
    loss_function : callable
        The loss function used for training the model.
    name : str
        A string name representing the model, based on the selected `model_type`.
    """

    def __init__(self, hidden_dim=16, 
                 dropout=0.1, num_layers=1, 
                 device='cpu', num_feaures=('upsell_amount', 'first_amount'), 
                 payment_col: str='paid_count', lr=1e-4, weight_decay=1e-4, 
                 n_epochs=15, batch_size=64, show_training_plots=True, 
                 tqdm_disable=False
                 ):
        self.device = device
        self.payment_col = payment_col
        self.hidden_dim, self.dropout, self.num_layers = hidden_dim, dropout, num_layers
        self.payment_model = HistGradientBoostingRegressor()
        self.preprocessor = Preprocessor(num_feaures)

        self.is_preprocessor_fitted = False
        self.is_payment_model_fitted = False
        self.is_model_fitted = False

        self.lr=lr
        self.weight_decay=weight_decay
        self.n_epochs=n_epochs
        self.batch_size=batch_size
        self.show_training_plots=show_training_plots
        self.tqdm_disable=tqdm_disable

        # The loss function is specific to the BdW model
        self.loss_function = BdWLoss

        # Set the name of the model based on the type
        self.name = 'bdw'
    
    def fit(self, X_train, T_train, c_train, X_payment, amount, X_test=None, T_test=None, c_test=None):

        self.fit_processor(X_train)

        cols = list(X_payment.columns)
        idx = np.where( X_payment.dtypes == 'object')[0]
        for i in idx:
            X_payment[cols[i]] = X_payment[cols[i]].astype('category')

        X_train_processed = self.preprocess(X_train)
        X_test_processed  = self.preprocess(X_test) if X_test is not None else None

        self.input_dim = X_train_processed.shape[1]
        self.model = BdWRegression(input_dim=self.input_dim, hidden_dim=self.hidden_dim, dropout=self.dropout, num_layers=self.num_layers).to(self.device)

        self.fit_model(X_train_processed, T_train, c_train, X_test_processed, T_test, c_test)
        self.fit_payment(X_payment, amount)

        return self

    def fit_model(self, X_train, T_train, c_train, X_test=None, T_test=None, c_test=None):
        """
        Trains the survival model using the provided training and testing data.

        This method optimizes the model parameters using stochastic gradient descent with a specified learning rate,
        L2 regularization, and a set number of epochs. It also supports saving the trained model and optionally
        plotting the training and test loss during the training process.

        Parameters
        ----------
        X_train : torch.Tensor
            The input features for the training data (shape: [n_samples, n_features]).
        T_train : torch.Tensor
            The survival times for the training data (shape: [n_samples]).
        c_train : torch.Tensor
            The censoring indicators for the training data (0 for uncensored, 1 for censored) (shape: [n_samples]).
        X_test : torch.Tensor
            The input features for the testing data (shape: [n_samples, n_features]).
        T_test : torch.Tensor
            The survival times for the testing data (shape: [n_samples]).
        c_test : torch.Tensor
            The censoring indicators for the testing data (0 for uncensored, 1 for censored) (shape: [n_samples]).
        lr : float
            The learning rate for the optimization algorithm.
        weight_decay : float
            The L2 regularization coefficient to prevent overfitting.
        n_epochs : int
            The number of epochs to train the model.
        batch_size : int
            The batch size to use during training.
        show_training_plots : bool
            If True, displays training and test loss plots after training.
        path : str, optional
            The path where the trained model will be saved. If None, the model is not saved.
        save_device : str, optional
            The device ('cpu' or 'cuda') to save the trained model.

        Returns
        -------
        self : object
            The trained model instance.

        Notes
        -----
        - This method assumes that the model has been instantiated with the necessary architecture and loss function.
        - If `show_training_plots` is True, the function will plot the training and testing losses after training is complete.
        - The trained model can be saved to the specified `path`. The model will be saved on the device specified by `save_device`.
        """
        
        self.train_data = SurvivalDataset(X_train, T_train, c_train)
        self.test_data = SurvivalDataset(X_test, T_test, c_test) if (X_test is not None) and (T_test is not None) and (c_test is not None) else None

        train_loader = DataLoader(self.train_data, batch_size=self.batch_size, shuffle=True)
        test_loader = DataLoader(self.test_data, batch_size=self.batch_size, shuffle=True) if self.test_data else None

        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=self.weight_decay)

        train_loss_list, test_loss_list = [], []

        for epoch in range(1, self.n_epochs+1):
            train_loss = self.train(train_loader, optimizer, self.loss_function, tqdm_disable=self.tqdm_disable)
            train_loss_list.append(train_loss)
            if test_loader is not None:
                test_loss = self.eval(test_loader, self.loss_function)
                test_loss_list.append(test_loss)

            if self.show_training_plots:
                clear_output(wait=True)
                fig, ax = plt.subplots(2 if test_loader is not None else 1, figsize=(10, 5))
                plt.title(f'Epoch №{epoch}')
                plt.tight_layout()

                ax[0].set_title(f'Train: {round(train_loss_list[-1], 3)}')
                ax[0].plot(np.arange(1, len(train_loss_list)+1), train_loss_list)
                ax[0].set_xticks(np.arange(1, self.n_epochs+1))

                if test_loader is not None:
                    ax[1].plot(np.arange(1, len(test_loss_list)+1), test_loss_list)
                    ax[1].set_xticks(np.arange(1, self.n_epochs+1))
                    ax[1].set_title(f'Test: {round(test_loss_list[-1], 3)}')

                plt.xlabel('Epoch')
                plt.ylabel('Loss')
                plt.xticks(rotation=45)

                plt.show()

        
        self.is_model_fitted = True
        return self

    def train(self, train_loader, optimizer, loss_function, tqdm_disable=False):
        """
        Trains the model for one epoch.

        This method iterates over the training data in batches, performs forward and backward passes,
        and updates the model parameters using the specified optimizer.

        Parameters
        ----------
        train_loader : DataLoader
            DataLoader for the training data, providing batches of inputs and target labels.
        optimizer : torch.optim.Optimizer
            The optimizer used to update model parameters based on the computed gradients.
        loss_function : function
            The loss function to compute the training loss, which guides the optimization process.

        Returns
        -------
        float
            The average training loss for the epoch, computed as the mean loss over all batches.

        Notes
        -----
        - The optimizer steps after computing the gradients for each batch, and the model weights are updated accordingly.
        - The loss function should be appropriate for the type of model (e.g., survival loss function for survival analysis models).
        - This method does not include evaluation of the model on validation/test data; it is focused on training.
        """
        self.model.train()
        loss_list = []
        for data in tqdm(train_loader, disable=tqdm_disable):
            x, t, c = data
            x, t, c = x.to(self.device), t.to(
                self.device), c.to(self.device)


            pred = self.model(x)
            alpha, beta, gamma = pred[0], pred[1], pred[2]
            loss = loss_function(alpha, beta, t, c, gamma)
 

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            loss_list.append(loss.detach().cpu().numpy())

        # Adjust learning rate
        optimizer.param_groups[0]['lr'] *= (1 - 1e-4)
        return np.mean(loss_list)

    def eval(self, test_loader, loss_function):
        """
        Evaluates the model on the test set.

        This method computes the average test loss by iterating over the test data in batches
        and applying the loss function to the model's predictions.

        Parameters
        ----------
        test_loader : DataLoader
            DataLoader for the test data, providing batches of inputs and target labels.
        loss_function : function
            The loss function to compute the test loss, used to evaluate the model's performance on the test data.

        Returns
        -------
        float
            The average test loss, computed as the mean loss over all batches in the test set.

        Notes
        -----
        - The model is not updated during evaluation; this is a pure inference phase.
        - The loss function should be appropriate for the task (e.g., survival analysis loss function).
        - This method is intended for evaluating the model's performance on unseen data after training.
        """
        self.model.eval()
        x, t, c = test_loader.dataset[:]
        x, t, c = x.to(self.device), t.to(self.device), c.to(self.device)
        pred = self.model(x)
        alpha, beta, gamma = pred[0], pred[1], pred[2]
        loss = loss_function(alpha, beta, t, c, gamma)
        return loss.detach().cpu().numpy().mean()

    def survival_function(self, X=None, method='expectation', n_period=6):
        """
        Computes the survival function for the given data.

        This method estimates the survival function, which provides the probability of survival at any given time
        for each observation in the dataset. The computation can be done using different methods, such as
        'expectation' for expected survival.

        Parameters
        ----------
        X : pd.DataFrame or np.ndarray
            The feature matrix containing the input data used for prediction. Each row corresponds to an observation,
            and each column represents a feature.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the computed survival function values for each observation. The rows represent
            observations, and the columns represent survival probabilities at different time points.

        Notes
        -----
        - The 'expectation' method typically calculates the survival function based on the expected time-to-event
        predicted by the model.
        - If an alternative method is implemented, it should be specified by the `method` argument.
        - The survival function is useful for evaluating the probability that an event of interest has not occurred by
        a certain time.
        """
        self.model.eval()
        index = None
        if X is not None:
            if isinstance(X, pd.DataFrame):
                index = X.index
            data = SurvivalDataset(X)
            X = data[:].to(self.device)
        else:
            X, _, _ = self.train_data[:]
        
        X = X.to(self.device)

        survival_dict = {}
        with torch.no_grad():
            pred = self.model(X)
            alpha, beta, gamma = pred[0].cpu(), pred[1].cpu(), pred[2]
            gamma = gamma.cpu().squeeze()

        for t in range(0, n_period):
            t = torch.tensor([float(t)])

            if method == 'expectation':
                S_hat = torch.exp(log_beta(alpha + torch.pow(t + 1, gamma), beta) - log_beta(alpha, beta)).numpy()
            else:
                S_hat = torch.pow(alpha / (alpha + beta),
                                  torch.pow(t + 1, gamma))
            survival_dict[f'S{int(t)}'] = S_hat

        self.survival_hat = pd.DataFrame(survival_dict, index=index)
        self.gamma = pd.DataFrame(gamma, index=index)
        return self.survival_hat
    
    def fit_payment(self, X, amount):
        self.is_payment_model_fitted = True
        self.amount_cols = list(X.columns)
        self.payment_model.fit(X, amount)
        return self
    
    def fit_processor(self, X):
        self.is_preprocessor_fitted = True
        self.preprocessor.fit(X)
        return self
    
    def predict_payment(self, X):
        return self.payment_model.predict(X)
    
    def preprocess(self, X):
        if not self.is_preprocessor_fitted: 
            self.is_preprocessor_fitted = True
            return self.preprocessor.fit_transform(X)
        else: return self.preprocessor.transform(X)
    
    def ltv_calc(self, X, n_periods: Optional[Union[int, np.ndarray, list]] = 6):

        n_periods = np.array(n_periods).reshape(-1,1)
        max_period = int(max(n_periods))
        paid_count = np.array(range(1, max_period+1))

        df_final = X.copy()

        if (not self.is_preprocessor_fitted) | (not self.is_model_fitted) | (not self.is_payment_model_fitted):
            raise ValueError('All models and processers must be fitted')
        
        X_pay = pd.DataFrame(
                        np.concatenate(
                            [
                            np.repeat(paid_count.reshape(1,-1), X.shape[0], axis=0).flatten().reshape(-1,1),
                            np.repeat(X[[col for col in self.amount_cols if col!=self.payment_col]], max_period, axis=0)
                            ],
                            axis=1
                            ),
                        columns=[self.payment_col]+[col for col in self.amount_cols if col!=self.payment_col], 
                        index = np.repeat(X.index, max_period))
        
        X = self.preprocess(X)

        

        mask = np.zeros((X.shape[0], max_period))
        mask[:] = paid_count
        mask = (mask <= n_periods).astype(int)
        
        
        
        merge_index = X.index.name
        
        S_hat = self.survival_function(X, n_period=max_period)

        df_final = df_final.merge(S_hat, on=merge_index, how='inner')
        df_final.loc[:, [f'S{i}' for i in range(max_period)]] = df_final[[f'S{i}' for i in range(max_period)]].apply(lambda x: round(x, 6))
        
        payments = pd.DataFrame(self.predict_payment(X_pay).reshape(-1, paid_count.shape[0]),
                            columns=[f'amount_period_{i}' for i in paid_count],
                            index=X.index
                            )
        
        df_final = df_final.merge(payments, on=merge_index, how='left')

        df_final[[f'amount_period_{i}' for i in paid_count]] *= mask

        df_final['ltv_recurring'] = np.einsum('ij, ij -> i', 
                                              df_final[[f'S{i}' for i in paid_count-1]].to_numpy(), 
                                              df_final[[f'amount_period_{i}' for i in paid_count]].to_numpy()
                                              )
        return df_final
    
    def save(self, directory_path='./ltv_model_weights'):

        os.makedirs(directory_path, exist_ok=True)
        torch.save(self.model.to('cpu').state_dict(), os.path.join(directory_path, 'bdw_weights.pth'))

        config = {
            'bdw_config': {
                'input_dim': self.input_dim,
                'hidden_dim': self.hidden_dim, 
                'dropout': self.dropout,
                'num_layers': self.num_layers,
                'lr': self.lr,
                'weight_decay': self.weight_decay,
                'n_epochs': self.n_epochs,
                'batch_size': self.batch_size,
            },
            
            # --- Payment Model (Scikit-learn) ---
            'payment_model': self.payment_model,
            'payment_model_config': {
                'payment_col': self.payment_col,
                'amount_cols': self.amount_cols,
            },
            
            # --- Preprocessor (Scikit-learn) ---
            'preprocessor': self.preprocessor,

            # --- Состояние модели ---
            'is_fitted_status': {
                'is_model_fitted': self.is_model_fitted,
                'is_payment_model_fitted': self.is_payment_model_fitted,
                'is_preprocessor_fitted': self.is_preprocessor_fitted,
            }
        }
        joblib_path = os.path.join(directory_path, 'config.joblib')
        joblib.dump(config, joblib_path)
        return self
    
    @classmethod
    def load(cls, directory_path='./ltv_model_weights'):
        """
        Loads the model weights from a saved file.

        This method loads the model's state dictionary from a specified path and transfers it to the given device.

        Parameters
        ----------
        path : str
            The path to the saved model state file.
        load_device : str, optional
            The device ('cpu' or 'cuda') to load the model onto. Default is 'cpu'.

        Returns
        -------
        self : object
            The model with the loaded weights.

        Notes
        -----
        - The model state is loaded onto the specified device. If the model was saved on a different device,
        it will automatically be transferred to the given `load_device`.
        - Ensure that the model architecture is defined before calling this method, as it requires the same architecture
        as the one used when saving the model.
        - The model's weights will be loaded, but other attributes like training state (e.g., optimizer states)
        may need to be handled separately.
        """
        joblib_path = os.path.join(directory_path, 'config.joblib')
        try:
            config = joblib.load(joblib_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"No such file in the directory: {joblib_path}")
        
        bdw_config = config['bdw_config']
        payment_config = config['payment_model_config']
        is_fitted_status = config['is_fitted_status']

        instance = cls(
            hidden_dim=bdw_config['hidden_dim'], 
            dropout=bdw_config['dropout'], 
            num_layers=bdw_config['num_layers'],
            lr=bdw_config['lr'],
            weight_decay=bdw_config['weight_decay'],
            n_epochs=bdw_config['n_epochs'],
            batch_size=bdw_config['batch_size'],
            device='cpu', 
            payment_col=payment_config['payment_col'],
        )
        

        torch_path = os.path.join(directory_path, 'bdw_weights.pth')
        instance.model = BdWRegression(
                        input_dim=bdw_config['input_dim'], 
                        hidden_dim=bdw_config['hidden_dim'], 
                        dropout=bdw_config['dropout'], 
                        num_layers=bdw_config['num_layers']
                    ).to('cpu')
        try:
            instance.model.load_state_dict(torch.load(torch_path, map_location=torch.device('cpu')))
        except FileNotFoundError:
            raise FileNotFoundError(f"No such file in the directory: {torch_path}")
        
        instance.payment_model = config['payment_model']
        instance.amount_cols = payment_config['amount_cols']

        instance.preprocessor = config['preprocessor']
        
        
        instance.is_model_fitted = is_fitted_status['is_model_fitted']
        instance.is_payment_model_fitted = is_fitted_status['is_payment_model_fitted']
        instance.is_preprocessor_fitted = is_fitted_status['is_preprocessor_fitted']

        return instance
        
        



class KaplanMeierEstimator:
    def __init__(self, n_periods=6, alpha=0.05):
        self.n_periods = n_periods
        self.alpha = alpha
        

    def fit(self, T, C):
        quantile = norm.ppf(1-self.alpha/2)
        c = []
        self.d = []
        self.n = []
        for i in range(self.n_periods):
            mask_churn = (T==i)&(C==0)
            mask_all = (T>i)|(T==i)&(C==0)
            self.d.append(sum(mask_churn))
            self.n.append(sum(mask_all))
            c.append(1-sum(mask_churn)/sum(mask_all))

        mask_positive = (np.array(c)>0) & (np.isnan(np.array(c))==False)
        self.c = np.array(c)[mask_positive]
        self.S = np.cumprod(c)[mask_positive]
        self.h = 1-self.c
        self.d = np.array(self.d)[mask_positive]
        self.n = np.array(self.n)[mask_positive]

        var_S = self.S**2 * np.cumsum(self.d / (self.n * (self.n - self.d + 1e-10)))
        self.S_lower = np.maximum(self.S - quantile*np.sqrt(var_S), 0)
        self.S_upper = np.minimum(self.S + quantile*np.sqrt(var_S), 1)

        var_c = self.c*(1-self.c)/self.n
        self.c_lower = self.c - quantile*np.sqrt(var_c)
        self.c_upper = self.c + quantile*np.sqrt(var_c)

        var_h = self.h*(1-self.h)/self.n
        self.h_lower = self.h- quantile*np.sqrt(var_h)
        self.h_upper = self.h + quantile*np.sqrt(var_h)
        
        return self

    def plot_results(self, title='', backend='plotly', model_S_dict=None, save=True):
        """
        model_S_dict: dict[str, np.ndarray] — доп. кривые S(t), например {'Model A': S_pred}
        """
        periods = np.arange(self.n_periods)
        if model_S_dict is None:
            model_S_dict = {}

        if backend == 'plotly':
            try:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                fig = make_subplots(rows=1, cols=3, subplot_titles=[
                    "Kaplan-Meier Survival Curve",
                    "Conditional Survival Probability c(t)",
                    "Hazard h(t)"
                ])

                # --- Survival S(t) ---
                fig.add_trace(go.Scatter(
                    x=periods, y=self.S, mode='lines+markers+text',
                    name='S(t)',
                    text=[f"{y:.2f}" for y in self.S],
                    textposition='top center'
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=periods, y=self.S_upper, fill=None, mode='lines',
                    line_color='lightblue', showlegend=False
                ), row=1, col=1)

                fig.add_trace(go.Scatter(
                    x=periods, y=self.S_lower, fill='tonexty', mode='lines',
                    line_color='lightblue', name=f'{int(100*(1-self.alpha))}% CI'
                ), row=1, col=1)

                # --- Conditional Survival c(t) ---
                fig.add_trace(go.Scatter(
                    x=periods, y=self.c, mode='lines+markers+text',
                    name='c(t)',
                    text=[f"{y:.2f}" for y in self.c],
                    textposition='top center', line_color='green'
                ), row=1, col=2)

                # CI lines only (no fill)
                fig.add_trace(go.Scatter(
                    x=periods, y=self.c_upper, mode='lines',
                    line=dict(color='lightblue', dash='dot'),
                    name=f'Upper {int(100*(1-self.alpha))}% CI'
                ), row=1, col=2)

                fig.add_trace(go.Scatter(
                    x=periods, y=self.c_lower, mode='lines',
                    line=dict(color='lightblue', dash='dot'),
                    name=f'Lower {int(100*(1-self.alpha))}% CI'
                ), row=1, col=2)

                # --- Hazard h(t) ---
                fig.add_trace(go.Scatter(
                    x=periods, y=self.h, mode='lines+markers+text',
                    name='h(t)',
                    text=[f"{y:.2f}" for y in self.h],
                    textposition='top center', line_color='red'
                ), row=1, col=3)

                # CI for c(t) — fill between upper and lower
                fig.add_trace(go.Scatter(
                    x=periods, y=self.c_upper, mode='lines',
                    line=dict(color='lightblue'), showlegend=False
                ), row=1, col=2)

                fig.add_trace(go.Scatter(
                    x=periods, y=self.c_lower, mode='lines',
                    fill='tonexty', line=dict(color='lightblue'),
                    name=f'{int(100*(1-self.alpha))}% CI'
                ), row=1, col=2)

                # CI for h(t) — fill between upper and lower
                fig.add_trace(go.Scatter(
                    x=periods, y=self.h_upper, mode='lines',
                    line=dict(color='lightblue'), showlegend=False
                ), row=1, col=3)

                fig.add_trace(go.Scatter(
                    x=periods, y=self.h_lower, mode='lines',
                    fill='tonexty', line=dict(color='lightblue'),
                    name=f'{int(100*(1-self.alpha))}% CI'
                ), row=1, col=3)

                # === Add model curves ===
                for model_name, S_model in model_S_dict.items():
                    c_model = np.concatenate((S_model[0:1], S_model[1:] / S_model[:-1]))
                    h_model = 1 - c_model

                    fig.add_trace(go.Scatter(
                        x=periods, y=S_model, mode='lines',
                        name=f"S(t) {model_name}",
                        line=dict(dash='dash')
                    ), row=1, col=1)

                    fig.add_trace(go.Scatter(
                        x=periods, y=c_model, mode='lines',
                        name=f"c(t) {model_name}",
                        line=dict(dash='dash')
                    ), row=1, col=2)

                    fig.add_trace(go.Scatter(
                        x=periods, y=h_model, mode='lines',
                        name=f"h(t) {model_name}",
                        line=dict(dash='dash')
                    ), row=1, col=3)

                # Layout
                fig.update_layout(
                    height=500, width=1400,
                    showlegend=True,
                    title_text="Kaplan-Meier Analysis: " + title,
                    template="plotly_white"
                )

                for i in range(1, 4):
                    fig.update_xaxes(
                        title_text="Period",
                        tickmode='array',
                        tickvals=periods,
                        ticktext=[str(p) for p in periods],
                        row=1, col=i
                    )

                fig.update_yaxes(title_text="S(t)", row=1, col=1)
                fig.update_yaxes(title_text="c(t)", row=1, col=2)
                fig.update_yaxes(title_text="h(t)", row=1, col=3)

                fig.show()
                if save:
                    os.makedirs('plots', exist_ok=True)
                    fig.write_html("./plots/Kaplan-Meier Analysis: " + title+".html")
                return
            except Exception as e:
                print(f"⚠️ Plotly backend failed ({e}), falling back to Matplotlib...")

        # === Matplotlib fallback ===
        plt.figure(figsize=(16,5))
        plt.suptitle("Kaplan-Meier Analysis: " + title, fontsize=16, y=1.05)  
        
        # ---- Survival ----
        plt.subplot(1,3,1)
        plt.plot(periods, self.S, marker='o', label='S(t)')
        plt.fill_between(periods, self.S_lower, self.S_upper, color='lightblue', alpha=0.6,
                         label=f'{int(100*(1-self.alpha))}% CI')
        
        for model_name, S_model in model_S_dict.items():
            plt.plot(periods, S_model, '--', label=f"S(t) {model_name}")
        plt.title("Kaplan-Meier Survival Curve")
        plt.xlabel("Period")
        plt.ylabel("Survival Probability")
        plt.ylim(np.maximum(0, min(self.S_lower)-0.05), max(self.S_upper)+0.05)
        plt.grid(True)
        plt.legend()

        # ---- Conditional survival ----
        plt.subplot(1,3,2)
        plt.plot(periods, self.c, marker='o', color='green', label='c(t)')
        for model_name, S_model in model_S_dict.items():
            c_model = np.concatenate(([1], S_model[1:]/S_model[:-1]))
            plt.plot(periods, c_model, '--', label=f"c(t) {model_name}")
        plt.title("Conditional Survival Probability c(t)")
        plt.xlabel("Period")
        plt.ylabel("c(t)")
        plt.ylim(0, 1.05)
        plt.grid(True)
        plt.legend()

        # ---- Hazard ----
        plt.subplot(1,3,3)
        plt.plot(periods, self.h, marker='o', color='red', label='h(t)')
        for model_name, S_model in model_S_dict.items():
            c_model = np.concatenate((S_model[0], S_model[1:]/S_model[:-1]))
            h_model = 1 - c_model
            plt.plot(periods, h_model, '--', label=f"h(t) {model_name}")
        plt.title("Hazard h(t)")
        plt.xlabel("Period")
        plt.ylabel("Hazard")
        plt.ylim(0, 1.05)
        plt.grid(True)
        plt.legend()

        plt.tight_layout()
        if save:
            os.makedirs('plots', exist_ok=True)
            plt.savefig("./plots/Kaplan-Meier Analysis: " + title+".html")
        plt.show()
 

        


    