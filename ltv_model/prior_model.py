import torch
import torch.nn as nn

class BdWRegression(nn.Module):
    """
    Beta-discrete-Weibull (BdW) regression model.

    This model predicts the parameters α and β of a Beta distribution
    and the Weibull shape parameter γ.

    Parameters
    ----------
    hidden_dim : int
        Number of input features.
    hidden : int, optional
        Number of hidden units. Defaults to hidden_dim // 5.

    Attributes
    ----------
    linear : nn.Linear
        First transformation layer.
    out : nn.Linear
        Output layer for α, β (and possibly γ).
    act : nn.SiLU
        Activation function.
    log_gamma : nn.Parameter
        Learnable parameter for constant γ mode.
    gamma_model : nn.Sequential
        Subnetwork for γ prediction in 'partitioned' mode.
    """

    def __init__(self, input_dim, hidden_dim=16, dropout=0.1, num_layers=1):
        super().__init__()
        hidden_dim = input_dim // 5 if hidden_dim is None else hidden_dim

        # If individual gamma is predicted, output one more value

        self.fc = nn.ModuleList([nn.Linear(input_dim, hidden_dim), nn.SiLU()])
        for _ in range(num_layers):
            self.fc.append(
                nn.Sequential(
                    nn.Linear(hidden_dim, hidden_dim),
                    nn.SiLU(),
                    nn.Dropout(dropout)
                    )
            )
        self.out = nn.Linear(hidden_dim, 3)

        self.sigmoid = nn.Sigmoid()
        self.softplus = nn.Softplus()

    def forward(self, x):
        z = x
        for module in self.fc:
            z = module(z)
        out = self.out(z)
        mu = self.sigmoid(out[:,0])
        kappa = self.softplus(out[:,1]) + 1e-8
        gamma = self.softplus(out[:,2]) + 1e-8
        return mu*kappa, (1-mu)*kappa, gamma
