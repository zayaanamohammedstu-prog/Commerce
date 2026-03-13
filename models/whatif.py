"""
models/whatif.py
What-If simulation engine using Monte Carlo over forecast residuals.

Supported scenario parameters (all optional, pass 0 for no effect):
  - price_change_pct   : % price change (positive = price increase)
  - demand_shock_pct   : % demand shock (positive = demand boost)
  - promo_boost_pct    : % promotional uplift

Returns median + p10/p90 probability corridor for each forecast day.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_N_SIMS = 1000
_PRICE_ELASTICITY = -1.5   # assumed demand elasticity to price
_CONFIDENCE_INTERVAL = (10, 90)   # p10 / p90


def _scenario_multiplier(
    price_change_pct: float = 0.0,
    demand_shock_pct: float = 0.0,
    promo_boost_pct:  float = 0.0,
) -> float:
    """
    Compute a combined revenue multiplier from scenario parameters.

    Price change affects revenue via elasticity:
      demand_change_pct = elasticity * price_change_pct
      revenue_change    = (1 + price_change_pct/100) * (1 + demand_change_pct/100) - 1
    """
    # Price effect (price change + corresponding demand change via elasticity)
    price_mult = 1.0
    if price_change_pct != 0:
        demand_from_price = _PRICE_ELASTICITY * (price_change_pct / 100)
        price_mult = (1 + price_change_pct / 100) * (1 + demand_from_price)

    demand_mult = 1 + demand_shock_pct / 100
    promo_mult  = 1 + promo_boost_pct  / 100

    return price_mult * demand_mult * promo_mult


def run_whatif(
    base_forecast: List[Dict],
    scenario: Dict,
    residual_std: float = None,
) -> List[Dict]:
    """
    Run a Monte Carlo what-if simulation.

    Parameters
    ----------
    base_forecast : list of {"date": ..., "yhat": ..., "yhat_lower": ..., "yhat_upper": ...}
    scenario      : dict with optional keys:
                    price_change_pct, demand_shock_pct, promo_boost_pct
    residual_std  : optional override for noise std (auto-computed from confidence band if None)

    Returns
    -------
    list of {"date": ..., "p10": ..., "p50": ..., "p90": ...}
    """
    if not base_forecast:
        return []

    price_change = float(scenario.get("price_change_pct",  0))
    demand_shock = float(scenario.get("demand_shock_pct",  0))
    promo_boost  = float(scenario.get("promo_boost_pct",   0))

    multiplier = _scenario_multiplier(price_change, demand_shock, promo_boost)

    yhats = np.array([r.get("yhat", 0.0) for r in base_forecast], dtype=float)
    lo    = np.array([r.get("yhat_lower", r.get("yhat", 0.0)) for r in base_forecast], dtype=float)
    hi    = np.array([r.get("yhat_upper", r.get("yhat", 0.0)) for r in base_forecast], dtype=float)

    # Estimate residual std from the confidence band width (≈ 1.28σ for 80 % CI)
    if residual_std is None:
        band_width  = np.mean(hi - lo)
        residual_std = max(1.0, band_width / (2 * 1.28))

    rng = np.random.default_rng(42)
    # (n_sims × horizon) noise matrix
    noise = rng.normal(0, residual_std, size=(_N_SIMS, len(yhats)))

    # Apply multiplier + noise
    sims = np.maximum(0, yhats[np.newaxis, :] * multiplier + noise)

    p10 = np.percentile(sims, _CONFIDENCE_INTERVAL[0], axis=0)
    p50 = np.percentile(sims, 50,                      axis=0)
    p90 = np.percentile(sims, _CONFIDENCE_INTERVAL[1], axis=0)

    return [
        {
            "date": base_forecast[i]["date"],
            "p10":  round(float(p10[i]), 2),
            "p50":  round(float(p50[i]), 2),
            "p90":  round(float(p90[i]), 2),
        }
        for i in range(len(base_forecast))
    ]
