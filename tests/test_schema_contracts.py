import pandas as pd
import pandera.pandas as pa
import pytest

from src.schema_contracts import SCHEMAS


def _valid_signals():
    return pd.DataFrame({
        "strategy_id": ["strategy_a", "strategy_b"],
        "date": pd.to_datetime(["2026-07-23", "2026-07-23"]),
        "ticker": ["AAPL", "MSFT"],
        "signal_score": [0.5, -1.2],
        "position_target_usd": [1_000_000.0, -500_000.0],
    })


def test_valid_signals_pass():
    validated = SCHEMAS["strategy_signals"].validate(_valid_signals())
    assert len(validated) == 2


def test_unknown_strategy_id_rejected():
    df = _valid_signals()
    df.loc[0, "strategy_id"] = "strategy_zzz"
    with pytest.raises(pa.errors.SchemaError):
        SCHEMAS["strategy_signals"].validate(df)


def test_signal_score_out_of_range_rejected():
    df = _valid_signals()
    df.loc[0, "signal_score"] = 999.0
    with pytest.raises(pa.errors.SchemaError):
        SCHEMAS["strategy_signals"].validate(df)


def test_negative_gross_exposure_rejected():
    df = pd.DataFrame({
        "strategy_id": ["strategy_a"],
        "date": pd.to_datetime(["2026-07-23"]),
        "pnl_usd": [1000.0],
        "gross_exposure_usd": [-5.0],
        "net_exposure_usd": [100.0],
    })
    with pytest.raises(pa.errors.SchemaError):
        SCHEMAS["strategy_pnl"].validate(df)


def test_extra_column_rejected_due_to_strict_mode():
    df = _valid_signals()
    df["unexpected_col"] = "oops"
    # strict-mode column violations raise the plural SchemaErrors, not SchemaError
    with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
        SCHEMAS["strategy_signals"].validate(df)
