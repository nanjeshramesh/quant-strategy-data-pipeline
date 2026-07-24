"""
Canonical, unified analytics schema. Every source strategy, regardless of its
raw shape, must conform to these contracts before it lands in the lake. This
is the "standardize outputs from strategy workflows into a unified analytics
schema" piece of the JD -- the contract is the enforceable version of a
schema doc, not just a wiki page.
"""
import pandera.pandas as pa
from pandera.typing import Series

VALID_STRATEGIES = {"strategy_a", "strategy_b", "strategy_c"}


class SignalsSchema(pa.DataFrameModel):
    strategy_id: Series[str] = pa.Field(isin=VALID_STRATEGIES)
    date: Series["datetime64[ns]"]
    ticker: Series[str] = pa.Field(str_length={"min_value": 1, "max_value": 10})
    signal_score: Series[float] = pa.Field(nullable=False, in_range={"min_value": -10, "max_value": 10})
    position_target_usd: Series[float] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True


class PnlSchema(pa.DataFrameModel):
    strategy_id: Series[str] = pa.Field(isin=VALID_STRATEGIES)
    date: Series["datetime64[ns]"]
    pnl_usd: Series[float] = pa.Field(nullable=False)
    gross_exposure_usd: Series[float] = pa.Field(nullable=False, ge=0)
    net_exposure_usd: Series[float] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True


SCHEMAS = {
    "strategy_signals": SignalsSchema,
    "strategy_pnl": PnlSchema,
}
