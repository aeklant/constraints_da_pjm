import polars as pl


df_constraints = pl.read_csv("~/Downloads/pjm_constraints_DART.csv")\
    .with_columns(
        pl.col("Datetime (HB)").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%z").dt.convert_time_zone("EST"))

df_lmps = pl.read_csv("~/Downloads/PJMmiso_DART.csv")\
    .with_columns(
        pl.col("Datetime (HB)").str.to_datetime("%Y-%m-%d %H:%M:%S%z").dt.convert_time_zone("EST"),
    (pl.col("PJ mi RTU") - pl.col("PJ mi DA")).alias("PJ mi DART"))

df = df_constraints\
    .join(df_lmps, on="Datetime (HB)", how="full", coalesce=True)

from plotnine import *
import pdb; pdb.set_trace()
e = ggplot(df, aes(x="shadow_price_DA", y="PJ mi DA")) + geom_point()
