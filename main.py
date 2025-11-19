import polars as pl


da_constraints = pl.read_csv("constraints_display.csv")\
    .with_columns(
        pl.col("Datetime (HB)")\
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%z")\
        .dt.convert_time_zone("EST")
        .alias("datetime"))

rt_constraints = pl.read_csv("rt_constraints_display.csv")\
    .with_columns(
        pl.col("datetime")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%.f")\
        .dt.replace_time_zone("EST"))

df = da_constraints\
    .join(
        other=rt_constraints,
        on="datetime",
        coalesce=True,
        suffix="_rt",
        how="full")\
    .with_columns(
        abs(pl.col("shadow_price_rt")),  # easier to interpret when positive
    )\
    .sort("datetime")\
    .select(
        pl.col("datetime").alias("Datetime (HB)"),
        pl.col("shadow_price").alias("shadow_price_DA"),
        pl.col("shadow_price_rt").alias("shadow_price_RT"),
    )\
    .with_columns(
        (pl.col("shadow_price_RT") - pl.col("shadow_price_DA"))
        .alias("shadow_price_DART"),
    )

print(df)
#df.write_csv("~/Downloads/pjm_constraints_DART.csv")
