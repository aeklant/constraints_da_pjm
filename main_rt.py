import polars as pl


df = pl.read_csv("rt_marginal_value.csv")\
    .with_columns(
        pl.col("datetime_beginning_utc").str.strptime(
            pl.Datetime,
            format="%m/%d/%Y %I:%M:%S %p").dt.convert_time_zone("EST")
        .alias("HB EST"))\
    .with_columns(
        pl.col("HB EST").dt.date().alias("date"),
        pl.col("HB EST").dt.hour().alias("hour"),
    )\
    .group_by("date", "hour")\
    .agg(pl.col("shadow_price").sum()/12)\
    .sort("date", "hour")\
    .with_columns(
        pl.datetime(
            year=pl.col("date").dt.year(),
            month=pl.col("date").dt.month(),
            day=pl.col("date").dt.day(),
            hour=pl.col("hour"))
        .alias("datetime"))\
    .select(
        "datetime",
        "shadow_price")\

print(df)

#df.write_csv("rt_constraints_display.csv")
