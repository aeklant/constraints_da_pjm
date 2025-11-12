import polars as pl


df = pl.read_csv("constraints_2025.csv")\
    .with_columns(
        pl.col("datetime_beginning_utc").str.strptime(
            pl.Datetime,
            format="%m/%d/%Y %I:%M:%S %p").dt.convert_time_zone("EST"))\
    .select(
        "datetime_beginning_utc",
        "shadow_price")\
    .group_by("datetime_beginning_utc")\
    .agg(pl.col("shadow_price").sum())\
    .rename({"datetime_beginning_utc":"Datetime (HB)"})\
    .sort("Datetime (HB)")


print(df)

df.write_csv("~/Downloads/constraints_display.csv")
