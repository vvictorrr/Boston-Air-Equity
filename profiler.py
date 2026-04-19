import pandas as pd
from ydata_profiling import ProfileReport

df = pd.read_csv("census_tracts.csv")

profile = ProfileReport(df, title="Census Data Report")
profile.to_file("report.html")