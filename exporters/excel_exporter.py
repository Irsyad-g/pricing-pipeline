import pandas as pd

def export_all(output, **dfs):
    with pd.ExcelWriter(output, engine="openpyxl", mode="w") as writer:
        for sheet, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet, index=False)