def df_to_json(df):
    return [
        dict([
            (colname, row[i])
            for i,colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
