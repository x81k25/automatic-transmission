import pandas as pd

# ------------------------------------------------------------------------------
# functions to safely write elements to dataframes without altering dtypes
# ------------------------------------------------------------------------------

def assign_row(df, index, new_row):
    """
    Safely assign a new row to the DataFrame while preserving dtypes.
    """
    for column, value in new_row.items():
        if column in df.columns:
            try:
                if str(df[column].dtype) == 'category':
                    if pd.notna(value):
                        if value not in df[column].cat.categories and value is not None:
                            df[column] = df[column].cat.add_categories([value])
                    df.at[index, column] = value
                else:
                    # For non-category types, convert to the correct dtype
                    df.at[index, column] = pd.Series([value], dtype=df[column].dtype)[0]
            except Exception as e:
                # If conversion fails, assign directly and log the issue
                df.at[index, column] = value
                print(f"Warning: Could not preserve dtype for column {column}: {e}")
    return df

def update_status(df, index, status):
    """
    Safely update the status column while preserving its category dtype.
    """
    try:
        if str(df['status'].dtype) == 'category':
            if status not in df['status'].cat.categories:
                df['status'] = df['status'].cat.add_categories([status])
        df.at[index, 'status'] = status
    except Exception as e:
        # Fallback: direct assignment if category operations fail
        df.at[index, 'status'] = status
        print(f"Warning: Could not preserve category dtype for status: {e}")
    return df

# ------------------------------------------------------------------------------
# end of safe.py
# ------------------------------------------------------------------------------
