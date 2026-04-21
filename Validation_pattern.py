def validate(df):
    errors = []

    #Check NULLS
    if df['amount'].isna().sum() > 0:
        errors.append("NULL amounts")

    #Check Negatives
    if (df['amount'] < 0).any():
        errors.append("Negative amounts")

    #Check duplicates
    if df['order_id'].duplicated().any():
        errors.append("Duplicate IDs")

    if errors:
        raise ValueError(f"Validation failed: {errors}")

    return True


#Bad fail

if problems:
    print("Warning: issues found") #Continue anyway
load(bad_data)

#Good
if problems:
    raise ValueError("Validation failed") #STOP
