import databento as db
from pathlib import Path

# 1. Path to your DBN file
dbn_path = Path(r"C:\Users\30704\Desktop\OA-HFT\CLX5_mbo (2).dbn")

# 2. Open the DBN file as a DBNStore
store = db.DBNStore.from_file(dbn_path)

# 3. Convert to a pandas DataFrame.
#    For MBO data, "mbo" schema is typically appropriate.
#    If this errors, try removing schema=... entirely.
df = store.to_df(schema="mbo")

# 4. Save to CSV next to the original file
out_path = dbn_path.with_suffix(".csv")
df.to_csv(out_path, index=False)

print(f"Done! CSV written to: {out_path}")
