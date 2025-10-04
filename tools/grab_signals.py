import os, s3fs
region = os.environ["AWS_DEFAULT_REGION"]
fs = s3fs.S3FileSystem(anon=False, client_kwargs={'region_name': region})

paths = fs.glob("s3://stock-options-ai-app/ohlc/meta/reports/*_signals.csv")
print("found", len(paths), "reports")
if paths:
    latest = sorted(paths)[-1]
    with fs.open(latest, "rb") as fr, open("signals.csv", "wb") as fw:
        fw.write(fr.read())
    print("downloaded: signals.csv")
