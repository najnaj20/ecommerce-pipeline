"""Demo LocalStack: S3 + Lambda (pola event-driven ETL paling umum di AWS).

Jalankan:  python scripts/demo_localstack.py

Alur yang didemonstrasikan:
  1. Bikin bucket S3 (object storage)
  2. Upload CSV (hasil gold layer) ke S3
  3. Lambda function yang otomatis TERPICU saat file masuk (event-driven!)
  4. Lambda baca file dari S3, hitung ringkasan, simpan hasil ke bucket lain
  5. Verifikasi hasil

Kenapa pola ini penting buat DE:
  - Di AWS asli, "file masuk S3 -> Lambda jalan" itu arsitektur ETL paling
    umum (S3 Event Notification + Lambda = serverless pipeline).
  - Kode ini 100% sama dengan AWS asli (hapus endpoint_url + fake creds).

CATATAN JUJUR: Athena butuh LocalStack PRO (lisensi berbayar). Di community
edition, S3 + Lambda jalan gratis — cukup untuk belajar pola boto3.
"""
from __future__ import annotations

import io
import json
import sys
import time
import zipfile

import boto3

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"
FAKE_CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

SRC_BUCKET = "ecommerce-gold"
DST_BUCKET = "ecommerce-gold-summary"
CSV_CONTENT = """order_item_id,order_id,revenue,customer_city,tier,full_date
OI-1,ORD-1,51.98,Jakarta,premium,2026-08-18
OI-2,ORD-1,25.99,Jakarta,premium,2026-08-18
OI-3,ORD-2,89.00,Bandung,standard,2026-08-19
OI-4,ORD-3,129.99,Surabaya,plus,2026-08-20
"""

# Kode Lambda (ini yang jalan di "cloud" — sama persis syntax AWS asli)
LAMBDA_CODE = """
import csv
import io
import json
import urllib.parse
import boto3

s3 = boto3.client("s3")

def handler(event, context):
    # event = notifikasi otomatis dari S3: "ada file baru masuk!"
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    # 1. baca file dari S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")

    # 2. proses (ETL mini: hitung revenue per tier)
    reader = csv.DictReader(io.StringIO(text))
    per_tier = {}
    for row in reader:
        tier = row["tier"]
        per_tier[tier] = per_tier.get(tier, 0) + float(row["revenue"])

    # 3. tulis hasil ke bucket tujuan
    summary = json.dumps(per_tier, indent=2)
    out_key = f"summary/{key.replace('.csv', '.json')}"
    s3.put_object(Bucket=%s, Key=out_key, Body=summary)
    return {"statusCode": 200, "body": summary}
""" % (f'"{DST_BUCKET}"')


def _client(svc: str):
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name=REGION, **FAKE_CREDS)


def _wait_lambda_active(fn_name: str) -> None:
    lam = _client("lambda")
    for _ in range(20):
        st = lam.get_function(FunctionName=fn_name)["Configuration"]["State"]
        if st == "Active":
            return
        time.sleep(1)


def _zip_lambda_code() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o755 << 16  # rwxr-xr-x permission
        z.writestr(info, LAMBDA_CODE)
    return buf.getvalue()


def step1_buckets() -> None:
    s3 = _client("s3")
    print("\n[1] Bikin bucket S3:", SRC_BUCKET, "dan", DST_BUCKET)
    for b in (SRC_BUCKET, DST_BUCKET):
        s3.create_bucket(Bucket=b)
    print("    ✅ 2 bucket dibuat (sumber + tujuan hasil ETL)")


def step2_upload() -> None:
    print(f"\n[2] Upload fact_sales.csv -> s3://{SRC_BUCKET}/")
    _client("s3").put_object(Bucket=SRC_BUCKET, Key="fact_sales.csv", Body=CSV_CONTENT.encode())
    print("    ✅ di-upload. Di AWS asli, upload ini MENCETUSKAN Lambda otomatis.")


def step3_create_lambda() -> None:
    lam = _client("lambda")
    print("\n[3] Bikin Lambda function 'etl_summary'")
    role_arn = "arn:aws:iam::000000000000:role/lambda-role"  # dummy utk LocalStack
    code = {"ZipFile": _zip_lambda_code()}
    # Hapus function kalau sudah ada (state mungkin corrupt dari run sebelumnya)
    try:
        lam.delete_function(FunctionName="etl_summary")
        print("    🗑️  Lambda lama dihapus (bersihin state)")
    except Exception:
        pass  # belum ada, lanjut
    lam.create_function(
        FunctionName="etl_summary",
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.handler",
        Code=code,
    )
    _wait_lambda_active("etl_summary")
    print("    ✅ Lambda 'etl_summary' aktif (kode Python yang jalan di 'cloud')")


def step4_add_trigger() -> None:
    """Pasang trigger S3 -> Lambda via bucket notification (cara AWS yang BENAR).

    Catatan penting: S3 -> Lambda di AWS pakai put_bucket_notification_configuration,
    BUKAN create_event_source_mapping (itu untuk DynamoDB/Kinesis/SQS).
    """
    s3 = _client("s3")
    print(f"\n[4] Pasang trigger: S3 upload di {SRC_BUCKET} -> Lambda")
    s3.put_bucket_notification_configuration(
        Bucket=SRC_BUCKET,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:etl_summary",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        },
    )
    print("    ✅ Bucket notification terpasang (di AWS asli, upload otomatis memicu Lambda)")

    # Upload file BARU sebagai pemicu event (simulasi data masuk berikutnya)
    new_csv = CSV_CONTENT + "OI-5,ORD-4,79.00,Yogyakarta,standard,2026-08-21\n"
    s3.put_object(Bucket=SRC_BUCKET, Key="fact_sales_v2.csv", Body=new_csv.encode())
    print("    ✅ fact_sales_v2.csv di-upload -> memicu Lambda (event-driven!)")
    time.sleep(3)  # beri waktu Lambda jalan


def step5_verify() -> None:
    print("\n[5] Verifikasi hasil ETL di bucket tujuan:")
    s3 = _client("s3")
    resp = s3.list_objects_v2(Bucket=DST_BUCKET)
    for obj in resp.get("Contents", []):
        data = s3.get_object(Bucket=DST_BUCKET, Key=obj["Key"])["Body"].read().decode()
        print(f"    📄 s3://{DST_BUCKET}/{obj['Key']}")
        for line in data.strip().splitlines():
            print(f"       {line}")


def main() -> int:
    print("=== Demo LocalStack: S3 + Lambda (event-driven ETL) ===")
    steps = [step1_buckets, step2_upload, step3_create_lambda, step4_add_trigger, step5_verify]
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for fn in steps:
        if only and only not in fn.__name__:
            continue
        fn()
    print("\n✅ Demo selesai. Ini pola boto3 + Lambda yang sama dipakai di AWS asli.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
