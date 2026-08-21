# 🗺️ Belajar AWS untuk Data Engineer — Panduan Nanaj

Alur belajar dari nol sampai bisa jawab interview "pernah pakai apa di AWS?",
disesuaikan untuk laptop dengan RAM terbatas.

---

## Pilih Jalur Sesuai RAM Laptop

| RAM Laptop | Jalur yang disarankan | Kenapa |
|---|---|---|
| **4 GB** | 🅰️ **moto** (library Python) → lalu AWS Free Tier | Docker Desktop makan 2-4 GB, nggak muat |
| **8 GB** | 🅱️ **LocalStack** via Docker Desktop (memory limit 3-4 GB) | Ketat tapi bisa |
| **16 GB+** | 🅱️ **LocalStack** lancar | — |

> ⚠️ Catatan penting (temuan saat setup): LocalStack **versi terbaru sudah
> berbayar** (butuh license token). Yang gratis adalah **versi 3.8.1**.
> Di community edition: S3 + Lambda + Kinesis dkk jalan, **Athena butuh PRO**.

---

## 🅰️ Jalur Moto (PALING RINGAN — RAM 4 GB)

moto = library Python yang *meniru* API AWS. Tanpa Docker, tanpa akun, ~200 MB RAM.

### Step 1: Install
```bash
# di folder project mana pun
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 moto pandas
```

### Step 2: Kode pertama (S3)
```python
# aws_latihan.py — S3 dengan moto
import boto3
from moto import mock_aws

@mock_aws  # dekorator ini bikin "AWS palsu" jalan di memori
def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="latihan-bucket")

    s3.put_object(Bucket="latihan-bucket", Key="data.csv", Body="a,b\n1,2\n")

    resp = s3.get_object(Bucket="latihan-bucket", Key="data.csv")
    print("Isi file:", resp["Body"].read().decode())

main()
```

### Step 3: Jalankan
```bash
python aws_latihan.py
# Output: Isi file: a,b\n1,2
```

### Latihan lanjutan (naik level)
1. **S3 + pandas**: upload CSV `fact_sales.csv` dari `ecommerce-pipeline`, baca pakai `pd.read_csv(io.BytesIO(...))`
2. **Lambda via moto**: `@mock_aws` + `lambda_.create_function` — pola sama seperti `demo_localstack.py`
3. **SQS**: kirim pesan queue → itu pola pipeline antrian
4. **Glue/DynamoDB**: pilih satu, pelajari API-nya

> Pola yang dipelajari di moto **identik** dengan AWS asli — tinggal hapus
> `@mock_aws` dan tambah credentials asli. Ini nilai utamanya.

---

## 🅱️ Jalur LocalStack (RAM 8 GB+)

### Step 1: Install Docker Desktop
- Windows: https://www.docker.com/products/docker-desktop/
- Set memory limit: Docker Desktop → Settings → Resources → **Memory: 3-4 GB** (jangan lebih, biar laptop nggak lemot)

### Step 2: Bikin `docker-compose.localstack.yml`
```yaml
services:
  localstack:
    image: localstack/localstack:3.8.1   # ⚠️ 3.8.1 = versi gratis terakhir
    container_name: my-localstack
    ports:
      - "4566:4566"
    environment:
      - SERVICES=s3,lambda,secretsmanager,sts
      - PERSISTENCE=1
    volumes:
      - localstack_data:/var/lib/localstack
      - /var/run/docker.sock:/var/run/docker.sock   # WAJIB untuk Lambda
volumes:
  localstack_data:
```

### Step 3: Jalankan & tes
```bash
docker compose -f docker-compose.localstack.yml up -d
curl http://localhost:4566/_localstack/health   # harus keluar {"services":{...}}

# Install CLI bantuan
pip install awscli-local   # perintah "awslocal" = aws tapi nunjuk ke localhost
awslocal s3 mb s3://tes-bucket    # bikin bucket
awslocal s3 ls                    # list bucket
```

### Step 4: Jalankan demo dari project ini
```bash
# clone project ini dulu, lalu:
pip install boto3
python scripts/demo_localstack.py
```

### Step 5: Latihan lanjutan
1. **S3 + Athena** — ⚠️ butuh PRO. Ganti: query pakai **DuckDB** lokal di atas file S3 (pola sama: file → SQL → hasil)
2. **Kinesis** (streaming AWS): `awslocal kinesis create-stream --stream-name test --shard-count 1`
3. **Lambda** lanjutan: coba trigger dari **SQS** (`create_event_source_mapping` — ini yang benar untuk SQS, bukan S3!)
4. **Secrets Manager**: simpan & baca secret — pola untuk API key di pipeline

---

## 🅲️ Jalur AWS Free Tier (PALING NYATA — 0 KB RAM laptop)

Semua jalan di cloud AWS → laptop cuma butuh browser + kode.

### Step 1: Bikin akun
1. https://aws.amazon.com/free/ → Create a Free Account
2. Butuh email + **kartu kredit** (hanya verifikasi, nggak ditagih kalau di limit)
3. Region: pilih **ap-southeast-1 (Singapore)** — terdekat & cepat

### Step 2: Install CLI
```bash
# di laptop
pip install awscli boto3
aws configure
# AWS Access Key ID: (dari IAM user — bikin di console)
# Default region: ap-southeast-1
```

### Step 3: Project pertama (gratis, di limit Free Tier)
```bash
# S3 (5 GB gratis)
aws s3 mb s3://nanaj-latihan-bucket
aws s3 cp fact_sales.csv s3://nanaj-latihan-bucket/

# Athena (query 5 TB/bulan gratis) — bikin tabel di atas CSV di S3
# (pakai pola DDL di demo_localstack.py bagian step3 — 100% sama!)

# Lambda (1 juta request/bulan gratis)
```

### Checklist "aman gratis" (Free Tier limit)
- ✅ S3: 5 GB storage, 20k GET, 2k PUT
- ✅ Lambda: 1 juta request + 400k GB-detik compute
- ✅ Athena: 5 TB data query
- ❌ JANGAN: EC2 besar, RDS, Redshift (bisa ditagih)

---

## 🎯 Target Akhir (biar bisa jawab interview)

Setelah 2-4 minggu, kamu harus bisa cerita:

> "Saya pernah bangun pipeline ETL di AWS: file masuk ke **S3** (bronze),
> **Lambda** yang terpicu otomatis membersihkan dan transform, hasilnya di
> **S3** lagi (silver/gold), lalu di-query pakai **Athena**. Di lokal saya
> pakai **LocalStack/moto** untuk development, dan kodenya pakai **boto3**
> yang sama dengan production."

Kalau bisa cerita itu + nunjukin `demo_localstack.py`, kamu udah di atas 90%
kandidat DE entry-level lain.

---

## ⚠️ Pitfall yang sudah kupelajari (biar kamu nggak kejebak)

1. **LocalStack terbaru = berbayar.** Pakai `localstack/localstack:3.8.1`.
2. **Lambda butuh Docker-in-Docker**: mount `/var/run/docker.sock` ke container.
3. **Zip Lambda harus permission 755**: `info.external_attr = 0o755 << 16`.
4. **S3 → Lambda pakai `put_bucket_notification_configuration`**, BUKAN
   `create_event_source_mapping` (itu untuk DynamoDB/Kinesis/SQS).
5. **Athena = PRO feature** di LocalStack. Community: S3 + Lambda + Kinesis.
6. Kalau state LocalStack korup: `docker compose down -v` (hapus volume) → up lagi.
7. **Free Tier tetap bisa ditagih** kalau salah service/region — selalu cek
   billing dashboard tiap minggu pertama.
