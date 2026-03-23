import requests
import time
import pandas as pd
import gzip
import io
import json
from datetime import datetime, timedelta

# -----------------------------
# 環境変数（GitHub対応）
# -----------------------------
import os
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["REFRESH_TOKEN"]

MARKETPLACE_ID = "A1VC38T7YXB528"

# -----------------------------
# 日付範囲（前日～7日）
# -----------------------------
def get_date_range():
    end = datetime.utcnow() - timedelta(days=1)
    start = end - timedelta(days=6)

    print(f"取得範囲: {start.date()} ～ {end.date()}")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates

# -----------------------------
# 認証
# -----------------------------
def get_access_token():
    print("アクセストークン取得中...")
    url = "https://api.amazon.com/auth/o2/token"

    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    r = requests.post(url, data=data)
    r.raise_for_status()

    print("アクセストークン取得OK")
    return r.json()["access_token"]

# -----------------------------
# 429対策POST
# -----------------------------
def safe_post(url, headers, payload):
    for i in range(5):
        r = requests.post(url, headers=headers, json=payload)

        if r.status_code == 429:
            wait = 5 * (i + 1)
            print(f"429発生 → {wait}秒待機")
            time.sleep(wait)
            continue

        r.raise_for_status()
        return r

    raise Exception("POSTリトライ上限")

# -----------------------------
# レポート作成
# -----------------------------
def create_report(token, date):
    print(f"{date} レポート作成中...")

    url = "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports"

    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    start = datetime.strptime(date, "%Y-%m-%d") - timedelta(hours=9)
    end = start + timedelta(days=1) - timedelta(seconds=1)

    payload = {
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataEndTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reportOptions": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        }
    }

    r = safe_post(url, headers, payload)
    report_id = r.json()["reportId"]

    print(f"report_id: {report_id}")
    time.sleep(5)

    return report_id

# -----------------------------
# ステータス確認
# -----------------------------
def wait_report(token, report_id):
    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": token}

    print("ステータス確認中...")

    while True:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()

        status = data["processingStatus"]
        print("ステータス:", status)

        if status == "DONE":
            return data["reportDocumentId"]

        elif status in ["CANCELLED", "FATAL"]:
            raise Exception("レポート生成失敗")

        time.sleep(10)

# -----------------------------
# ダウンロード（JSON）
# -----------------------------
def download(token, doc_id):
    print("レポートダウンロード中...")

    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/documents/{doc_id}"
    headers = {"x-amz-access-token": token}

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    doc = r.json()

    res = requests.get(doc["url"])
    res.raise_for_status()

    if doc.get("compressionAlgorithm") == "GZIP":
        content = gzip.decompress(res.content)
    else:
        content = res.content

    return json.loads(content.decode("utf-8"))

# -----------------------------
# データ整形（JSON専用）
# -----------------------------
def transform(json_data, target_date):
    rows = []

    for r in json_data.get("salesAndTrafficByAsin", []):
        s = r.get("salesByAsin", {})
        t = r.get("trafficByAsin", {})

        rows.append({
            "対象日": target_date,
            "親ASIN": r.get("parentAsin"),
            "子ASIN": r.get("childAsin"),
            "注文数": s.get("unitsOrdered"),
            "注文数_B2B": s.get("unitsOrderedB2B"),
            "売上": (s.get("orderedProductSales") or {}).get("amount"),
            "売上_B2B": (s.get("orderedProductSalesB2B") or {}).get("amount"),
            "セッション数": t.get("sessions"),
            "セッション_B2B": t.get("sessionsB2B"),
            "ブラウザセッション": t.get("browserSessions"),
            "モバイルセッション": t.get("mobileAppSessions"),
            "PV": t.get("pageViews"),
            "PV_B2B": t.get("pageViewsB2B"),
            "ブラウザPV": t.get("browserPageViews"),
            "モバイルPV": t.get("mobileAppPageViews"),
            "CVR": t.get("unitSessionPercentage"),
            "CVR_B2B": t.get("unitSessionPercentageB2B")
        })

    return pd.DataFrame(rows)

# -----------------------------
# メイン処理
# -----------------------------
if __name__ == "__main__":
    try:
        dates = get_date_range()
        token = get_access_token()

        dfs = []

        for d in dates:
            print(f"取得開始: {d}")

            report_id = create_report(token, d)
            doc_id = wait_report(token, report_id)

            json_data = download(token, doc_id)
            df = transform(json_data, d)

            print(f"{d} 件数:", len(df))

            dfs.append(df)

        result = pd.concat(dfs, ignore_index=True)

        filename = f"ama_access_{dates[0]}_{dates[-1]}.csv"
        result.to_csv(filename, index=False, encoding="utf-8-sig")

        print("完了:", filename)

    except Exception as e:
        print("エラー発生:", e)