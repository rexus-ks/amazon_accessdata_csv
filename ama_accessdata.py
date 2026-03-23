import requests
import time
import pandas as pd
import gzip
import io
import json
from datetime import datetime, timedelta

# -----------------------------
# 認証情報（※本番は環境変数推奨）
# -----------------------------
import os

CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["REFRESH_TOKEN"]
MARKETPLACE_ID = "A1VC38T7YXB528"

# -----------------------------
# 日付範囲（昨日〜過去31日）
# -----------------------------
def get_date_range():
    end = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=30)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def date_range(start, end):
    d = datetime.strptime(start, "%Y-%m-%d")
    end_d = datetime.strptime(end, "%Y-%m-%d")
    while d <= end_d:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)

# -----------------------------
# LWA認証
# -----------------------------
def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# -----------------------------
# 429対応POST
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
# レポート作成（重要：sleep入り）
# -----------------------------
def create_report(token, date):
    url = "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports"

    headers = {
        "Authorization": f"Bearer {token}",
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    start = datetime.strptime(date, "%Y-%m-%d") - timedelta(hours=9)
    end   = start + timedelta(days=1) - timedelta(seconds=1)

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

    time.sleep(6)  # ←レート制限対策（最重要）

    return r.json()["reportId"]

# -----------------------------
# レポート待機
# -----------------------------
def wait_report(token, report_id):
    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "x-amz-access-token": token
    }

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
# ダウンロード
# -----------------------------
def download(token, doc_id):
    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/documents/{doc_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "x-amz-access-token": token
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    doc = r.json()

    res = requests.get(doc["url"])
    res.raise_for_status()

    content = gzip.decompress(res.content) if doc.get("compressionAlgorithm") == "GZIP" else res.content
    return json.loads(content)

# -----------------------------
# データ整形
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
# 実行
# -----------------------------
if __name__ == "__main__":
    start_date, end_date = get_date_range()
    print(f"取得範囲: {start_date} ～ {end_date}")

    token = get_access_token()
    dfs = []

    for d in date_range(start_date, end_date):
        try:
            print(f"取得開始: {d}")

            report_id = create_report(token, d)
            doc_id = wait_report(token, report_id)
            json_data = download(token, doc_id)

            df = transform(json_data, d)
            dfs.append(df)

        except Exception as e:
            print(f"エラー（{d}）:", e)

    result = pd.concat(dfs, ignore_index=True)

    filename = f"ama_access_{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
    result.to_csv(filename, index=False, encoding="utf-8-sig")

    print("完了:", filename)