import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# =============================
# 環境変数
# =============================
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["REFRESH_TOKEN"]

MARKETPLACE_ID = "A1VC38T7YXB528"

# =============================
# 日付生成（前日～7日前）
# =============================
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

# =============================
# アクセストークン取得
# =============================
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

# =============================
# レポート作成（retry付き）
# =============================
def create_report(token, date, retry=3):
    print(f"{date} レポート作成中...")

    url = "https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": f"{date}T00:00:00Z",
        "dataEndTime": f"{date}T23:59:59Z",
        "reportOptions": {"dateGranularity": "DAY"}
    }

    for i in range(retry):
        r = requests.post(url, headers=headers, json=payload)

        if r.status_code == 429:
            wait = (i + 1) * 10
            print(f"429発生 → {wait}秒待機")
            time.sleep(wait)
            continue

        r.raise_for_status()
        report_id = r.json()["reportId"]
        print(f"report_id: {report_id}")
        return report_id

    raise Exception("レポート作成失敗")

# =============================
# ステータス確認
# =============================
def wait_report(token, report_id):
    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"Authorization": f"Bearer {token}"}

    print("ステータス確認中...")

    while True:
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        data = r.json()
        status = data["processingStatus"]
        print(f"現在ステータス: {status}")

        if status == "DONE":
            return data["reportDocumentId"]

        elif status in ["CANCELLED", "FATAL"]:
            raise Exception("レポート生成失敗")

        time.sleep(10)

# =============================
# ダウンロード
# =============================
def download_report(token, doc_id):
    print("レポートダウンロード中...")

    url = f"https://sellingpartnerapi-fe.amazon.com/reports/2021-06-30/documents/{doc_id}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    doc = r.json()
    data = requests.get(doc["url"]).json()

    return data

# =============================
# データ整形
# =============================
def parse_data(json_data, target_date):
    rows = []

    for item in json_data.get("salesAndTrafficByAsin", []):
        row = {
            "対象日": target_date,
            "親ASIN": item.get("parentAsin"),
            "子ASIN": item.get("childAsin"),
            "注文数": item.get("orderedProductSales", {}).get("amount"),
            "注文数_B2B": item.get("orderedProductSalesB2B", {}).get("amount"),
            "売上": item.get("orderedProductSales", {}).get("amount"),
            "売上_B2B": item.get("orderedProductSalesB2B", {}).get("amount"),
            "セッション数": item.get("trafficByAsin", {}).get("sessions"),
            "セッション_B2B": item.get("trafficByAsin", {}).get("sessionsB2B"),
            "ブラウザセッション": item.get("trafficByAsin", {}).get("browserSessions"),
            "モバイルセッション": item.get("trafficByAsin", {}).get("mobileAppSessions"),
            "PV": item.get("trafficByAsin", {}).get("pageViews"),
            "PV_B2B": item.get("trafficByAsin", {}).get("pageViewsB2B"),
            "ブラウザPV": item.get("trafficByAsin", {}).get("browserPageViews"),
            "モバイルPV": item.get("trafficByAsin", {}).get("mobileAppPageViews"),
            "CVR": item.get("trafficByAsin", {}).get("unitSessionPercentage"),
            "CVR_B2B": item.get("trafficByAsin", {}).get("unitSessionPercentageB2B"),
        }
        rows.append(row)

    return rows

# =============================
# メイン処理
# =============================
if __name__ == "__main__":
    try:
        dates = get_date_range()
        token = get_access_token()

        all_rows = []

        for date in dates:
            report_id = create_report(token, date)
            doc_id = wait_report(token, report_id)
            json_data = download_report(token, doc_id)

            rows = parse_data(json_data, date)
            all_rows.extend(rows)

            time.sleep(5)  # API負荷軽減

        df = pd.DataFrame(all_rows)

        file_name = f"ama_access_{dates[0]}_{dates[-1]}.csv"
        df.to_csv(file_name, index=False)

        print(f"CSV出力完了: {file_name}")

    except Exception as e:
        print("エラー発生:", e)