import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ===================== 配置项 =====================
URL_LIMIT        = "https://gg.cfi.cn/data_ndkA0A1934A1935A60.html"
URL_INDUSTRY     = "https://gg.cfi.cn/cfi_datacontent_server.aspx?ndk=A0A1934A1935A39&client=pc"
URL_PROFIT       = "https://gg.cfi.cn/cfi_datacontent_server.aspx?ndk=A0A1934A1935A59&client=pc"
SAVE_FILE        = "行业涨跌停统计_每日追加.xlsx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
# ===================================================

def get_today_date():
    return datetime.now().strftime("%Y-%m-%d")

# ===================== 爬取行业涨跌停数据 =====================
def crawl_limit_data():
    print("正在爬取行业涨跌停数据...")
    resp = requests.get(URL_LIMIT, headers=HEADERS, timeout=15)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")
    all_data = []
    current_industry = "未知行业"
    industry_pattern = re.compile(r"^([^-]+)-([^（]+)")

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            txt = tr.get_text(strip=True)
            if "（共" in txt and "家数" in txt:
                match = industry_pattern.match(txt)
                if match:
                    current_industry = match.group(1).strip()
                continue
            tds = tr.find_all("td")
            if len(tds) >= 15:
                row = [td.get_text(strip=True) for td in tds]
                all_data.append({
                    "日期": get_today_date(),
                    "行业": current_industry,
                    "代码": row[0],
                    "名称": row[1],
                    "最新": row[2],
                    "涨跌": row[3],
                    "涨跌幅": row[4],
                    "前收": row[5],
                    "开盘": row[6],
                    "最高": row[7],
                    "最低": row[8],
                    "成交量": row[9],
                    "成交额": row[10],
                    "市盈率": row[11],
                    "换手率": row[12],
                    "总股本": row[13],
                    "流通股本": row[14]
                })
    df = pd.DataFrame(all_data)
    print(f"✅ 涨跌停数据抓取完成：{len(df)} 条")
    return df

# ===================== 修复：行业涨跌排行（去掉标题脏行）=====================
def crawl_industry_rank():
    print("正在爬取行业涨跌排行...")
    today = get_today_date()
    try:
        resp = requests.get(URL_INDUSTRY, headers=HEADERS, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find("table").find_all("tr")

        data = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            txt = tds[0].get_text(strip=True)
            # 过滤掉标题行、脏数据
            if "涨跌%" in txt or "行业" in txt or len(txt) > 50:
                continue

            data.append([
                today,
                tds[0].get_text(strip=True),
                tds[1].get_text(strip=True),
                tds[2].get_text(strip=True)
            ])

        df = pd.DataFrame(data, columns=["统计日期", "行业", "涨跌幅", "家数"])
        print("✅ 行业涨跌排行抓取完成")
        return df
    except Exception as e:
        print(f"❌ 行业涨跌排行失败：{e}")
        return pd.DataFrame(columns=["统计日期","行业","涨跌幅","家数"])

# ===================== 爬取A股股东盈亏 =====================
def crawl_profit_data():
    print("正在爬取A股股东盈亏数据...")
    today = get_today_date()
    try:
        resp = requests.get(URL_PROFIT, headers=HEADERS, timeout=10)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find("table").find_all("tr")[1:]
        data = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 4:
                data.append([
                    today,
                    tds[0].get_text(strip=True),
                    tds[1].get_text(strip=True),
                    tds[2].get_text(strip=True),
                    tds[3].get_text(strip=True)
                ])
        df = pd.DataFrame(data, columns=["统计日期","原始日期","类型","股东数","占比"])
        print("✅ 股东盈亏数据抓取完成")
        return df
    except Exception as e:
        print(f"❌ 股东盈亏抓取失败：{e}")
        return pd.DataFrame(columns=["统计日期","原始日期","类型","股东数","占比"])

# ===================== 去重 =====================
def deduplicate(df_new):
    today = get_today_date()
    try:
        df_old = pd.read_excel(SAVE_FILE, sheet_name=0, dtype={"代码": str})
        if today in df_old["日期"].values:
            print(f"✅ 今日{today}数据已存在，跳过重复录入")
            return pd.DataFrame()
    except:
        pass
    return df_new

# ===================== 每日汇总 + 全市场总计 =====================
def generate_daily_summary(df_today):
    today = get_today_date()
    df = df_today.copy()
    df["涨跌幅_数值"] = pd.to_numeric(df["涨跌幅"].str.replace("%", ""), errors="coerce")
    limit_up   = df[df["涨跌幅_数值"] >= 9.8].copy()
    limit_down = df[df["涨跌幅_数值"] <= -9.8].copy()
    summary = []
    for ind in df["行业"].unique():
        up   = len(limit_up[limit_up["行业"] == ind])
        down = len(limit_down[limit_down["行业"] == ind])
        summary.append({"日期":today,"行业":ind,"涨停数":up,"跌停数":down,"合计涨跌停":up+down})
    # 全市场总计
    summary.append({
        "日期":today,"行业":"全市场总计",
        "涨停数":len(limit_up),"跌停数":len(limit_down),"合计涨跌停":len(limit_up)+len(limit_down)
    })
    df_summary = pd.DataFrame(summary)

    # 环比
    try:
        hist = pd.read_excel(SAVE_FILE, sheet_name="每日汇总统计", dtype=str)
        hist["涨停数"] = pd.to_numeric(hist["涨停数"], errors="coerce")
        hist["跌停数"] = pd.to_numeric(hist["跌停数"], errors="coerce")
        last_date = hist["日期"].max()
        last = hist[hist["日期"] == last_date]
        for i, row in df_summary.iterrows():
            match_last = last[last["行业"] == row["行业"]]
            l_up = match_last["涨停数"].iloc[0] if not match_last.empty else 0
            l_down = match_last["跌停数"].iloc[0] if not match_last.empty else 0

            df_summary.at[i, "涨停环比"] = f"{row['涨停数']-l_up:+}" if l_up == 0 else f"{row['涨停数']-l_up:+} ({(row['涨停数']-l_up)/l_up:.1%})"
            df_summary.at[i, "跌停环比"] = f"{row['跌停数']-l_down:+}" if l_down == 0 else f"{row['跌停数']-l_down:+} ({(row['跌停数']-l_down)/l_down:.1%})"
    except:
        df_summary["涨停环比"] = 0
        df_summary["跌停环比"] = 0
    return df_summary

# ===================== 连板 ≥2 板才记录 =====================
def update_limit_streak(df_today):
    today = get_today_date()
    df = df_today.copy()
    df["涨跌幅_数值"] = pd.to_numeric(df["涨跌幅"].str.replace("%", ""), errors="coerce")
    today_up = df[df["涨跌幅_数值"] >= 9.8].copy()
    try:
        streak = pd.read_excel(SAVE_FILE, sheet_name="连板记录", dtype={"代码": str})
    except:
        streak = pd.DataFrame(columns=["日期","代码","名称","行业","连板天数"])
    new_rows = []
    for _, s in today_up.iterrows():
        prev_days = streak[streak["代码"]==s["代码"]]["连板天数"].max()
        days = int(prev_days)+1 if pd.notna(prev_days) else 1
        new_rows.append({
            "日期":today,"代码":s["代码"],"名称":s["名称"],
            "行业":s["行业"],"连板天数":days
        })
    streak = pd.concat([streak, pd.DataFrame(new_rows)], ignore_index=True)
    streak = streak.drop_duplicates(subset=["日期","代码"], keep="last")
    streak = streak[streak["连板天数"] >= 2].copy()
    return streak

# ===================== 保存 =====================
def save_all(df_all, df_summary, df_streak, df_profit, df_industry):
    with pd.ExcelWriter(SAVE_FILE, engine="openpyxl") as writer:
        # 行业明细
        for ind in df_all["行业"].unique():
            df_all[df_all["行业"]==ind].to_excel(writer, sheet_name=ind, index=False)
        # 汇总
        df_summary.to_excel(writer, sheet_name="每日汇总统计", index=False)
        # 连板
        df_streak.to_excel(writer, sheet_name="连板记录", index=False)
        # 股东盈亏
        df_profit.to_excel(writer, sheet_name="A股股东盈亏", index=False)
        # 行业涨跌排行
        df_industry.to_excel(writer, sheet_name="行业涨跌排行", index=False)
    print("✅ 全部数据保存完成！")

# ===================== 主程序 =====================
if __name__ == "__main__":
    try:
        today = get_today_date()
        df_today    = crawl_limit_data()
        df_today    = deduplicate(df_today)
        if df_today.empty:
            time.sleep(2)
            exit()

        try:
            df_old = pd.read_excel(SAVE_FILE, sheet_name=0, dtype={"代码": str})
            df_all = pd.concat([df_old, df_today], ignore_index=True)
        except:
            df_all = df_today

        df_summary  = generate_daily_summary(df_today)
        df_streak   = update_limit_streak(df_today)
        df_profit   = crawl_profit_data()
        df_industry = crawl_industry_rank()

        save_all(df_all, df_summary, df_streak, df_profit, df_industry)

    except Exception as e:
        print(f"❌ 运行出错：{e}")
    time.sleep(3)
