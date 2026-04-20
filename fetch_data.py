import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import sys

# ===================== 配置项 =====================
URL_LIMIT        = "https://gg.cfi.cn/data_ndkA0A1934A1935A60.html"
URL_INDUSTRY     = "https://gg.cfi.cn/cfi_datacontent_server.aspx?ndk=A0A1934A1935A39&client=pc"
URL_PROFIT       = "https://gg.cfi.cn/cfi_datacontent_server.aspx?ndk=A0A1934A1935A59&client=pc"
SAVE_FILE        = "行业涨跌停统计_每日追加.xlsx"

# 增强型请求头，减少被反爬几率
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://gg.cfi.cn/",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}

def get_today_date():
    return datetime.now().strftime("%Y-%m-%d")

# ===================== 1. 爬取行业涨跌停数据 =====================
def crawl_limit_data():
    print("正在爬取行业涨跌停数据...")
    try:
        resp = requests.get(URL_LIMIT, headers=HEADERS, timeout=20)
        resp.encoding = "utf-8"
        if resp.status_code != 200:
            print(f"⚠️ 网页响应异常，状态码：{resp.status_code}")
            return pd.DataFrame()
            
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
                        "换手率": row[12]
                    })
        df = pd.DataFrame(all_data)
        print(f"✅ 涨跌停数据抓取完成：{len(df)} 条")
        return df
    except Exception as e:
        print(f"❌ 抓取行业明细异常: {e}")
        return pd.DataFrame()

# ===================== 2. 行业涨跌排行 =====================
def crawl_industry_rank():
    print("正在爬取行业涨跌排行...")
    today = get_today_date()
    try:
        resp = requests.get(URL_INDUSTRY, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table: return pd.DataFrame()
        
        rows = table.find_all("tr")
        data = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 3: continue
            txt = tds[0].get_text(strip=True)
            if "涨跌%" in txt or "行业" in txt or len(txt) > 40: continue
            data.append([today, txt, tds[1].get_text(strip=True), tds[2].get_text(strip=True)])
        return pd.DataFrame(data, columns=["统计日期", "行业", "涨跌幅", "家数"])
    except:
        return pd.DataFrame()

# ===================== 3. A股股东盈亏 =====================
def crawl_profit_data():
    print("正在爬取A股股东盈亏数据...")
    try:
        resp = requests.get(URL_PROFIT, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find("table").find_all("tr")[1:]
        data = [[get_today_date()] + [td.get_text(strip=True) for td in tr.find_all("td")[:4]] for tr in rows if len(tr.find_all("td"))>=4]
        return pd.DataFrame(data, columns=["统计日期","原始日期","类型","股东数","占比"])
    except:
        return pd.DataFrame()

# ===================== 4. 数据汇总与连板逻辑 =====================
def process_extended_data(df_today):
    if df_today.empty or "涨跌幅" not in df_today.columns:
        return pd.DataFrame(), pd.DataFrame()

    today = get_today_date()
    df = df_today.copy()
    # 核心修复：处理非数值情况
    df["涨跌幅_数值"] = pd.to_numeric(df["涨跌幅"].str.replace("%", ""), errors="coerce")
    
    # 汇总统计
    limit_up = df[df["涨跌幅_数值"] >= 9.8]
    limit_down = df[df["涨跌幅_数值"] <= -9.8]
    summary = []
    for ind in df["行业"].unique():
        up = len(limit_up[limit_up["行业"] == ind])
        down = len(limit_down[limit_down["行业"] == ind])
        summary.append({"日期":today,"行业":ind,"涨停数":up,"跌停数":down,"合计":up+down})
    df_summary = pd.DataFrame(summary)

    # 连板计算（基于已有文件）
    streak_rows = []
    try:
        old_streak = pd.read_excel(SAVE_FILE, sheet_name="连板记录")
    except:
        old_streak = pd.DataFrame()

    for _, s in limit_up.iterrows():
        prev_days = 0
        if not old_streak.empty:
            match = old_streak[old_streak["代码"] == s["代码"]]
            if not match.empty: prev_days = match["连板天数"].max()
        streak_rows.append({"日期":today,"代码":s["代码"],"名称":s["名称"],"行业":s["行业"],"连板天数":int(prev_days)+1})
    
    df_streak = pd.DataFrame(streak_rows)
    if not df_streak.empty:
        df_streak = df_streak[df_streak["连板天数"] >= 2] # 仅记录2板及以上

    return df_summary, df_streak

# ===================== 5. 保存 =====================
def save_all(df_all, df_summary, df_streak, df_profit, df_industry):
    try:
        with pd.ExcelWriter(SAVE_FILE, engine="openpyxl") as writer:
            # 行业明细汇总页
            df_all.to_excel(writer, sheet_name="数据明细", index=False)
            # 也可以保留你原来的“按行业分 Sheet”逻辑
            for ind in df_all["行业"].unique():
                df_all[df_all["行业"]==ind].to_excel(writer, sheet_name=str(ind)[:30], index=False)
            
            df_summary.to_excel(writer, sheet_name="每日汇总统计", index=False)
            df_streak.to_excel(writer, sheet_name="连板记录", index=False)
            df_profit.to_excel(writer, sheet_name="A股股东盈亏", index=False)
            df_industry.to_excel(writer, sheet_name="行业涨跌排行", index=False)
        print("✅ 全部数据保存完成！")
    except Exception as e:
        print(f"❌ 保存Excel失败: {e}")

# ===================== 主程序 =====================
if __name__ == "__main__":
    df_today = crawl_limit_data()
    
    if df_today.empty:
        print("🛑 今日未抓取到有效数据，可能是非交易日或触发反爬。程序将跳过更新。")
        sys.exit(0)

    # 合并历史数据
    try:
        df_old = pd.read_excel(SAVE_FILE, sheet_name="数据明细", dtype={"代码": str})
        if get_today_date() in df_old["日期"].values:
            print("📅 今日数据已存在，不再重复保存。")
            sys.exit(0)
        df_all = pd.concat([df_old, df_today], ignore_index=True)
    except:
        df_all = df_today

    df_summary, df_streak = process_extended_data(df_today)
    df_profit = crawl_profit_data()
    df_industry = crawl_industry_rank()

    save_all(df_all, df_summary, df_streak, df_profit, df_industry)
