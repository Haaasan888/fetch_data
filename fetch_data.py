#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
from datetime import datetime
from typing import List, Dict, Set

# ===================== 配置区域 =====================
START_DATE = "20260401"  # 开始日期 YYYYMMDD
END_DATE = "20260420"    # 结束日期 YYYYMMDD
BASE_URL = "https://gg.cfi.cn/data_ndkA0A1934A1935A36.html"
OUTPUT_CSV = "a_share_daily.csv"
REQUEST_DELAY = (1, 3)   # 随机延迟范围（秒）
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]
# ===================================================

def get_existing_dates(filename: str) -> Set[str]:
    """从现有CSV中读取所有日期（格式 YYYY-MM-DD）"""
    if not os.path.exists(filename):
        return set()
    with open(filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        dates = {row.get('日期', '') for row in reader if row.get('日期')}
    return dates

def fetch_page_data(td: str, page: int) -> List[Dict]:
    """抓取单个日期的单个页面数据，返回解析后的记录列表"""
    url = f"{BASE_URL}?curpage={page}&td={td}"
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': BASE_URL,
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='tabData')
        if not table:
            return []
        records = []
        for row in table.find_all('tr')[1:]:  # 跳过表头
            cols = row.find_all('td')
            if len(cols) < 6:
                continue
            code_tag = cols[0].find('a')
            name_tag = cols[1].find('a')
            code = code_tag.text.strip() if code_tag else ''
            name = name_tag.text.strip() if name_tag else ''
            chg_tag = cols[2]
            change = chg_tag.text.strip() if chg_tag else ''
            # 提取期初收盘价、期末收盘价、涨跌额
            price_start = cols[3].text.strip()
            price_end = cols[4].text.strip()
            change_amount = cols[5].text.strip()
            # 所属行业
            industry_tag = cols[6].find('a')
            industry = industry_tag.text.strip() if industry_tag else cols[6].text.strip()
            # 格式化日期（YYYYMMDD -> YYYY-MM-DD）
            formatted_date = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
            records.append({
                '日期': formatted_date,
                '股票代码': code,
                '股票名称': name,
                '日涨幅%': change,
                '期初收盘价': price_start,
                '期末收盘价': price_end,
                '日涨跌': change_amount,
                '所属行业': industry,
            })
        return records
    except Exception as e:
        print(f"  请求异常: {e}，日期 {td}，第 {page} 页")
        return []

def fetch_one_day(td: str) -> List[Dict]:
    """抓取指定日期的所有分页数据"""
    all_records = []
    page = 1
    while True:
        print(f"    正在抓取第 {page} 页...")
        records = fetch_page_data(td, page)
        if not records:
            break
        all_records.extend(records)
        print(f"      第 {page} 页获取 {len(records)} 条，累计 {len(all_records)} 条")
        # 检查是否有下一页（通过检查分页区域是否存在当前页码+1的链接）
        url = f"{BASE_URL}?curpage={page+1}&td={td}"
        try:
            resp = requests.get(url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=15)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.text, 'html.parser')
            # 如果下一页的表格为空或没有新数据，则终止
            if not soup.find('table', id='tabData').find_all('tr')[1:]:
                break
        except:
            break
        page += 1
        time.sleep(random.uniform(*REQUEST_DELAY))
    return all_records

def date_range(start_date: str, end_date: str) -> List[str]:
    """生成日期字符串列表 (YYYYMMDD)"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]

def save_to_csv(rows: List[Dict], filename: str, append: bool = True):
    if not rows:
        print("没有新数据需要保存。")
        return
    mode = "a" if append else "w"
    fieldnames = ['日期', '股票代码', '股票名称', '日涨幅%', '期初收盘价', '期末收盘价', '日涨跌', '所属行业']
    write_header = not os.path.exists(filename) or not append
    with open(filename, mode, newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    print(f"已保存 {len(rows)} 条记录到 {filename}（模式：{'追加' if append else '覆盖'}）")

def sort_and_dedupe_csv(filename: str):
    """按日期升序排序并基于（日期+股票代码）去重"""
    if not os.path.exists(filename):
        return
    with open(filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return
    seen = set()
    unique_rows = []
    for row in rows:
        key = (row.get('日期', ''), row.get('股票代码', ''))
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)
    unique_rows.sort(key=lambda x: x.get('日期', ''))
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(unique_rows)
    print(f"排序+去重完成：原 {len(rows)} 条 → {len(unique_rows)} 条")

def main():
    print(f"开始处理 {START_DATE} 至 {END_DATE} 的A股日涨跌数据...")
    existing_dates = get_existing_dates(OUTPUT_CSV)
    if existing_dates:
        print(f"CSV中已存在 {len(existing_dates)} 个日期的数据，将跳过这些日期")
    all_dates = date_range(START_DATE, END_DATE)
    missing_dates = [d for d in all_dates if f"{d[:4]}-{d[4:6]}-{d[6:8]}" not in existing_dates]
    print(f"总日期数: {len(all_dates)}，需要抓取的日期: {len(missing_dates)}")
    if not missing_dates:
        print("所有日期均已存在，无需抓取。")
        sort_and_dedupe_csv(OUTPUT_CSV)
        return
    all_new_rows = []
    for i, d in enumerate(missing_dates, 1):
        print(f"\n[{i}/{len(missing_dates)}] 正在处理日期 {d}")
        day_records = fetch_one_day(d)
        if day_records:
            all_new_rows.extend(day_records)
            print(f"  日期 {d} 共获取 {len(day_records)} 条记录")
        else:
            print(f"  日期 {d} 无数据")
        time.sleep(random.uniform(*REQUEST_DELAY))
    print(f"\n本次新获取 {len(all_new_rows)} 条记录")
    if all_new_rows:
        save_to_csv(all_new_rows, OUTPUT_CSV, append=True)
        sort_and_dedupe_csv(OUTPUT_CSV)
    else:
        print("未获取到任何新数据。")

if __name__ == "__main__":
    from datetime import timedelta
    main()
