#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
import re
from typing import List, Dict, Set

BASE_URL = "https://gg.cfi.cn/data_ndkA0A1934A1935A36.html"
OUTPUT_CSV = "a_share_daily.csv"
REQUEST_DELAY = (0.5, 1.5)
MAX_RETRIES = 3
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]

def get_latest_trade_date() -> str:
    """从页面标题中提取最新交易日，返回 YYYY-MM-DD 格式"""
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(BASE_URL, headers=headers, timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            title_tag = soup.find('div', class_='ptitle')
            if not title_tag:
                raise Exception("未找到标题")
            title_text = title_tag.get_text(strip=True)
            match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', title_text)
            if not match:
                raise Exception(f"无法提取日期: {title_text}")
            year, month, day = match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"
        except Exception as e:
            print(f"  获取交易日失败，重试 {attempt+1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                raise

def get_total_pages(estimated_total_stocks=5700, page_size=100) -> int:
    """
    估算总页数
    estimated_total_stocks: 预估的 A 股总数（可微调）
    page_size: 每页显示的股票数（固定为 100）
    """
    return (estimated_total_stocks + page_size - 1) // page_size

def fetch_page_data(page: int, latest_date: str) -> List[Dict]:
    """
    使用正确的分页参数 pgnum 抓取指定页面数据
    返回记录列表
    """
    url = f"{BASE_URL}?pgnum={page}"
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', id='tabData')
            if not table:
                return []
            rows = table.find_all('tr')[1:]
            records = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 7:
                    continue
                # 提取数据
                code_tag = cols[0].find('a')
                name_tag = cols[1].find('a')
                code = code_tag.text.strip() if code_tag else cols[0].text.strip()
                name = name_tag.text.strip() if name_tag else cols[1].text.strip()
                change = cols[2].text.strip()
                price_start = cols[3].text.strip()
                price_end = cols[4].text.strip()
                change_amount = cols[5].text.strip()
                industry_tag = cols[6].find('a')
                industry = industry_tag.text.strip() if industry_tag else cols[6].text.strip()
                records.append({
                    '日期': latest_date,
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
            print(f"  第 {page} 页请求失败，重试 {attempt+1}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                return []
    return []

def fetch_all_pages(latest_date: str) -> List[Dict]:
    """估算总页数并抓取所有分页数据"""
    total_pages = get_total_pages()
    print(f"根据 A 股总数估算总页数为: {total_pages}")
    all_records = []
    for page in range(1, total_pages + 1):
        print(f"  正在抓取第 {page}/{total_pages} 页...")
        records = fetch_page_data(page, latest_date)
        if records:
            all_records.extend(records)
            print(f"    第 {page} 页获取 {len(records)} 条，累计 {len(all_records)} 条")
        else:
            print(f"    第 {page} 页无数据，可能已结束")
            # 连续两页无数据就停止抓取
            if page > 1 and len(all_records) == 0:
                break
        time.sleep(random.uniform(*REQUEST_DELAY))
    return all_records

def get_existing_dates(filename: str) -> Set[str]:
    """读取CSV中已存在的日期"""
    if not os.path.exists(filename):
        return set()
    with open(filename, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return {row.get('日期', '') for row in reader if row.get('日期')}

def save_to_csv(rows: List[Dict], filename: str, append: bool = True):
    if not rows:
        print("没有新数据需要保存")
        return
    mode = "a" if append else "w"
    fieldnames = ['日期', '股票代码', '股票名称', '日涨幅%', '期初收盘价', '期末收盘价', '日涨跌', '所属行业']
    write_header = not os.path.exists(filename) or not append
    with open(filename, mode, newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    print(f"已保存 {len(rows)} 条记录到 {filename}")

def sort_and_dedupe_csv(filename: str):
    """按日期升序排序并去重"""
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
    print("开始获取最新交易日数据...")
    latest_date = get_latest_trade_date()
    print(f"最新交易日: {latest_date}")

    existing_dates = get_existing_dates(OUTPUT_CSV)
    if latest_date in existing_dates:
        print(f"日期 {latest_date} 的数据已存在，无需抓取")
        return

    print(f"开始抓取 {latest_date} 的数据...")
    records = fetch_all_pages(latest_date)
    print(f"共抓取 {len(records)} 条记录")

    if records:
        save_to_csv(records, OUTPUT_CSV, append=True)
        sort_and_dedupe_csv(OUTPUT_CSV)
    else:
        print("未获取到任何数据")

if __name__ == "__main__":
    main()
