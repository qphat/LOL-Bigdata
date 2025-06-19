import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Đọc danh sách PUUID từ file .txt
def read_puuids_from_file(file_path):
    with open(file_path, 'r') as file:
        # Đọc từng dòng và loại bỏ ký tự xuống dòng
        puuids = [line.strip() for line in file if line.strip()]
    return puuids

# Lấy match ID từ API
def get_match_ids(puuid, api_key, start=0, count=20):
    url = f"https://sea.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"

    # Tham số truy vấn
    params = {
        "start": start,
        "count": count
    }

    # Header chứa API key
    headers = {
        "X-Riot-Token": api_key
    }

    # Gửi yêu cầu GET
    response = requests.get(url, headers=headers, params=params)

    # Kiểm tra kết quả
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Lỗi khi lấy match ID cho PUUID {puuid}: {response.status_code} - {response.text}")
        return []

# Lưu danh sách match ID vào file JSON
def save_match_ids(puuid, match_ids, output_dir):
    output_dir.mkdir(exist_ok=True)  # Tạo thư mục nếu chưa tồn tại
    output_file = output_dir / f"{puuid}_match_ids.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(match_ids, f, ensure_ascii=False, indent=4)
    print(f"Đã lưu match ID cho PUUID {puuid} vào {output_file}")

# Thông tin cần thiết

api_key = os.getenv("RIOT_API_KEY")

file_path = "puuids.txt"  # Đường dẫn tới file chứa danh sách PUUID
output_dir = Path("matchID")  # Thư mục lưu file JSON

# Đọc danh sách PUUID từ file
puuids = read_puuids_from_file(file_path)

# Lấy match ID cho từng PUUID và lưu vào JSON
for puuid in puuids:
    print(f"\nĐang lấy match ID cho PUUID: {puuid}")
    match_ids = get_match_ids(puuid, api_key)
    if match_ids:
        save_match_ids(puuid, match_ids, output_dir)