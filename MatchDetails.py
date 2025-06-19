import asyncio
import aiohttp
import json
import logging
from pathlib import Path
from itertools import cycle
from ratelimit import limits, sleep_and_retry
import os
from dotenv import load_dotenv

# Thiết lập log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("riot_api")

load_dotenv()

api_key = os.getenv("RIOT_API_KEY")

# Giới hạn API: 20 requests mỗi giây, 100 requests mỗi 2 phút
CALLS_PER_SECOND = 20
PERIOD_PER_SECOND = 1
CALLS_PER_2MIN = 100
PERIOD_2MIN = 120


# Hàm lấy chi tiết một trận đấu với giới hạn rate limit
@sleep_and_retry
@limits(calls=CALLS_PER_SECOND, period=PERIOD_PER_SECOND)
@limits(calls=CALLS_PER_2MIN, period=PERIOD_2MIN)
async def fetch_match(session, match_id):
    url = f"https://sea.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": next(api_key)}

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                logger.info(f"Fetched match {match_id} successfully")
                # Lưu dữ liệu vào file JSON trong thư mục matchDetails
                output_dir = Path("matchDetails")
                output_dir.mkdir(exist_ok=True)
                output_file = output_dir / f"{match_id}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                return match_id, data
            else:
                text = await resp.text()
                logger.warning(f"Failed match {match_id} - Status: {resp.status}, Body: {text}")
                return match_id, None
    except Exception as e:
        logger.error(f"Exception fetching match {match_id}: {e}")
        return match_id, None


# Hàm xử lý nhiều trận đấu cùng lúc
async def fetch_matches(match_ids):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_match(session, match_id) for match_id in match_ids]
        return await asyncio.gather(*tasks)


# Đọc danh sách match ID từ file JSON
def read_match_ids_from_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            match_ids = json.load(f)
            if isinstance(match_ids, list):
                return match_ids
            else:
                logger.error(f"File {file_path} không chứa danh sách match ID hợp lệ")
                return []
    except Exception as e:
        logger.error(f"Lỗi khi đọc file {file_path}: {e}")
        return []


# Duyệt qua các file JSON trong thư mục và lấy chi tiết trận đấu
async def process_json_files(json_dir):
    json_dir = Path(json_dir)
    if not json_dir.exists():
        logger.error(f"Thư mục {json_dir} không tồn tại")
        return

    for json_file in json_dir.glob("*.json"):
        logger.info(f"Đang xử lý file: {json_file}")
        match_ids = read_match_ids_from_json(json_file)
        if match_ids:
            logger.info(f"Tìm thấy {len(match_ids)} match ID trong {json_file}")
            results = await fetch_matches(match_ids)
            for match_id, result in results:
                if result:
                    logger.info(f"Đã lưu dữ liệu của match {match_id} vào matchDetails/{match_id}.json")
                else:
                    logger.warning(f"Không thể lấy dữ liệu của match {match_id}")


# Chạy chương trình
if __name__ == "__main__":
    json_dir = "match_ids"  # Thư mục chứa các file JSON
    asyncio.run(process_json_files(json_dir))