import json
import pandas as pd
from pandas import json_normalize
import os
import logging
from multiprocessing import Pool
from tqdm import tqdm

# Thiết lập logging để ghi lỗi
logging.basicConfig(
    filename='../process_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Danh sách cột được chọn, thêm teamId
selected_columns = [
    'teamId', 'teamPosition', 'championName', 'win',
    'kills', 'deaths', 'assists', 'kda', 'visionScore'
]


def process_json_file(file_path):
    try:
        # Đọc file JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Lấy dữ liệu người chơi
        participants = data['info']['participants']
        df = json_normalize(participants)

        # Thêm matchId từ tên file
        match_id = os.path.basename(file_path).replace('.json', '')
        df['matchId'] = match_id

        # Lọc các cột cần thiết
        available_columns = [col for col in selected_columns if col in df.columns]
        result_df = df[available_columns + ['matchId']].copy()  # Tạo bản sao rõ ràng

        # Chuyển win thành bool một cách an toàn
        if 'win' in result_df.columns:
            result_df.loc[:, 'win'] = result_df['win'].astype(bool)

        return result_df
    except Exception as e:
        logging.error(f"Lỗi khi xử lý {file_path}: {e}")
        return None


def main():
    # Thư mục chứa file JSON
    json_dir = '../matchDetails'
    json_files = [os.path.join(json_dir, f) for f in os.listdir(json_dir) if f.endswith('.json')]

    # Kiểm tra số lượng file
    print(f"📁 Tìm thấy {len(json_files)} file JSON để xử lý.")

    # Sử dụng multiprocessing để xử lý song song
    with Pool() as pool:
        results = list(tqdm(pool.imap(process_json_file, json_files), total=len(json_files), desc="Processing files"))

    # Gộp các DataFrame hợp lệ
    dfs = [df for df in results if df is not None]
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_csv('match_stats.csv', index=False, encoding='utf-8')
        print(f"✅ Đã xuất {len(final_df)} hàng dữ liệu ra match_stats.csv")
        print(f"📊 Số trận đấu duy nhất: {final_df['matchId'].nunique()}")
    else:
        print("⚠️ Không có dữ liệu để xuất. Kiểm tra process_errors.log để biết chi tiết.")


if __name__ == '__main__':
    main()