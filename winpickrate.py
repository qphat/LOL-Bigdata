import json
import os
import pandas as pd
from multiprocessing import Pool
from pathlib import Path


def process_json_file(file_path):
    """Xử lý một file JSON và trích xuất championName, win, teamPosition."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        results = []
        for participant in data['info']['participants']:
            results.append({
                'championName': participant['championName'],
                'win': participant['win'],
                'teamPosition': participant.get('teamPosition', 'Unknown')
            })
        return results
    except Exception as e:
        print(f"Lỗi khi xử lý {file_path}: {e}")
        return []


def main():
    # Thư mục chứa các file JSON
    json_dir = Path('matchId')  # Đường dẫn thư mục
    output_csv = 'match_stats.csv'

    # Lấy danh sách file JSON
    json_files = list(json_dir.glob('*.json'))
    print(f"Tìm thấy {len(json_files)} file JSON.")

    # Sử dụng multiprocessing để xử lý song parallel
    with Pool() as pool:
        all_results = pool.map(process_json_file, json_files)

    # Gộp kết quả
    flattened_results = [item for sublist in all_results for item in sublist]

    # Tạo DataFrame và lưu vào CSV
    df = pd.DataFrame(flattened_results)
    df.to_csv(output_csv, index=False)
    print(f"Đã lưu dữ liệu vào {output_csv}")


if __name__ == '__main__':
    main()