import json

def extract_puuids(input_file: str, output_file: str):
    try:
        # Đọc file JSON
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Lấy danh sách puuid từ entries
        entries = data.get('entries', [])
        puuids = [entry['puuid'] for entry in entries if 'puuid' in entry]

        # In ra console
        print("Danh sách PUUID:")
        for puuid in puuids:
            print(puuid)

        # Ghi vào file output
        with open(output_file, 'w', encoding='utf-8') as out:
            for puuid in puuids:
                out.write(puuid + '\n')

        print(f"\nĐã lưu {len(puuids)} PUUID vào '{output_file}'.")

    except FileNotFoundError:
        print(f"Không tìm thấy file '{input_file}'.")
    except json.JSONDecodeError:
        print(f"File '{input_file}' không đúng định dạng JSON.")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

# Gọi hàm với file đầu vào và đầu ra
if __name__ == "__main__":
    extract_puuids('profile.json', 'puuids.txt')
