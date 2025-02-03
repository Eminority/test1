import subprocess
import threading

# 파일 경로 설정
file1 = r"C:\Users\pc\Desktop\bdv\EFB\EFB(Extracting_From_Bithumb).rev0.03\Main.py"
file2 = r"C:\Users\pc\Desktop\bdv\Coin_Dashboard\Coin_Dashboard.rev1.00\MAIN\Main.py"
file3 = r"C:\Users\pc\Desktop\bdv\GUI\gui.rev0.01\gui.py"

def run_first_file_and_wait_for_message(file_path, target_message):
    """
    첫 번째 파일을 실행하고 특정 메시지가 출력될 때까지 기다림
    """
    with subprocess.Popen(
        ["python", file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    ) as process:
        for line in process.stdout:
            print(line, end="")  # 첫 번째 파일의 출력을 실시간으로 보여줌
            if target_message in line:
                print("\n[INFO] 메시지 감지됨: 두 번째 및 세 번째 파일 실행 시작...\n")
                return process  # 메시지 감지 후 반환

def run_other_files(file2, file3):
    """
    두 번째와 세 번째 파일을 동시에 실행
    """
    t2 = threading.Thread(target=subprocess.run, args=(["python", file2],))
    t3 = threading.Thread(target=subprocess.run, args=(["python", file3],))
    t2.start()
    t3.start()
    t2.join()
    t3.join()

def main():
    # 첫 번째 파일에서 출력될 메시지 설정
    target_message = "데이터 업데이트 완료. 다음 업데이트까지 대기 중..."
    
    print("[INFO] 첫 번째 파일 실행 중...")
    process1 = run_first_file_and_wait_for_message(file1, target_message)
    
    # 두 번째와 세 번째 파일 실행
    run_other_files(file2, file3)
    
    # 첫 번째 파일 종료 대기 (옵션)
    process1.wait()
    print("[INFO] 모든 파일 실행이 완료되었습니다.")

if __name__ == "__main__":
    main()
