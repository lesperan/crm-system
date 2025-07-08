import subprocess
import webbrowser
import time
import threading

def run_app():
    print("🚀 CRM 시스템을 시작합니다...")
    print("📡 서버 주소: http://localhost:8501")
    print("⏹️  종료하려면 Ctrl+C를 누르세요")
    
    # 브라우저 자동 열기
    def open_browser():
        time.sleep(3)
        webbrowser.open('http://localhost:8501')
    
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    # Streamlit 실행
    subprocess.run(['streamlit', 'run', 'crm_app.py', '--server.port=8501'])

if __name__ == "__main__":
    run_app()
