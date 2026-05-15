@echo off
echo === Analyzing CALL options ===
python analyze_calls.py
echo.
echo === Analyzing PUT options ===
python analyze_puts.py
echo.
echo === Starting web server ===
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8000/web/"
python -m http.server
