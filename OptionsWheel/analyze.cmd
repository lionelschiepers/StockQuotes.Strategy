@echo off
setlocal
pushd "%~dp0"

echo === Upgrading pip ===
python -m pip install --upgrade pip --quiet
if errorlevel 1 goto :error

echo.
echo === Installing required packages ===
python -m pip install -r "%~dp0requirements.txt" --quiet
if errorlevel 1 goto :error

echo.
echo === Analyzing CALL options ===
python analyze_calls.py
if errorlevel 1 goto :error

echo.
echo === Analyzing PUT options ===
python analyze_puts.py
if errorlevel 1 goto :error

echo.
echo === Starting web server ===
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8000/web/"
python -m http.server 8000
if errorlevel 1 goto :error

popd
exit /b 0

:error
set "exit_code=%errorlevel%"
popd
exit /b %exit_code%
