@echo off
setlocal
cd /d F:\AI\copytrade_v5_muti-clob-v2
python tools\nav_value_snapshot.py --root . --with-24h-report >> logs\nav_snapshot_hourly_task.log 2>&1
endlocal
