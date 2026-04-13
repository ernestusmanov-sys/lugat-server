@echo off
chcp 65001 > nul
title Загрузка словаря на сервер

echo ================================================
echo   Lügat — Загрузка словаря на сервер
echo ================================================
echo.

set /p SERVER_URL="Введите URL сервера (например https://lugat.onrender.com): "
set /p ADMIN_TOKEN="Введите ADMIN_TOKEN (ваш пароль): "
set /p DB_PATH="Путь к файлу dictionary.db (Enter = текущая папка): "

if "%DB_PATH%"=="" set DB_PATH=dictionary.db

if not exist "%DB_PATH%" (
    echo [ОШИБКА] Файл %DB_PATH% не найден!
    pause
    exit /b 1
)

echo.
echo Загружаем %DB_PATH% на %SERVER_URL% ...
echo.

curl -X POST "%SERVER_URL%/api/upload_dict" ^
     -H "X-Admin-Token: %ADMIN_TOKEN%" ^
     -F "file=@%DB_PATH%" ^
     --progress-bar

echo.
echo Готово! Проверьте результат выше.
pause
