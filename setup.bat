@echo off
setlocal EnableDelayedExpansion

REM Step 1: If there’s an existing .env, load and maybe skip setup
if exist .env (
    for /f "tokens=1,2 delims==" %%A in (".env") do (
        set "%%A=%%B"
    )

    if defined CREATE_NEW_ENV if defined DAGSTER_HOME if defined WAREHOUSE_PATH (
        if /i "!CREATE_NEW_ENV!"=="FALSE" (
            echo Existing valid .env configuration found, skipping setup...
            echo Starting Dagster development server...
            dagster dev
            exit /b 0
        )
    )

    if /i "!CREATE_NEW_ENV!"=="TRUE" (
        del .env 2>nul
    )
)

REM Step 2: Create the virtual environment
uv venv

IF NOT EXIST ".venv\Scripts\activate" (
    echo Error: Virtual environment not found at .venv\Scripts\activate
    exit /b 1
)

REM Step 3: Activate it
call .venv\Scripts\activate || (
    echo Error: Failed to activate virtual environment
    exit /b 1
)

REM Step 4: Install dependencies
uv sync
IF ERRORLEVEL 1 (
    echo Error: Failed to sync dependencies
    exit /b 1
)

REM Step 5: Prepare a fresh .env
del .env 2>nul
echo. > .env

REM Step 6: Pull in WAREHOUSE_PATH and DAGSTER_HOME
set i=0
for /f "delims=" %%L in ('uv run scripts\exportpathwindows.py') do (
    if !i! equ 0 (
        set "WAREHOUSE_PATH=%%L"
    ) else (
        set "DAGSTER_HOME=%%L"
    )
    set /a i+=1
)

if not defined WAREHOUSE_PATH (
    echo Error: Failed to retrieve WAREHOUSE_PATH
    exit /b 1
)
if not defined DAGSTER_HOME (
    echo Error: Failed to retrieve DAGSTER_HOME
    exit /b 1
)

REM Step 7: Write the core env vars
>> .env echo WAREHOUSE_PATH=!WAREHOUSE_PATH!
>> .env echo DAGSTER_HOME=!DAGSTER_HOME!
>> .env echo CREATE_NEW_ENV=FALSE

REM Step 8: Generate dagster.yaml in the target folder
if not exist "!DAGSTER_HOME!" mkdir "!DAGSTER_HOME!"
uv run scripts\generate_dagsteryaml.py "!DAGSTER_HOME!" > "!DAGSTER_HOME!\dagster.yaml"

REM Step 9: Launch Dagster
echo Starting Dagster development server...
dagster dev
