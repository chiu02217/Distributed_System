@echo off
setlocal

:: set paths
set PROJECT_ROOT=%~dp0..
set PROTO_DIR=%PROJECT_ROOT%\src\common\grpc\protos
set OUTPUT_DIR=%PROJECT_ROOT%\src\common\grpc\auto_generated

::  directory check
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

:: clear legacy files
del /Q "%OUTPUT_DIR%\*.py" 2>nul
del /Q "%OUTPUT_DIR%\*.pyi" 2>nul

:: compile .proto files
echo Compiling .proto files...
python -m grpc_tools.protoc ^
    -I"%PROTO_DIR%\messages" ^
    -I"%PROTO_DIR%\services" ^
    --python_out="%OUTPUT_DIR%" ^
    --grpc_python_out="%OUTPUT_DIR%" ^
    --pyi_out="%OUTPUT_DIR%" ^
    "%PROTO_DIR%\messages\*.proto" ^
    "%PROTO_DIR%\services\*.proto"

::  fix relative imports
echo Patching generated files for relative imports...
powershell -Command "Get-ChildItem -Path '%OUTPUT_DIR%\file_operation_service_*.py*' | ForEach-Object { (Get-Content -Path $_.FullName -Raw) -replace 'import file_operation_message_pb2', 'from . import file_operation_message_pb2' | Set-Content -Path $_.FullName -Encoding utf8 }"
::  create __init__.py
:: auto-generate __init__.py to make the directory a package
echo. > "%OUTPUT_DIR%\__init__.py"

echo Proto files compiled successfully!
echo Output directory: %OUTPUT_DIR%

endlocal