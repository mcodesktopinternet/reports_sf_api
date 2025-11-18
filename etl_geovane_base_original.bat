@echo off
chcp 65001

echo Ativando ambiente virtual...
call venv\Scripts\activate.bat

python etl_geovane_base_original.py

