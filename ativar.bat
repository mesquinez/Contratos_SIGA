@echo off
:: Script para ativar o ambiente virtual e abrir o CMD na pasta do projeto
if exist .venv\Scripts\activate.bat (
    echo [OK] Ativando ambiente .venv...
    call .venv\Scripts\activate.bat
    :: O comando abaixo mantem o terminal aberto e ativo
    cmd /k
) else (
    echo [ERRO] O ambiente .venv nao foi encontrado!
    echo Execute: python -m venv .venv antes de usar este script.
    pause
)
