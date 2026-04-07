@echo off
git config --global user.email "mesquinez@gmail.com"
git config --global user.name "mesquinez"
git remote remove origin
git remote add origin "https://github.com/mesquinez/Contratos_SIGA.git"
git branch -M main
git add .
git commit -m "Initial commit"
git push -u origin main
