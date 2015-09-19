pushd "%~dp0"
cd sqlite4dummy
python zzz_manual_install.py
cd ..
python create_doctree.py
make html