echo '----------------------------------------------------------------------'
echo '                        RUNNING PYTHON 2 TESTS                        '
echo '----------------------------------------------------------------------'
python2 --version
python2 baseTaskTest.py
python2 assemblerTest.py
python2 parserTest.py

echo '----------------------------------------------------------------------'
echo '                        RUNNING PYTHON 3 TESTS                        '
echo '----------------------------------------------------------------------'
python3 --version
python3 baseTaskTest.py
python3 assemblerTest.py
python3 parserTest.py
