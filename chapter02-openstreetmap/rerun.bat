@echo on
if exist nodes-* del nodes-*
if exist ways-* del ways-*
if exist ways.pq del ways.pq

@echo "====================================STARTOF 02_parsefile"
py 02_parsefile.py
@echo "====================================ENDOF 02_parsefile"

@echo "====================================STARTOF 03_bloomfilterindices.py"
py 03_bloomfilterindices.py
@echo "====================================ENDOF 03_bloomfilterindices.py"

@echo "====================================STARTOF 03_distributeways.py"
py 03_distributeways.py
@echo "====================================ENDOF 03_distributeways.py"
 


