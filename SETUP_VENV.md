# Setup Virtual Environment

Windows (PowerShell):

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Windows (cmd):

```bat
py -m venv .venv
.\.venv\Scripts\activate.bat
pip install -r requirements.txt
python app.py
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```
