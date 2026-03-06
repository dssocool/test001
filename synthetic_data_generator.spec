# PyInstaller spec for Windows single exe. Run: pyinstaller synthetic_data_generator.spec
# From project root. Requires: pip install pyinstaller

import os

block_cipher = None

# Add app templates and static so the frozen app can find them
# Windows uses ';' for path separator in add-data, PyInstaller expects (source, dest)
app_dir = os.path.join('app')
datas = [
    (os.path.join(app_dir, 'templates'), 'app/templates'),
    (os.path.join(app_dir, 'static'), 'app/static'),
]

a = Analysis(
    ['run_local.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'flask',
        'werkzeug',
        'jinja2',
        'pyodbc',
        'azure.storage.blob',
        'msal',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='SyntheticDataGenerator',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Set to False for --windowed (no console window)
)
