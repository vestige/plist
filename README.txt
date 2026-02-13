## はじめに
* FastAPIを使って試すのにつくったなんちゃって備品管理

## 前提
* 動かすのにPython3が必要
* FastAPI と uvicorn をインストールが必要

## サーバーの起動

    python -m uvicorn main:app --reload --port 1234

## ブラウザでアクセス
* http://127.0.0.1:1234/ui/assets

## データについて
* bulk_load_sqlite.py

### 全削除
    python bulk_load_sqlite.py --db .\data\equip.db --wipe

### CSVデータの登録
    python bulk_load_sqlite.py --db .\data\equip.db --wipe --csv .\tests\equip_import_test_en.csv

### CSVデータの追記
    python bulk_load_sqlite.py --db .\data\equip.db --csv .\tests\equip_import_hogehoge.csv

## テスト

    python -m pytest


## 配布について
> pyinstaller -y --noconfirm `
  --name app `
  --onedir `
  --add-data "templates;templates" `
  --add-data "static;static" `
  main.py

### そのあと
> Copy-Item -Recurse -Force dist\app\* plist-dist\app\
