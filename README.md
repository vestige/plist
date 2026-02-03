## 前提
* 動かすのにPython3が必要

## サーバーの起動

    python -m uvicorn main:app --reload --port 8000

## ブラウザでアクセス
* http://127.0.0.1:8000/ui/assets

## データについて
* bulk_load_sqlite.py

### 全削除
    python bulk_load_sqlite.py --db equip.db --wipe

### CSVデータの登録
    python bulk_load_sqlite.py --db equip.db --wipe --csv .\test\equip_import_test_en.csv

### CSVデータの追記
    python bulk_load_sqlite.py --db equip.db --csv .\test\equip_import_hogehoge.csv

## テスト

    python -m pytest
