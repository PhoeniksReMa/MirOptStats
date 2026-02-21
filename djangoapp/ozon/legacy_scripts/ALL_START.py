# ALL_START.py
import subprocess
import sys
from pathlib import Path

SCRIPTS = [
    "021.FBS_ALL_ZAKAZ.py",
    "000.ID_Cluster_FBO.py",
    "001.Product_offer_id.py",
    "002.Tovar_info.py",
    "003.Content_rating.py",
    "004.Kategory.py",
    "005.Gabarits.py",
    "006.Gabarits_error.py",
    "007.FBO_Upravlenie_stocks_28.py",
    "008.Edet_na_FBO.py",
    "009.Stock_FBO_FBS.py",
    "010.Hranenie.py",
    "011.Zakazi_FBO.py",
    "012.Zakazi_FBS.py",
    "013.Otmena_vozvrat.py",
    "016.Price_logistic.py",
    "017.Analytics_base.py",
    "018.Analytics_keywords.py",
    "020.FBO_Dinamyc.py",
]

BASE_DIR = Path(__file__).resolve().parent


def run_script(script_name: str) -> None:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Не найден файл: {script_path}")

    print(f"\n=== START: {script_name} ===")
    # Запускаем тем же интерпретатором Python, что и ALL_START.py
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Скрипт упал с кодом {result.returncode}: {script_name}")

    print(f"=== OK: {script_name} ===")


def main():
    print(f"Runner: {BASE_DIR}")
    for s in SCRIPTS:
        run_script(s)
    print("\n✅ ВСЕ СКРИПТЫ УСПЕШНО ВЫПОЛНЕНЫ")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        sys.exit(1)
