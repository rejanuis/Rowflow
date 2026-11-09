import pathlib
import sys

DAGS = pathlib.Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))
