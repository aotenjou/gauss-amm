"""Entry point for SQL collection against the generated local openGauss instance."""

from pathlib import Path
import sys

from sqlGEM import main


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    config = root / "config" / "gaussdb-15432.json"
    if "--config" not in sys.argv[1:] and not any(arg.startswith("--config=") for arg in sys.argv[1:]):
        sys.argv[1:1] = ["--config", str(config)]
    main()
