import shutil
from pathlib import Path

import dagster as dg


@dg.asset(name="raw-csv-import")
def raw_csv_import(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
    Copy raw CSV files from Downloads into `data/raw/`.

    Matches: `4340561S033*.csv`
    """

    downloads_dir = Path("/Users/daviddasilva/Downloads")
    filename_pattern = "4340561S033*.csv"

    # Resolve repo root by locating `compte_b/pyproject.toml` in parent dirs.
    this_file = Path(__file__).resolve()
    repo_root = next(
        (
            parent
            for parent in this_file.parents
            if (parent / "compte_b" / "pyproject.toml").exists()
        ),
        None,
    )
    if repo_root is None:
        raise RuntimeError("Could not locate repo root (missing compte_b/pyproject.toml)")
    destination_dir = repo_root / "compte_b" / "data" / "raw"

    destination_dir.mkdir(parents=True, exist_ok=True)

    matches = sorted(downloads_dir.glob(filename_pattern))
    if not matches:
        raise FileNotFoundError(
            f"No files found in {downloads_dir} matching pattern {filename_pattern}"
        )

    copied_files: list[str] = []
    for src_path in matches:
        dst_path = destination_dir / src_path.name
        shutil.copy2(src_path, dst_path)
        copied_files.append(src_path.name)

    context.log.info(
        f"Copied {len(copied_files)} file(s) from {downloads_dir} to {destination_dir}"
    )

    return dg.MaterializeResult(
        metadata={
            "copied_count": dg.MetadataValue.int(len(copied_files)),
            "destination_dir": dg.MetadataValue.path(str(destination_dir)),
            "copied_files": dg.MetadataValue.json(copied_files),
        }
    )
