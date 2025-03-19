from pathlib import Path
import shutil
import argparse


def create_new_die(job_name: str, template_dir: Path, destination_dir: Path) -> None:
    """
    Creates a new DIE directory based on the template files.

    Args:
        job_name: The name of the new DIE (without the 'DIE_' prefix on the job directory).
        template_dir: The path to the directory containing the DIE template files (i.e., main.py and query.sql).
        destination_dir: The parent directory of the new DIE directory.

    Raises:
        OSError: If shutil could not copy template directory tree to specified location.
    """
    new_die_path = destination_dir / f"DIE_{job_name}"

    if new_die_path.exists():
        raise ValueError(
            f"DIE directory '{new_die_path}' already exists. Choose a different job name."
        )

    try:
        shutil.copytree(
            template_dir, new_die_path, ignore=shutil.ignore_patterns("test_main.py")
        )
        print(f"Created new DIE: {new_die_path}")
    except OSError as e:
        print(f"Error copying template directory: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error has occured: {e}")
        exit(1)


def main():
    """
    Parses arguments passed to this script, and calls the helper function with the
    provided command line arg.
    """
    parser = argparse.ArgumentParser(
        description="Create a new Data Integration Element (DIE)."
    )
    parser.add_argument(
        "job_name",
        help="The name of the new DIE (without the 'DIE_' prefix).\nMust exactly match what the corresponding entry in the config.ini file.",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    template_dir = script_dir / "(DIE_template)"
    destination_dir = script_dir

    if not template_dir.is_dir():
        print(f"Error: Template directory not found: {template_dir}")
        exit(1)

    create_new_die(args.job_name, template_dir, destination_dir)


if __name__ == "__main__":
    main()
