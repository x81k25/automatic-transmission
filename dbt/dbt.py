import os
import subprocess
import argparse
from pathlib import Path

def run_dbt_model(model_name, profiles_dir=None, target=None, project_dir=None):
    """
    Run a specific dbt model

    Args:
        model_name: Name of the model to run
        profiles_dir: Path to profiles.yml directory
        target: dbt target profile to use
        project_dir: Path to dbt project directory
    """
    cmd = ["dbt", "run", "--models", model_name]

    if profiles_dir:
        cmd.extend(["--profiles-dir", profiles_dir])

    if target:
        cmd.extend(["--target", target])

    if project_dir:
        os.chdir(project_dir)

    print(f"Executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print("dbt model execution successful!")
        print(result.stdout)
    else:
        print("dbt model execution failed!")
        print(result.stderr)

    return result.returncode

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a dbt model')
    parser.add_argument('model_name', help='Name of the model to run')
    parser.add_argument('--profiles-dir', help='Path to profiles.yml directory')
    parser.add_argument('--target', help='dbt target profile to use')
    parser.add_argument('--project-dir', help='Path to dbt project directory')

    args = parser.parse_args()

    exit_code = run_dbt_model(
        args.model_name,
        args.profiles_dir,
        args.target,
        args.project_dir
    )

    exit(exit_code)