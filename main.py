# standard library imports
import subprocess
import sys
import os

# ------------------------------------------------------------------------------
# end-to-end pipeline for downloading all media
# ------------------------------------------------------------------------------

def full_pipeline():
    """full pipeline for downloading contents"""
    env = os.environ.copy()
    env['PYTHONPATH'] = os.getcwd()

    subprocess.run([sys.executable, "src/core/_01_rss_ingest.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_02_collect.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_03_parse.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_04_file_filtration.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_05_metadata_collection.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_06_media_filtration.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_07_initiation.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_08_download_check.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_09_transfer.py"], cwd=os.getcwd(), env=env, check=True)
    subprocess.run([sys.executable, "src/core/_10_cleanup.py"], cwd=os.getcwd(), env=env, check=True)

# -------------------------------------------------------------------------------
# main guard
# -------------------------------------------------------------------------------

def main():
    """
    Main function for command-line interface
    """
    full_pipeline()


if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------
# end of main.py
# ------------------------------------------------------------------------------