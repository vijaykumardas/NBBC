import os
import subprocess
import shutil

def clean_untracked_files(folder_to_clean=None):
    if folder_to_clean is None:
        folder_to_clean = os.path.dirname(os.path.abspath(__file__))

    # Get list of tracked files from git
    result = subprocess.run(
        ['git', 'ls-files'],
        cwd=folder_to_clean,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        print("Error fetching tracked files from git:", result.stderr)
        return

    # Set of tracked files (relative paths)
    tracked_files = set(result.stdout.strip().split('\n'))

    # Convert tracked files to absolute paths
    tracked_abs_paths = {os.path.abspath(os.path.join(folder_to_clean, f)) for f in tracked_files}
    print(tracked_abs_paths)
    for root, dirs, files in os.walk(folder_to_clean, topdown=False):
        for name in files:
            print(root+ " " +name)
            full_path = os.path.abspath(os.path.join(root, name))
            if full_path not in tracked_abs_paths:
                #os.remove(full_path)
                print(f"Deleted file: {full_path}")

        for name in dirs:
            dir_path = os.path.abspath(os.path.join(root, name))
            # Skip .git folder and keep only empty untracked folders
            if name == '.git':
                print('Skipping .git')
                continue
            if name == '.github':
                print('skipping .github')
                continue
            if not os.listdir(dir_path):
                #shutil.rmtree(dir_path)
                print(f"Deleted empty folder: {dir_path}")

if __name__ == "__main__":
    clean_untracked_files()
