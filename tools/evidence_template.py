import os
import shutil
import subprocess

def setup_evidence_app():
    # Use current working directory as project root
    project_root = os.getcwd()

    # Define the app folder path
    app_dir = os.path.join(project_root, "app")
    
    # If an "app" folder exists, check for existing files in "Pages"
    if os.path.exists(app_dir):
        pages_dir = os.path.join(app_dir, "Pages")
        if os.path.isdir(pages_dir) and any(os.scandir(pages_dir)):
            print("Existing files found in 'app/Pages'. Aborting to avoid overwriting existing pages.")
            return
        else:
            # No pages found, so delete the existing "app" folder
            shutil.rmtree(app_dir)
            print("Existing 'app' folder deleted.")

    # Clone the template into the "app" folder
    try:
        subprocess.run(["npx", "degit", "evidence-dev/template", "app"], check=True)
        print("Template cloned successfully.")
    except subprocess.CalledProcessError as e:
        print("Error cloning template:", e)
        return
    except FileNotFoundError as e:
        print("Command not found:", e)
        return
    
    # Change into the cloned folder "app"
    os.chdir(app_dir)
    print(f"Changed directory to {app_dir}")
    
    # Delete the folder "sources/needful_things" if it exists
    needful_things_dir = os.path.join(app_dir, "sources", "needful_things")
    if os.path.isdir(needful_things_dir):
        shutil.rmtree(needful_things_dir)
        print(f"Deleted directory: {needful_things_dir}")
    else:
        print(f"Directory {needful_things_dir} not found; skipping deletion.")
    
    # Create a new folder "sources/app"
    new_app_folder = os.path.join(app_dir, "sources", "app")
    os.makedirs(new_app_folder, exist_ok=True)
    print(f"Created new folder: {new_app_folder}")
    
    # Create the file connection.yaml with the specified content
    connection_file = os.path.join(new_app_folder, "connection.yaml")
    connection_content = """name: data
type: duckdb
options:
  filename: data.duckdb
"""
    with open(connection_file, "w") as f:
        f.write(connection_content)
    print(f"Created file: {connection_file}")
    
    # Run 'bun install' inside the "app" folder
    try:
        subprocess.run(["bun", "install"], check=True)
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print("Error installing dependencies:", e)
    except FileNotFoundError as e:
        print("Command not found:", e)

if __name__ == "__main__":
    setup_evidence_app()
