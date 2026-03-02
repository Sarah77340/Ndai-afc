import os
from great_expectations.data_context import FileDataContext

def main():
    project_dir = os.path.abspath(os.path.dirname(__file__))  # .../quality
    ge_dir = os.path.join(project_dir, "great_expectations")
    yml_path = os.path.join(ge_dir, "great_expectations.yml")

    os.makedirs(ge_dir, exist_ok=True)

    if not os.path.exists(yml_path):
        # Force la création du context file-based (GE 1.13)
        FileDataContext._create(project_root_dir=project_dir)
        print("✅ Data Context créé.")
    else:
        print("✅ Data Context déjà présent.")

    print("GE dir:", ge_dir)
    print("YML exists:", os.path.exists(yml_path))

if __name__ == "__main__":
    main()