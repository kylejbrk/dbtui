import os
import shutil

import yaml


class DBTCLI:
    def __init__(self, path=None):
        if not path:
            self.path = shutil.which("dbt")
        else:
            self.path = path


class DBTProject:
    def __init__(self, project_path=None):
        if project_path is None:
            if "dbt_project.yml" in os.listdir():
                self.project_path = os.getcwd()
            else:
                raise FileNotFoundError(
                    "No dbt_project.yml found in the current directory."
                )
        else:
            self.project_path = project_path

        self.project_yaml_file = os.path.join(self.project_path, "dbt_project.yml")
        with open(self.project_yaml_file, "r") as f:
            self.project_yaml = yaml.safe_load(f)


dbt_cli = DBTCLI()
dbt_project = DBTProject(project_path="./jaffle_shop_duckdb/")
print(dbt_project.project_yaml)
