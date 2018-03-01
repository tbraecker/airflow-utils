"""Create dag from a JSON file"""

import json
import zipfile
import glob
import os

__author__ = 'Tobias Braecker'
__version__ = '0.1'


class JsonToDag:
    def __init__(self):
        self.OPS_JSON_PATH_IN = os.environ['OPS_JSON_PATH_IN']
        self.OPS_JSON_PATH_IN_ZIP = os.environ['OPS_JSON_PATH_IN_ZIP']
        self.OPS_JSON_PATH_OUT= os.environ['OPS_JSON_PATH_OUT']
        self.OPS_PROCESSED_FILE = os.environ['OPS_JSON_PROCESSED_FILE']
        self.OPS_SCRIPT_DIR = os.environ['OPS_SCRIPT_DIR']

    def unzip(self, zip_file):
        f_zip = zipfile.ZipFile(zip_file, 'r')
        f_zip.extractall(self.OPS_JSON_PATH_IN)
        f_zip.close()

    def get_processed(self, filename):
        try:
            with open(self.OPS_PROCESSED_FILE, 'r') as f_proc:
                if [c for c in f_proc.read().splitlines() if c == filename]:
                    if os.path.isfile(self.OPS_JSON_PATH_IN + "/force.flag"):
                        print("Force Flag found. Start dag creation")
                        return 5
                    return 1
                else:
                    return 0
        except FileNotFoundError:
            print('Processed file not found. Will be created!')
            return 0

    def move_scripts(self):
        for f in os.listdir(self.OPS_JSON_PATH_IN):
            if f.endswith('.py') or f.endswith('.sh'):
                os.rename(self.OPS_JSON_PATH_IN + '/' +
                          f, self.OPS_SCRIPT_DIR + '/' + os.path.basename(f))

    def cleanup_processed_files(self):
        for f in os.listdir(self.OPS_JSON_PATH_IN):
            try:
                file_path = os.path.join(self.OPS_JSON_PATH_IN, f)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

    def create_dag(self, filename, processed):
        with open(self.OPS_JSON_PATH_IN + '/' + filename + '.json','r') \
                as f_json:
            d = json.load(f_json)
            with open(self.OPS_JSON_PATH_OUT + "/" + filename + '_dag.py',
                      'w') as f_dag:
                f_dag.write("# DAG created automatically\n")

                # Airflow imports
                f_dag.write('from airflow import DAG\n')
                f_dag.write('from airflow.operators.python_operator import PythonOperator\n')
                f_dag.write('from airflow.operators.bash_operator import BashOperator\n')

                # Imports
                for item in d['imports']:
                    f_dag.write('import ' + item +'\n')
                f_dag.write('\n')

                # From Imports
                if len(d['import_from']) != 0:
                    for item in d['import_from']:
                        f_dag.write('from ' + item['file'] + ' import ' +
                                    item['class'] + "\n")
                    f_dag.write('\n')

                # Default args
                f_dag.write("default_args = {\n")
                for item in d['scheduler_arguments']:
                    if item == 'owner':
                        f_dag.write("    '" + item + "': '" +
                              str(d['scheduler_arguments'][item]) + "',")
                    elif item == 'retry_delay':
                        f_dag.write("    '" + item + "': " +
                                    str("timedelta(minutes=" +
                                    str(d['scheduler_arguments'][item])) +
                                    "),")
                    else:
                        f_dag.write("    '" + item + "': " +
                                    str(d['scheduler_arguments'][item]) + ",")
                    f_dag.write('\n')

                f_dag.write('}\n\n')

                # DAG initialisation
                f_dag.write("dag = DAG('" + d['dag']['name'] +
                            "',default_args=default_args, schedule_interval='" +
                            d['dag']['schedule'] + "')\n")

                f_dag.write("dag.catchup = " + d['dag']['catchup'] + "\n\n")

                # Definitions
                for item in d['definitions']:
                    f_dag.write("def " + item['name'] + "(ds, **kwargs):\n")
                    if 'command' in item:
                        f_dag.write("    " + item['command'] + "\n\n")
                    if 'class' in item:
                        f_dag.write("    " + item['class'][:1] + " = " +
                                    item['class'] + "()\n")
                        for method in item['methods']:
                            f_dag.write("    " + item['class'][:1] + "." +
                                        method + "()\n")
                        f_dag.write("\n")

                # Tasks
                links = []
                for task in d['tasks']:
                    f_dag.write(task['name'] + " = \\\n")
                    if task['operator'] == 'python':
                        f_dag.write("    PythonOperator(task_id='" + task['id']
                                    + "',provide_context=True, python_callable="
                                    + task['definition'] + ", dag=dag)\n\n")
                    if task['operator'] == 'bash':
                        f_dag.write("    BashOperator(task_id='" + task['id']
                                    + "',bash_command='" + task['definition']
                                    + "',dag=dag)\n\n")
                    if 'link' in task:
                        for link in task['link']:
                            links.append(task['name'] + "." +
                                         link['type'] + "(" +
                                         link['name'] + ")")

                # Links
                for link in links:
                    f_dag.write(link + "\n")

                if processed == 0:
                    with open(self.OPS_PROCESSED_FILE,'a') as f_proc:
                        f_proc.write(filename + "\n")

                print("Finished DAG creation")

    def main(self):
        zip_files = glob.glob(self.OPS_JSON_PATH_IN_ZIP + '/*.zip')
        for zip_file in zip_files:
            filename = os.path.basename(zip_file.replace('.zip',''))
            self.unzip(zip_file)
            processed = self.get_processed(filename)

            if processed in (0,5):
                self.create_dag(filename,processed)
            else:
                print('DAG already created! For overwrite add force.flag to your zip-file!')

            self.move_scripts()
            self.cleanup_processed_files()


if __name__ == "__main__":
    cd = JsonToDag()
    cd.main()


