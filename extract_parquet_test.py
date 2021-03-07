from pyspark import SparkConf
from pyspark.sql import SparkSession, Column, SQLContext
from pyspark.sql.utils import CapturedException
from pyspark.sql.functions import lit, col, to_timestamp, coalesce
from pyspark.sql.types import *
from datetime import datetime
import traceback
import sys
import os
import utils
import json
import glob
import shutil
from pathlib import Path

sparkSession = None
sc = None
sql_context = None

OUTPUT_FOLDER = r'/opt/projects/datahub/python/edds'
OUTPUT_PREFIX = 'MSA_DATAHUB'
SCRATCH = r'/opt/projects/datahub/python/edds/scratch'


class GeneralException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class FileList:

    def __init__(self, output_tag):
        self.output_tag = output_tag
        self.date_tag = datetime.now().strftime('%Y%m%d')
        self.output_container = f'{output_tag}_{self.date_tag}'
        self.scratch_output_dir = f'{SCRATCH}{os.path.sep}{self.output_container}.parquet'
        self.output_dir = f'{OUTPUT_FOLDER}{os.path.sep}{OUTPUT_PREFIX}_{self.output_tag}'
        self.output_file_name = f'{self.output_container}.parquet'
        self.manifest_file_name = f'{self.output_container}.manifest.json'
        self.parquet_file_name = f'{self.output_container}.parquet'
        self.scratch_parquet_file_name = ''

    def create_output_dir(self):
        try:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise GeneralException(f'Could not create {self.output_dir}')

    def write_manifest_file(self, data):
        try:
            with open(f'{self.output_dir}{os.path.sep}{self.manifest_file_name}', 'w') as writer:
                writer.write(data)
        except OSError as e:
            raise GeneralException(f'OS Error while writing file {self.manifest_file_name} to {self.output_dir}:\n{e}')

    def rename_parquet_file(self):
        src = f'{self.scratch_output_dir}{os.path.sep}*.parquet'
        files = glob.glob(src)
        if len(files) == 1:
            self.scratch_parquet_file_name = f'{self.scratch_output_dir}{os.path.sep}{self.output_file_name}'
            try:
                os.rename(files[0], self.scratch_parquet_file_name)
            except OSError as e:
                raise GeneralException(f'OS Error on rename: {e}')
        else:
            raise GeneralException(f'Error renaming parquet file - please check scratch directory {SCRATCH}')

    def move_parquet_file(self):
        try:
            shutil.move(self.scratch_parquet_file_name, f'{self.output_dir}{os.path.sep}{self.parquet_file_name}')
        except OSError as e:
            raise GeneralException(f'OS Error on move: {e}')


def check_args():
    if len(sys.argv) == 3:
        return sys.argv[1], sys.argv[2]
    else:
        print('Usage: extract_parquet.py sql_file_name output_tag')
        sys.exit(1)


def init_spark():
    global sparkSession, sc, sql_context
    app_name = "jdbc_test.py"
    master = "local[*]"
    conf = SparkConf().setAppName(app_name).setMaster(master).set('spark.executor.memory', '500m')
    # Get path for Oracle jdbc driver
    jdbc_jar = get_key_value('/jdbc/Oracle/jar')
    if jdbc_jar:
        conf.set('spark.jars', jdbc_jar)  # set the spark.jars
        print('Using jdbc driver {}'.format(jdbc_jar))
    else:
        raise GeneralException('Could not get path to JDBC driver from config')
    sparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = sparkSession.sparkContext
    sql_context = SQLContext(sc)


last_seq = 0


def create_manifest_file(files, row_count):

    json_dict = {'table': files.output_tag, 'date': files.date_tag, 'rows': row_count}
    json_data = json.dumps(json_dict)
    files.write_manifest_file(json_data)


def rename_file(output_tag, date_tag):
    src = f'{SCRATCH}{os.path.sep}{output_tag}_{date_tag}.parquet{os.path.sep}*.parquet'
    files = glob.glob(src)
    if len(files) == 1:
        tgt = f'{SCRATCH}{os.path.sep}{output_tag}_{date_tag}.parquet{os.path.sep}{output_tag}_{date_tag}.parquet'
        os.rename(files[0], tgt)
        return 0
    else:
        print(f'Error renaming parquet file - please check scratch directory {SCRATCH}')
        return 1


def extract_table(sql_file_name, output_tag):
    # read SQL
    try:
        with open(sql_file_name, 'r') as reader:
            sql = reader.read()
    except OSError as e:
        print(f'OS Error: {e}')
        return 1

    files = FileList(output_tag)
    print(f'Output folder: {files.scratch_output_dir}')
    sql = f'({sql}) qry'
    #sql = 'Select POLICYID FROM DATAHUB.POLICY_DIM WHERE ROWNUM<2'
    init_spark()
    url = get_key_value('/jdbc/Oracle/url')
    if not url:
        raise GeneralException('Could not get JDBC URL from config')

    print('Using JDBC URL {}'.format(url))
    # properties = {'user': 'datahub', 'password': 'DATAHUB', 'driver': 'oracle.jdbc.OracleDriver', 'isolationLevel': 'READ_COMMITTED'}
    properties = {'driver': 'oracle.jdbc.OracleDriver', 'isolationLevel': 'READ_COMMITTED'}

    try:
        tableRDD = sql_context.read.jdbc(url=url, table=sql, properties=properties)
        tableRDD.coalesce(1).write.parquet(files.scratch_output_dir)
        print('Extracted {} rows'.format(tableRDD.count()))
        create_manifest_file(files, tableRDD.count())
        files.rename_parquet_file()
        files.move_parquet_file()
        return 0

    except (GeneralException, CapturedException) as e:
        print(f'Exception {e.__class__.__name__}: {e}')
        return 1

    # except Exception as e:
    #     traceback.print_tb(e.__traceback__)
    #     print(e)
    #     return 1


def get_key_value(key):
    value = utils.getinipath(key, None)
    if not value:
        print('Could not find value for key {}'.format(key))
        return None
    return value


def main():

    sql_file_name, output_tag = check_args()
    return extract_table(sql_file_name, output_tag)


sys.exit(main())

