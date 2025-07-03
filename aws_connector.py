import boto3
from config.config import Config

class AWSConnector:
       def __init__(self):
             self.session = boto3.Session(region_name = Config.AWS_REGION)

       def get_s3_client(self):
             return self.session.client('s3')

       def get_glue_client(self):
             return self.session.client('glue')

        def get_athena_client(self):
             return self.session.client('athena')

        def upload_to_s3(self, file_path, s3_key):
             s3 = self.get_s3_client()
             s3.upload_file(file_path, Config.S3_BUCKET, s3_key)

        def create_glue_table(self, table_name, schema, location):
               glue = self.get_glue_client()
               try:
                    glue.create_table(
                      DatabaseName = Config.GLUE_DATABASE,
                      TableInput = {
                      'Name' : table_name,
                      'StorageDescriptor': {
                        'Columns': schema,
                        'Location': location,
                        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE'
                }
            )
        except glue.exceptions.AlreadyExistsException:
            print(f"Table {table_name} already exists")
