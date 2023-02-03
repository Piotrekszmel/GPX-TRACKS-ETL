from dotenv import load_dotenv
import os

from etlgpx.processor import Processor


def main():
    load_dotenv()
    data_path = os.getenv('DATA_PATH')
    create_table_query = os.getenv('CREATE_TABLE')
    p = Processor(data_path)
    p.run_pipeline(create_table_query=create_table_query)


if __name__ == '__main__':
    main()