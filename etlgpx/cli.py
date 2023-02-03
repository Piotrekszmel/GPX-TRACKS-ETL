import argparse
from dotenv import load_dotenv

from etlgpx.processor import Processor


def cli():
    load_dotenv()
    parser = argparse.ArgumentParser(description='Process input arguments')
    parser.add_argument('operation',
                        help='Type of a operation that will be performed.' + \
                             'Possible values: extract | transform | load | execute.')
    parser.add_argument('--data',
                        help='Path to the data in gpx format that will be extracted.')
    parser.add_argument('--df_data',
                        help='.')
    parser.add_argument('--transform_data',
                        help='Data that will be transformed.' + \
                             'Supported format: List[GPXTrack].')
    parser.add_argument('--create_table',
                        help='Path to a sql script that will be' + \
                             'used to create a table that will' + \
                             'store prepared data.')
    args = vars(parser.parse_args())

    p = Processor(data_path=args['data'])
    if args["operation"] == "extract":
        p.extract()
    elif args["operation"] == "transform":
        p.transform(args['transform_data'])
    elif args["operation"] == "pipeline":
        p.run_pipeline(create_table_query=args['create_table'])
    else:
        print("Wrong argument! Supported args: extract | transform | pipeline")


if __name__ == "__main__":
    cli()