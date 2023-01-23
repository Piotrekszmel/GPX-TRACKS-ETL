import argparse
from dotenv import load_dotenv

from processor import Processor


def cli():
    load_dotenv()
    parser = argparse.ArgumentParser(description='Process input arguments')
    parser.add_argument('operation')
    parser.add_argument('--data')
    parser.add_argument('--transform_data')
    parser.add_argument('--df_data')
    parser.add_argument('--create_table')
    parser.add_argument('--sql')
    args = vars(parser.parse_args())
    p = Processor(data_path=args['data'])
    
    if args["operation"] == "extract":
        print(p.extract())
    elif args["operation"] == "transform":
        p.transform(args['transform_data'])
    elif args["operation"] == "load":
        p.load(args['data'])
    elif args["operation"] == "pipeline":
        p.run_pipeline(create_table_query=args['create_table'])
    else:
        print("Wrong argument! Supported args: extract | transform | load | execute")


if __name__ == "__main__":
    cli()