import boto3
import argparse
import json

with open('./config.json') as json_data_file:
    app_config = json.load(json_data_file)


session = boto3.session.Session()

def arg_parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('--aws_access_key_id', type=str,
                        default=app_config["aws_access_key_id"])
    parser.add_argument('--aws_secret_access_key', type=str,
                        default=app_config["aws_secret_access_key"])
    parser.add_argument('--region_name', type=str,
                        default=app_config["region_name"])
    parser.add_argument('--db_name', type=str,
                        default=app_config["redshift_db_name"])
    parser.add_argument('--node_type', type=str,
                        default=app_config["redshift_node_type"])
    parser.add_argument('--cluster_id', type=str,
                        default=app_config["redshift_cluster_id"])
    parser.add_argument('--master_username', type=str,
                        default=app_config["redshift_user"])
    parser.add_argument('--master_user_pass', type=str,
                        default=app_config["redshift_password"])
    parser.add_argument('--n_nodes', type=int,
                        default=app_config["redshift_n_nodes"])

    return parser.parse_args()


if __name__ == "__main__":

    args = arg_parse()

    s3_client = session.client(
        service_name='redshift',
        region_name=args.region_name,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    response = s3_client.create_cluster(
        DBName=args.db_name,
        NodeType=args.node_type,
        ClusterIdentifier=args.cluster_id,
        Port=5439,
        MasterUsername=args.master_username,
        MasterUserPassword=args.master_user_pass,
        NumberOfNodes=args.n_nodes
    )

    print(response)