import boto3
import argparse

session = boto3.session.Session()

def arg_parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('--aws_access_key_id', type=str,
                        default='AKIAJ5V6NEAI3YNTWGDA')
    parser.add_argument('--aws_secret_access_key', type=str,
                        default='xdyXL4jP1SYhiKO9OGhOLYijVbG0BwPnq7J6oRDZ')
    parser.add_argument('--region_name', type=str,
                        default="eu-north-1")
    parser.add_argument('--db_name', type=str,
                        default="gundb")
    parser.add_argument('--node_type', type=str,
                        default="dc2.large")
    parser.add_argument('--cluster_id', type=str,
                        default="gunCluster")
    parser.add_argument('--master_username', type=str,
                        default="awsuser")
    parser.add_argument('--master_user_pass', type=str,
                        default="Awsuser12345")
    parser.add_argument('--n_nodes', type=int,
                        default=2)

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