import boto3

def create_dynamodb_table():
    # Code to create DynamoDB table
    
    dynamodb = boto3.resource('dynamodb')
    table_name = 'MyDynamoDBTable'

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'UniqueID', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'UniqueID', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)


if __name__ == "__main__":
    create_dynamodb_table()
