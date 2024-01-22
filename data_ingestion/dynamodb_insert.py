import boto3

def insert_data_into_dynamodb():
    # Code to insert data into DynamoDB
    
    dynamodb = boto3.resource('dynamodb')
    table_name = 'MyDynamoDBTable'

    table = dynamodb.Table(table_name)

    data = {
        'UniqueID': '123456',
        'Attribute1': 'Value1',
        'Attribute2': 'Value2',
        # Add more attributes as needed
    }

    table.put_item(Item=data)

if __name__ == "__main__":
    insert_data_into_dynamodb()
