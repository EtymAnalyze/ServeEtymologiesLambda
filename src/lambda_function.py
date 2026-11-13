import boto3
import time

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

def batch_get_items(table_name, keys):
  """
    Retrieves multiple items and automatically retries UnprocessedKeys.
    :param table_name: Name of the DynamoDB table
    :param keys: List of primary key dicts, e.g. [{'id': '1'}, {'id': '2'}]
    """
  retrieved_items = []
  # Format the initial request
  request_items = {table_name: {'Keys': keys}}

  retries = 0
  while request_items:
    response = dynamodb.batch_get_item(RequestItems=request_items)

    # Collect successfully retrieved items
    items = response.get('Responses', {}).get(table_name, [])
    retrieved_items.extend(items)

    # Check if any keys were left unprocessed due to throttling or size limits
    request_items = response.get('UnprocessedKeys', {})

    if request_items:
      retries += 1
      # Exponential backoff before retrying unprocessed keys
      time.sleep(0.1 * (2**retries))

  return retrieved_items

def lambda_handler(event, context):
    # Get words from the body of the API Gateway event
    body = event.get('body', '')
    if not body or not body.get('words'):
        return {
            'statusCode': 400,
            'body': 'No words provided in the request body.'
        }
    words = body['words']
    # if words is not a list, return an error
    if not isinstance(words, list):
        return {
            'statusCode': 400,
            'body': 'Words must be provided as a list.'
        }

    # Query all the words against the english_words DynamoDB table
    english_words = batch_get_items('english_words', [{'word': word} for word in words])

    print(f"Retrieved {len(english_words)} words from DynamoDB.")
    print(f"Words: {english_words}")

    return {
        'statusCode': 200,
        'body': {
            'message': "chill it's a work in prog"
        }
    }