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
    english_words = batch_get_items('english-words', [{'word': word} for word in words])

    print(f"Retrieved {len(english_words)} words from DynamoDB.")
    print(f"Words: {english_words}")

    merriam_status_dict = {item['word']: item.get('MerriamStatus', '') for item in english_words}

    response_dict = {}

    already_persisted_words = []

    sqs = boto3.client("sqs", region_name="us-west-2")

    for word in words:
        merriam_status = merriam_status_dict.get(word, '')
        if not merriam_status:
           response_dict[word] = 'Not a Word'
        elif merriam_status == 'NotFetched':
            response_dict[word] = 'Retry Later'
            sqs.send_message(
                QueueUrl='https://sqs.us-west-2.amazonaws.com/994969863268/FetchEtymologyQueue',
                Message=word
            )
        else:
           already_persisted_words.append(word)

    merriam_etymologies = batch_get_items('merriam-webster-etymologies', [{'word': word} for word in already_persisted_words])

    print(f"Retrieved {len(merriam_etymologies)} etymologies from DynamoDB.")
    print(f"Etymologies: {merriam_etymologies}")

    merriam_data_dict = {item['word']: {'fullText': item.get('fullText', ''), 'languageChain': item.get('languageChain', [])} for item in merriam_etymologies}

    for word in already_persisted_words:
        merriam_data = merriam_data_dict.get(word, {})
        if merriam_data:
           response_dict[word] = merriam_data
        else:
            print(f"No etymology data found for word: {word}, despite it being marked as already persisted.")
            response_dict[word] = 'No etymology data found'

    return {
        'statusCode': 200,
        'body': response_dict
    }