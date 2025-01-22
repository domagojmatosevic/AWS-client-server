import boto3
import uuid

clientID = str(uuid.uuid4())

sqs_queue1 = boto3.client('sqs', region_name='us-east-1')
queue1_url = 'https://sqs.us-east-1.amazonaws.com/433923865808/queue1.fifo'

sqs_queue2 = boto3.client('sqs', region_name='us-east-1')
queue2_url = 'https://sqs.us-east-1.amazonaws.com/433923865808/queue2.fifo'

s3bucket = 'ticketsrepo'
s3_client = boto3.client('s3')

if __name__ == '__main__':
    flag = 'y'
    while flag.lower() == 'y':
        print("Welcome to TICKET BUYER!")
        print("If you want to buy tickets, please fill in the following information")
        name = input('Name of customer: ')
        event = input("Name of event: ").lower()
        
        while True:
            try:
                num_of_tickets = int(input("Number of tickets: "))
                break
            except ValueError:
                print('Please enter an integer value!')

        if num_of_tickets > 6 or num_of_tickets <= 0:
            print('Sorry, the maximum number of tickets is 6 and the minimum is 1.')
            print('Try again!')
            continue

        # Send a message to SQS queue1
        sqs_queue1.send_message(
            QueueUrl=queue1_url,
            MessageBody="This is a ticket request",
            MessageGroupId=clientID,
            MessageAttributes={
                'Title': {
                    'DataType': 'String',
                    'StringValue': 'Ticket request'
                },
                'Author': {
                    'DataType': 'String',
                    'StringValue': name
                },
                'ClientID': {
                    'DataType': 'String',
                    'StringValue': clientID
                },
                'NumberOfTickets': {
                    'DataType': 'Number',
                    'StringValue': str(num_of_tickets)
                },
                'Event': {
                    'DataType': 'String',
                    'StringValue': event
                }
            }
        )

        print('Sent a messsage to queue1.')
        # Wait for response from SQS queue2
        print("Waiting for ticket processing...")
        while True:
            response = sqs_queue2.receive_message(
                QueueUrl=queue2_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5,
                MessageAttributeNames=['All']
            )

            messages = response.get('Messages', [])
            if messages:
                message = messages[0]
                message_attributes = message.get('MessageAttributes', {})

                # Compare ClientID to find the correct message
                if message_attributes.get('ClientID', {}).get('StringValue') == clientID:
                    receipt_id = message_attributes.get('ReceiptID', {}).get('StringValue')
                    receipt_handle = message['ReceiptHandle']
                    break

        if receipt_id is None:
            print('Sorry, no more tickets available.')
        else:
            print('Order confirmation number: ' + receipt_id)

            # Delete the processed message from the queue
            sqs_queue2.delete_message(
                QueueUrl=queue2_url,
                ReceiptHandle=receipt_handle
            )

        flag = input("Want to buy another ticket? (y/n): ")

    # Download the receipt PDF from S3
    receipt_id = input('Please enter your order confirmation number: ')
    try:
        s3_client.download_file(s3bucket, receipt_id + '.pdf', receipt_id + '.pdf')
        print(f"File downloaded successfully: {receipt_id}.pdf")
    except Exception as e:
        print(f"Error downloading file: {e}")
