import boto3
import uuid
import datetime
from fpdf import FPDF
import os

sqs_queue1 = boto3.client('sqs', region_name='us-east-1')
queue1_url = 'https://sqs.us-east-1.amazonaws.com/433923865808/queue1.fifo'

sqs_queue2 = boto3.client('sqs', region_name='us-east-1')
queue2_url = 'https://sqs.us-east-1.amazonaws.com/433923865808/queue2.fifo'

s3bucket = 'ticketsrepo'
s3_client = boto3.client('s3')
s3_tickets = 'remainingTickets.txt'

if __name__ == '__main__':
    while True:
        while True:
            print('Waiting for message from queue1...a')
            response = sqs_queue1.receive_message(
                QueueUrl=queue1_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,  
                WaitTimeSeconds=1,
                MessageAttributeNames=['All']
            )
            
            if 'Messages' in response and len(response['Messages']) > 0:
                break

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        
        # Extract message attributes
        attributes = message.get('MessageAttributes', {})
        buyer_name = attributes.get('Author', {}).get('StringValue', 'Unknown')
        event_name = attributes.get('Event', {}).get('StringValue', 'Unknown')
        num_tickets = int(attributes.get('NumberOfTickets', {}).get('StringValue', 0))

        # Checking if there is enough tickets
        res = s3_client.get_object(Bucket=s3bucket, Key=s3_tickets)
        remaining = int(response['Body'].read().decode('utf-8'))

        if remaining < num_tickets:
            print("No tickets available.")
        else:
            # Create PDF
            s3_client.put_object(Bucket=s3_tickets, Key=s3_tickets, Body=str(remaining-num_tickets))
            print("Remaining tickets updated!")
            fileName = str(uuid.uuid4())
            title = 'Receipt ' + fileName
            textLines = [
                f'Receipt ID: {fileName}',
                f'Name of the buyer: {buyer_name}',
                f'Time of purchase: {datetime.datetime.now()}',
                f'Name of the event: {event_name}',
                f'Number of tickets: {num_tickets}'
            ]

            for i in range(num_tickets):
                textLines.append(f'Ticket {i + 1} ID: {uuid.uuid4()}')

            pdf = FPDF()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.add_page()
            pdf.set_font("Arial", size=12)

            # Add content to PDF
            pdf.cell(0, 20, title, ln=True, align="C")
            for line in textLines:
                pdf.cell(0, 5, line, ln=True)

            pdf_file_name = fileName + '.pdf'
            pdf.output(pdf_file_name)

            # Upload PDF to S3
            s3_client.upload_file(pdf_file_name, s3bucket, pdf_file_name)
            print(f"PDF successfully uploaded to S3 bucket '{s3bucket}' as '{pdf_file_name}'")

            # Remove the file locally after upload
            os.remove(pdf_file_name)

        # Delete processed SQS message
        sqs_queue1.delete_message(
            QueueUrl=queue1_url,
            ReceiptHandle=receipt_handle
        )

        # Send message to the second queue (queue2)
        sqs_queue2.send_message(
            QueueUrl=queue2_url,
            MessageAttributes={
                'Title': {
                    'DataType': 'String',
                    'StringValue': 'Ticket request'
                },
                'Author': {
                    'DataType': 'String',
                    'StringValue': buyer_name
                },
                'ClientID': {
                    'DataType': 'String',
                    'StringValue': attributes.get('ClientID', {}).get('StringValue', 'Unknown')
                },
                'ReceiptID': {
                    'DataType': 'String',
                    'StringValue': fileName
                }
            },
            MessageBody='Ticket processed',
            MessageGroupId=event_name
        )

        print("Message sent to SQS queue2")

