import os
from ftplib import FTP
import cv2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from pyzbar.pyzbar import decode  # Using pyzbar for decoding QR codes
from dotenv import load_dotenv

load_dotenv()


# Email credentials
sender_email = os.environ.get("SENDER_EMAIL")
sender_password = os.environ.get("SENDER_PASSWORD")
error_email = 'error_read_document@sureflow.ae'


# Connect to FTP server
def connect_ftp():
    ftp = FTP(os.environ.get("FTP_ADDRESS"))
    ftp.login(user=os.environ.get("FTP_USERNAME"), passwd=os.environ.get("FTP_PASSWORD"))
    return ftp


# Connect to PostgreSQL database
def connect_database():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        port=os.environ.get("DB_PORT")
    )
    return conn


# Send email in case of error
def send_error_email(error_message):
    subject = 'Error Reading Document'
    body = f'There was an error reading a document:\n\n{error_message}'

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = error_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, error_email, message.as_string())


# Process QR code and extract information
def process_qr_code(image_path):
    image = cv2.imread(image_path)
    qr_codes = decode(image)

    if qr_codes:
        qr_code_data = qr_codes[0].data.decode('utf-8')
        # Parse qr_code_data and extract file type, person identifier, start validity date, and end validity date
        # For example: file_type, person_identifier, start_date, end_date = parse_qr_code_data(qr_code_data)
        file_type, person_identifier, start_date, end_date = qr_code_data.split(':')
        return True, (file_type, person_identifier, start_date, end_date)
    else:
        return False, None


# Main script
def main():
    ftp = connect_ftp()

    # Retrieve list of files on FTP server
    file_list = ftp.nlst()

    conn = connect_database()
    cursor = conn.cursor()

    for file_name in file_list:
        # Check if file has been processed before
        cursor.execute("SELECT * FROM processed_files WHERE file_name = %s", (file_name,))
        if cursor.fetchone():
            continue  # File has been processed, move on to the next file

        # Download file
        local_file_path = f'/path/to/local/folder/{file_name}'
        with open(local_file_path, 'wb') as local_file:
            ftp.retrbinary('RETR ' + file_name, local_file.write)

        # Process QR code
        success, qr_data = process_qr_code(local_file_path)

        if success:
            # Insert into PostgreSQL database
            cursor.execute("INSERT INTO documents (file_name, file_type, person_identifier, start_date, end_date) "
                           "VALUES (%s, %s, %s, %s, %s)",
                           (file_name, qr_data[0], qr_data[1], qr_data[2], qr_data[3]))
            conn.commit()

            # Mark file as processed
            cursor.execute("INSERT INTO processed_files (file_name) VALUES (%s)", (file_name,))
            conn.commit()
        else:
            # Send error email
            send_error_email(f'Error reading QR code from file: {file_name}')

        # Remove the local file
        os.remove(local_file_path)

    # Close connections
    ftp.quit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
