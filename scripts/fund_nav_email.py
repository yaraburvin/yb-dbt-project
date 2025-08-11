import os
import pandas as pd
import snowflake.connector
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            account='ah02912.eu-west-2.aws',
            user=os.getenv('DBT_SECRET__USER'),
            password=os.getenv('PASSWORD'),
            database='ANALYTICS_DB',
            warehouse='SCHEDULER_WH',
            schema='REPORTS'
        )
        print("Connected to Snowflake!")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def get_data(connection, table_name, start_date=None, end_date=None):
    ## Fetch data for specific period or return all the data available in the table
    if not start_date or not end_date:
        query = f"""SELECT * FROM {table_name}"""
    else:
        query = f"""
            SELECT * FROM {table_name}
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """

    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def save_to_excel(df, ):
    """Save DataFrame to Excel file and return the filename."""
    filename = f"nav_report_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
    try:
        df.to_excel(filename, index=False, sheet_name='NAV Report', engine='xlsxwriter')
        print(f"Excel file saved as {filename}")
        return filename
    except Exception as e:
        print(f"Error saving Excel file: {e}")
        return None
    

def send_email(data, to_email, from_email, password):
    """Send the NAV report via email with Excel attachment."""
    try:
    
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = 'NAV Report'
        
        # Excel attachment
        if not data.empty:
            excel_filename = save_to_excel(data)
            with open(excel_filename, 'rb') as attachment_file:
                attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                attachment.set_payload(attachment_file.read())
                encoders.encode_base64(attachment)
                attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename={excel_filename}'
                )
                msg.attach(attachment)
                
                # Clean up - delete the temporary file
            os.remove(excel_filename)
            print(f"file {excel_filename} deleted")
        
        # Send email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, password)
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()
        
        print(f"Email sent successfully to {to_email}")
        return True
        
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

def main():
    
    # .env file to reference env virables in production
    to_email = 'yaraburvin@gmail.com'   
    from_email = 'yaras.testemail@gmail.com'   
    email_password = os.getenv('GMAIL_PASSWORD')
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    if conn is None:
        print("Failed to connect to Snowflake.")
        return

    data = get_data(conn, table_name='PLAYGROUND_DB.PLAYGROUND_YARA_BURVIN.FUND_NAV')
    conn.close()

    # double checking i am not sending this to someone else by mistake
    send_email_choice = input(f"\nSend email report to {to_email}? (y/n): ").lower().strip()

    if send_email_choice == 'y':
        success = send_email(data, to_email, from_email, email_password)
        if success:
            print("Report sent successfully!")
        else:
            print("Failed to send report!")

if __name__ == "__main__":
    main()
