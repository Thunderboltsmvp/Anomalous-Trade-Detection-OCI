import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
import pandas as pd

def send_email():
    
    print("In send_email function")
    
    fromaddr = "thunderbolt677@outlook.com"
    toaddr = "gnana.venus.lazarus@oracle.com"   
    #toaddr = "thunderboltsmvp@gmail.com"   


    # Instance of MIMEMultipart
    msg = MIMEMultipart()

    # Storing the senders email address
    msg['From'] = fromaddr

    # Storing the receivers email address
    msg['To'] = toaddr

    # Storing the subject
    msg['Subject'] = "Wash Trades Detected"
    
    #html email template
    body = """\
                <html>
                <head>
                <style>
                body {
                        background-color: #f6f6f6;
                        font-family: sans-serif;
                        -webkit-font-smoothing: antialiased;
                        font-size: 14px;
                        line-height: 1.4;
                        margin: 0;
                        padding: 0;
                        -ms-text-size-adjust: 100%;
                        -webkit-text-size-adjust: 100%; }
                table {
                        border-collapse: separate;
                        mso-table-lspace: 0pt;
                        mso-table-rspace: 0pt;
                        width: 100%; }
                        table td {
                        font-family: sans-serif;
                        font-size: 14px;
                        vertical-align: top; }
                .body {
                        background-color: #f6f6f6;
                        width: 100%; }
                .content {
                        box-sizing: border-box;
                        display: block;
                        margin: 0 auto;
                        max-width: 580px;
                        padding: 10px; }
                .main {
                        background: #ffffff;
                        border-radius: 3px;
                        width: 100%; }

                .wrapper {
                        box-sizing: border-box;
                        padding: 20px; }

                .content-block {
                        padding-bottom: 10px;
                        padding-top: 10px;
                    }
                </style>
                </head>
                <body>
                <table class="main" >
                <div class="content">
                <tr>
                <td class="wrapper"><b>Wash Trades Detected!</b><td>
                </tr>
                <tr>
                <td class="wrapper">Wash Trades have been detected as per the IRS Wash Sale Rule (IRC Section 1091).<br>Please find attached the Wash Trades file.<td>
                </tr>
                <tr>
                <td class="wrapper">Regards, <br>Anomalous Trade Detection MVP<td>
                </tr>
                <div>
                </table>
                <body>
                </html>
                """

    # String to store the body of the mail
    #body = "Wash Trades have been detected as per the IRS Wash Sale Rule (IRC Section 1091). Please find attached the Wash Trades detected."
    
    # Attach the body with the msg instance
    msg.attach(MIMEText(body, 'html'))

    washtradesdf = pd.read_csv('Trade_data.csv')
    with open('Trade_data.csv','rb') as file:
    # Attach the file with filename to the email
        msg.attach(MIMEApplication(file.read(), Name='WashTrades.csv'))
        
    # File to be sent
    #part = MIMEApplication(washtradesdf, Name="WashTrades.csv")
    #part['Content-Disposition'] = 'attachment; filename="%s"' % 'WashTrades.csv'

    # Attach the instance 'p' to instance 'msg'
    #msg.attach(part)

    # Create SMTP session
    s = smtplib.SMTP('smtp-mail.outlook.com', 587)
    
    # Start TLS for security
    s.starttls()
    
    # Authentication
    s.login(fromaddr, "Macnam007000")
    
    # Convert the Multipart msg into a string
    text = msg.as_string()
    
    # Send the email
    s.sendmail(fromaddr, toaddr, text)
    print("Successfully sent mail with the attached file")
    
    # Terminate the session
    s.quit()
    print("Email sent")
    
send_email()