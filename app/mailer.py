from logging import config
import smtplib,os,json, logging, traceback
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime


from app.constants import load_mail_config
from app.logger import get_global_logger

class Mailer:
    def __init__(self, config = None):
        
        mail_config = config
        if not config:
            mail_config = load_mail_config()

        recipients = mail_config.get("recipients")
        dev_recipients = mail_config.get("dev_recipients")
        cc = mail_config.get("cc")
        bcc = mail_config.get("bcc")               
     
        
        self.SERVER = mail_config.get("server")
        self.PORT = mail_config.get("port")
        self.FROM = mail_config.get("sender")
        self.RECPTS = recipients if isinstance(recipients, list) else [recipients] if recipients else []
        self.DEVRECPTS = dev_recipients if isinstance(dev_recipients, list) else [dev_recipients] if dev_recipients else []
        self.CC = cc if isinstance(cc, list) else [cc] if cc else []
        self.BCC = bcc if isinstance(bcc, list) else [bcc] if bcc else []
        
        self.SEND_MAIL = mail_config.get("send_mail",False)
        
        self.LOGGER = get_global_logger()


    def start_mail(self, program, data=None, attachments=None,custom_html = None, dev=True):
        try:
            subject = f"[STARTED] {program} — Scraping Initiated"
            codes = ', '.join(map(str, data)) if data else "N/A"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

            body = f"""
            <html>
                <body>
                    <p>Hello Team,</p>
                    <p>This is to inform you that the program <b>{program}</b> has <b>started</b> execution.</p>
                    <p><b>Start Time:</b> {timestamp}</p>
                    <p><b>Scraping Codes:</b> {codes}</p>
                    {custom_html if custom_html else ""}
                    <p>Regards,<br>Automation System</p>
                </body>
            </html>
            """
            msg = self.construct_mail(subject=subject, body_html=body, attachments=attachments, dev=dev)
            self.send_mail(msg, dev=dev)

        except Exception as e:
            self.LOGGER.error("Start Mail not Sent.")
            self.LOGGER.error(f"{type(e).__name__}: {e}")
            self.LOGGER.error(traceback.format_exc())
            raise
   
    def end_mail(self, program, data=None, attachments=None,custom_html = None, dev=False):
        try:
            subject = f"[COMPLETED] {program} — Execution Finished"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

            body = f"""
            <html>
                <body>
                    <p>Hello Team,</p>
                    <p>The program <b>{program}</b> has <b>successfully completed</b> execution.</p>
                    <p><b>Completion Time:</b> {timestamp}</p>
                    <p>The output files are attached in a zip format.</p>
                    {custom_html if custom_html else ""}
                    <p><i>Note: This is an automated email. Please do not reply.</i></p>
                    <p>Regards,<br>Automation System</p>
                </body>
            </html>
            """
            msg = self.construct_mail(subject=subject, body_html=body, attachments=attachments, dev=dev)
            self.send_mail(msg, dev=dev)

        except Exception as e:
            self.LOGGER.error("End Mail not Sent.")
            self.LOGGER.error(f"{type(e).__name__}: {e}")
            self.LOGGER.error(traceback.format_exc())
            raise

    def fatal_error_mail(self, program,custom_msg = None, error_message=None, exception_obj=None, attachments=None, dev = True):
        try:
            subject = f"{program} Fatal Error Occurred"
            error_details = f"<pre>{traceback.format_exc()}</pre>" if exception_obj else ""
            body = f"""
            <html>
                <body>
                    <p>Hello Team,</p>
                    <p>The program <b>{program}</b> encountered a <b>fatal error</b>.</p>
                    <p>Error Comment: <b>{custom_msg}</b></p>
                    <p>Error Message: <b>{error_message}</b></p>
                    {error_details}
                    <p>Please check logs and investigate.</p>
                    <p>Regards,<br>Automation System</p>
                </body>
            </html>
            """
            msg = self.construct_mail(subject=subject, body_html=body, attachments=attachments, dev=dev)
            self.send_mail(msg, dev=dev)
        except Exception as e:
            self.LOGGER.error("Fatal Error Mail not sent.")
            self.LOGGER.error(f"{type(e).__name__}: {e}")
            self.LOGGER.error(traceback.format_exc())
    
    def default_body(self):
        return """
        <html>
            <body>
                <p>Hello Team,</p>
                <p>This is Default Mail Message.</p>
                <p>Regards,<br>System</p>
            </body>
        </html>
        """
    
    def send_custom(self, subject, body_html=None, body_text=None):
        msg = self.construct_mail(subject=subject, body_html=body_html, body_text=body_text)
        self.send_mail(msg)
    
    def construct_mail(self, subject, body_html=None, body_text=None, attachments=None, dev=True):
        msg = MIMEMultipart("alternative")

        recpts = self.DEVRECPTS if dev else self.RECPTS
        msg["From"] = self.FROM
        msg["To"] = ", ".join(recpts)
        msg["Subject"] = f"{subject} - {datetime.now().strftime('%Y-%m-%d')}"

        if not dev and self.CC:
            msg["Cc"] = ", ".join(self.CC)

        if body_text:
            msg.attach(MIMEText(body_text, "plain"))
        if body_html:
            msg.attach(MIMEText(body_html, "html"))
        else:
            msg.attach(MIMEText(self.default_body(), "html"))

        # Attach files if provided
        if attachments:
            attachments = [attachments] if isinstance(attachments, str) else attachments
            for file_path in attachments:
                if not file_path:
                    continue
                path = Path(file_path)
                if path.exists():
                    with open(path, "rb") as f:
                        part = MIMEApplication(f.read(), Name=path.name)
                        part['Content-Disposition'] = f'attachment; filename="{path.name}"'
                        msg.attach(part)
                else:
                    self.LOGGER.warning(f"Attachment not found: {file_path}")

        return msg

    def send_mail(self, msg, dev = True):
        try:
            recpts = self.DEVRECPTS if dev else self.RECPTS
            all_recipients = recpts + self.CC + self.BCC
            with smtplib.SMTP(self.SERVER, self.PORT) as server:
                server.send_message(msg, from_addr=self.FROM, to_addrs=all_recipients)
            self.LOGGER.info("Email sent successfully.")
        except Exception as e:
            self.LOGGER.error(f"Failed to send email: {e}")
