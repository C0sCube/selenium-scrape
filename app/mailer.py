import smtplib,os,json, logging, traceback
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime


from app.constants import PATHS
from app.logger import get_global_logger

class Mailer:
    def __init__(self, server='172.17.0.126', port=25, 
                sender='Kaustubh.Keny@cogencis.com', 
                recipients=['Kaustubh.Keny@cogencis.com'], 
                cc=None, bcc=None, logger=None):
        
        try:
            
            paths = PATHS
            mail_config = paths.get("mail", {})
            server = mail_config.get("server", server)
            port = mail_config.get("port", port)
            sender = mail_config.get("sender", sender)
            recipients = mail_config.get("recipients", recipients)
            cc = mail_config.get("cc", cc)
            bcc = mail_config.get("bcc", bcc)               
        except FileNotFoundError:
            
            print("paths.json file not found. Using default values.")
        
        self.SERVER = server
        self.PORT = port
        self.FROM = sender or "noreply@example.com"
        self.RECPTS = recipients if isinstance(recipients, list) else [recipients] if recipients else []
        self.CC = cc if isinstance(cc, list) else [cc] if cc else []
        self.BCC = bcc if isinstance(bcc, list) else [bcc] if bcc else []
        
        self.logger = get_global_logger()


    def start_mail(self, program, data=None,attachments=None):
        subject = f"{program} — Execution Started {datetime.now().strftime("%d/%m/%Y, %H:%M")}"
        body = f"""
        <html>
            <body>
                <p>Hello Team,</p>
                <p>The program <b>{program}</b> has <b>started</b>.</p>
                <p>Scraping Websites of Code:{','.join(data)}</p>
                <p>Regards,<br>Kaustubh</p>
            </body>
        </html>
        """
        
        if attachments:
            attachments = [a for a in attachments if isinstance(a,str)]
        msg = self.construct_mail(subject=subject, body_html=body, attachments = attachments)
        self.send_mail(msg)
   
    def end_mail(self, program:str, data=None, attachments=None, custom_html=None):
        subject = f"{program} — Execution Completed"
        
        if isinstance(data,list): data = " ".join(data)
        
        body = f"""
        <html>
            <body>
                <p>Hello Team,</p>
                <p>The program <b>{program}</b> has <b>completed</b> execution.</p>
                <p>{data}</p>
                <p>Regards,<br>Kaustubh</p>
            </body>
        </html>
        """
        if attachments:
            attachments = [a for a in attachments if isinstance(a,str)]
        
        msg = self.construct_mail(subject=subject, body_html=body, attachments=attachments, custom_html=custom_html)
        self.send_mail(msg)


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
    
    def combine_html_bodies(self, default_html: str, custom_html: str = None) -> str:
        """
        Combines the default email body with an additional custom HTML section.
        The appended section is placed below a horizontal rule.
        """
        if not custom_html:
            return default_html

        combined = f"""
        <html>
            <body style='font-family: Arial, sans-serif; font-size: 11pt; color: #222;'>
                {default_html}
                <hr style='margin: 20px 0; border: none; border-top: 1px solid #ccc;'>
                {custom_html}
            </body>
        </html>
        """
        return combined

    
    def send_custom(self, subject, body_html=None, body_text=None):
        msg = self.construct_mail(subject=subject, body_html=body_html, body_text=body_text)
        self.send_mail(msg)
    
    
    def construct_mail(self, subject, body_html=None, body_text=None, attachments=None, custom_html=None):
        """ Construct an email with optional additional HTML (custom_html). """
        msg = MIMEMultipart("alternative")
        msg["From"] = self.FROM
        msg["To"] = ", ".join(self.RECPTS)
        if self.CC:
            msg["Cc"] = ", ".join(self.CC)
        msg["Subject"] = f"{subject} - {datetime.now().strftime('%Y-%m-%d')}"

        # --- Combine HTMLs ---
        final_html = self.combine_html_bodies(body_html or self.default_body(), custom_html)

        # --- Attach body parts ---
        if body_text:
            msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(final_html, "html"))

        # --- Attach files ---
        if attachments:
            for file_path in attachments:
                path = Path(file_path)
                if path.exists():
                    with open(path, "rb") as f:
                        part = MIMEApplication(f.read(), Name=path.name)
                        part['Content-Disposition'] = f'attachment; filename="{path.name}"'
                        msg.attach(part)
                else:
                    self.logger.warning(f"Attachment not found: {file_path}")

        return msg

    
    def fatal_error_mail(
        self, 
        program,
        custom_msg = None, 
        error_message=None, 
        exception_obj=None, 
        attachments=None
    ):
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
                    <p>Regards,<br>System</p>
                </body>
            </html>
            """
            msg = self.construct_mail(subject=subject, body_html=body, attachments=attachments)
            self.send_mail(msg)
        except Exception as e:
            self.logger.error("Fatal Error Mail not sent.")
            self.logger.error(f"{type(e).__name__}: {e}")
            self.logger.error(traceback.format_exc())
    
    
    
    # def construct_mail(self, subject, body_html=None, body_text=None, attachments = None):
    #     msg = MIMEMultipart("alternative")
    #     msg["From"] = self.FROM
    #     msg["To"] = ", ".join(self.RECPTS)
    #     if self.CC: msg["Cc"] = ", ".join(self.CC)
    #     msg["Subject"] = f"{subject} - {datetime.now().strftime('%Y-%m-%d')}"

    #     if body_text: msg.attach(MIMEText(body_text, "plain"))
    #     if body_html: msg.attach(MIMEText(body_html, "html"))
    #     else: msg.attach(MIMEText(self.default_body(), "html"))
        
        
    #     if attachments:
    #         for file_path in attachments:
    #             path = Path(file_path)
    #             if path.exists():
    #                 with open(path, "rb") as f:
    #                     part = MIMEApplication(f.read(), Name=path.name)
    #                     part['Content-Disposition'] = f'attachment; filename="{path.name}"'
    #                     msg.attach(part)
    #             else:
    #                 self.logger.warning(f"Attachment not found: {file_path}")

    #     return msg

    def send_mail(self, msg):
        try:
            all_recipients = self.RECPTS + self.CC + self.BCC
            with smtplib.SMTP(self.SERVER, self.PORT) as server:
                server.send_message(msg, from_addr=self.FROM, to_addrs=all_recipients)
            self.logger.info("Email sent successfully.")
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")

