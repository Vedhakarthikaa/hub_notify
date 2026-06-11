"""
Email channel — sends via SMTP (dev) or AWS SES (prod).

Students: the send_email() function is partially implemented.
TODO: add HTML template support and handle SES in production.
"""
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.config import settings


async def send_email(
    to: str,
    subject: str,
    body: str,
    html_body: str | None = None,
) -> str:
    """
    Send an email via SMTP.

    Returns the SMTP message ID on success.
    Raises an exception on failure (caller handles retry logic).
    """
    msg = MIMEMultipart("alternative")
    msg["From"] = settings.smtp_from_email
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    if html_body:
        msg.attach(MIMEText(html_body, "html"))

    await aiosmtplib.send(
        msg,
        hostname=settings.smtp_host,
        port=settings.smtp_port,
        username=settings.smtp_username,
        password=settings.smtp_password,
        start_tls=True,
    )
    return msg["Message-Id"] or "sent"
