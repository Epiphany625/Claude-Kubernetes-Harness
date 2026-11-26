import json
import smtplib
import ssl
from email.message import EmailMessage
from html import escape
from pathlib import Path
from string import Template
from urllib.parse import urlencode

from config.config import EmailNotifierConfig
from notifier.notifier import Action, Notifier

# inherited from Notifier base class.
class EmailNotifier(Notifier):
    def __init__(self, emailConfig: EmailNotifierConfig):
        self.user = emailConfig.user
        self.password = emailConfig.password
        self.to_address = emailConfig.to
        self.approval_url = emailConfig.approvalURL
        self.smtp_host = emailConfig.smtpHost
        self.smtp_port = emailConfig.smtpPort
        self.smtp_timeout = emailConfig.smtpTimeout

        template_dir = Path(__file__).with_name("templates")
        self.html_template = Template((template_dir / "approval_email.html").read_text(encoding="utf-8"))
        self.html_action_template = Template((template_dir / "approval_email_action.html").read_text(encoding="utf-8"))
        self.text_template = Template((template_dir / "approval_email.txt").read_text(encoding="utf-8"))

    # emails a link to alertprocessor's approval page, which shows the actions and records the decision.
    def notify(self, subject: str, actions: list[Action], event_id: str, session_id: str):
        link = self.approval_link(subject, actions, event_id, session_id)

        message = EmailMessage()
        message["Subject"] = f"Approval needed: {' '.join(subject.split())}"
        message["From"] = self.user
        message["To"] = self.to_address
        message.set_content(self.text_template.substitute(
            summary=subject,
            actions="\n".join(f"- [{action.risk}] {action.tool}: {action.intent}" for action in actions),
            link=link,
            event_id=event_id,
            session_id=session_id,
        ))
        message.add_alternative(self.html_template.substitute(
            summary=escape(subject),
            actions="".join(self.html_action(action) for action in actions),
            link=escape(link),
            event_id=escape(event_id),
            session_id=escape(session_id),
        ), subtype="html")

        self.send(message)

    # the page renders straight from these parameters, so the link carries every action.
    def approval_link(self, subject: str, actions: list[Action], event_id: str, session_id: str) -> str:
        query = urlencode({
            "event_id": event_id,
            "session_id": session_id,
            "summary": subject,
            "actions": json.dumps([action.model_dump() for action in actions], separators=(",", ":")),
        })
        return f"{self.approval_url}?{query}"

    def html_action(self, action: Action) -> str:
        # (background, foreground) of each risk badge.
        risk_colors = {
            "low": ("#e4f4ea", "#1f6b3d"),
            "medium": ("#fff2d6", "#8a5a00"),
            "high": ("#fde4e2", "#a3302a"),
        }
        background, foreground = risk_colors[action.risk]
        return self.html_action_template.substitute(
            tool=escape(action.tool),
            intent=escape(action.intent),
            risk=escape(action.risk),
            risk_background=background,
            risk_foreground=foreground,
        )

    # port 465 speaks TLS from the start; other ports upgrade with STARTTLS when the server offers it.
    def send(self, message: EmailMessage):
        context = ssl.create_default_context()
        if self.smtp_port == 465:
            smtp = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context, timeout=self.smtp_timeout)
        else:
            smtp = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_timeout)
        with smtp:
            if self.smtp_port != 465:
                smtp.ehlo()
                if smtp.has_extn("starttls"):
                    smtp.starttls(context=context)
                    smtp.ehlo()
            if self.password:
                smtp.login(self.user, self.password)
            smtp.send_message(message)
