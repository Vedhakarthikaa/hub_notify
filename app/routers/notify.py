"""
Notify router — /api/v1/notify/*

Handles single-send and bulk notification requests.
"""
import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.channels.email import send_email
from app.channels.sms import send_sms
from app.channels.push import send_push
from app.queue.producer import publish
from app.queue.schemas import NotifyPayload

router = APIRouter(prefix="/notify", tags=["notify"])


class SingleSendRequest(BaseModel):
    channel: str
    recipient: str
    subject: str | None = None
    body: str = ""
    html_body: str | None = None
    title: str | None = None
    data: dict | None = None


class BulkRecipient(BaseModel):
    recipient: str
    subject: str | None = None
    body: str = ""
    html_body: str | None = None


class BulkSendRequest(BaseModel):
    channel: str
    recipients: list[BulkRecipient]


@router.post("/send")
async def send_single(body: SingleSendRequest):
    """Send a single notification immediately (no queue)."""
    try:
        match body.channel:
            case "email":
                msg_id = await send_email(
                    to=body.recipient,
                    subject=body.subject or "",
                    body=body.body,
                    html_body=body.html_body,
                )
            case "sms":
                msg_id = send_sms(to=body.recipient, body=body.body)
            case "push":
                msg_id = send_push(
                    device_token=body.recipient,
                    title=body.title or "CixioHub",
                    body=body.body,
                    data=body.data,
                )
            case _:
                raise HTTPException(status_code=400, detail=f"Unknown channel: {body.channel}")
        return {"status": "sent", "message_id": msg_id}
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))


@router.post("/bulk", status_code=status.HTTP_202_ACCEPTED)
async def send_bulk(body: BulkSendRequest):
    """Enqueue a bulk notification job. Returns job_id immediately."""
    if not body.recipients:
        raise HTTPException(status_code=400, detail="Recipients list is empty")

    job_id = str(uuid.uuid4())
    # TODO: create NotificationJob record in DB here

    for r in body.recipients:
        payload = NotifyPayload(
            job_id=job_id,
            channel=body.channel,
            recipient=r.recipient,
            subject=r.subject,
            body=r.body,
            html_body=r.html_body,
        )
        await publish(payload)

    return {"job_id": job_id, "total": len(body.recipients), "status": "queued"}


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the current status of a bulk notification job.

    TODO: query the notification_jobs table and return progress.
    """
    # Placeholder response
    return {
        "job_id": job_id,
        "status": "not_implemented",
        "message": "Implement job tracking — query notification_jobs table",
    }
