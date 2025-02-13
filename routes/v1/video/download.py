import logging

from flask import Blueprint

from app_utils import *
from services.authentication import authenticate
from services.cloud_storage import upload_file
from services.v1.video.download import download_yt_video

v1_video_download_bp = Blueprint("v1_video_download", __name__)
logger = logging.getLogger(__name__)


@v1_video_download_bp.route("/v1/video/download", methods=["POST"])
@authenticate
@validate_payload(
    {
        "type": "object",
        "properties": {
            "video_url": {"type": "string", "format": "uri"},
            "id": {"type": "string"},
        },
        "required": ["video_url"],
        "additionalProperties": False,
    }
)
@queue_task_wrapper(bypass_queue=False)
def download(job_id, data):
    media_url = data["video_url"]
    id = data.get("id")

    logger.info(f"Job {job_id}: Received download request for '{media_url}'")

    try:
        output_file = download_yt_video(media_url, job_id)
        logger.info(f"Job {job_id}: Video download process completed successfully")

        cloud_url = upload_file(output_file)
        logger.info(
            f"Job {job_id}: Downloaded video uploaded to cloud storage: {cloud_url}"
        )

        return cloud_url, "/v1/video/download", 200

    except Exception as e:
        logger.error(f"Job {job_id}: Error during video download process - {str(e)}")
        return str(e), "/v1/video/download", 500
