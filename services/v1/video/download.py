import logging
import os

from yt_dlp import YoutubeDL

logger = logging.getLogger(__name__)

# Set the default local storage directory
STORAGE_PATH = "/tmp/"


def download_yt_video(yt_url: str, job_id: str) -> str:
    """Download a video from a URL."""
    ydl_opts = {
        "format": "best[ext=mp4]",
        "outtmpl": f"{STORAGE_PATH}%(title)s.%(ext)s",
        "restrictfilenames": True,
        "noplaylist": True,
        "nocheckcertificate": True,
        "ignoreerrors": False,
        "logtostderr": False,
        "no_color": False,
    }

    logger.info(f"Job {job_id}: Downloading video from URL: {yt_url}")

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(yt_url, download=False)
            output_filename = ydl.prepare_filename(info)

            ydl.download([yt_url])

            if not os.path.exists(output_filename):
                raise FileNotFoundError(
                    f"Output file '{output_filename}' does not exist after download."
                )

        return output_filename

    except Exception as e:
        logger.error(f"Job {job_id}: Error during video download - {str(e)}")
        raise
