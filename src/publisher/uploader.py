import os
from typing import List, Optional
import mimetypes
import pathlib
import boto3
from botocore.config import Config


def _s3_client():
    endpoint = os.getenv("S3_ENDPOINT_URL")
    region = os.getenv("S3_REGION", "auto")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    if not (access_key and secret_key and os.getenv("S3_BUCKET")):
        raise RuntimeError("Missing S3 credentials or bucket in env: S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET")
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "virtual"}),
    )


def upload_file(local_path: str, key: str, bucket: Optional[str] = None, public_base_url: Optional[str] = None) -> str:
    """Upload a single file to S3-compatible storage and return a public URL.
    If public_base_url provided, URL is f"{public_base_url.rstrip('/')}/{key}"; else use path-style URL.
    """
    s3 = _s3_client()
    bucket = bucket or os.getenv("S3_BUCKET")
    assert bucket
    ctype, _ = mimetypes.guess_type(local_path)
    extra_args = {"ACL": "public-read"}
    if ctype:
        extra_args["ContentType"] = ctype
    s3.upload_file(local_path, bucket, key, ExtraArgs=extra_args)
    if public_base_url:
        return f"{public_base_url.rstrip('/')}/{key}"
    # Fallback: construct from endpoint
    endpoint = os.getenv("S3_PUBLIC_BASE_URL") or os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        return f"{endpoint.rstrip('/')}/{bucket}/{key}"
    # Last resort: standard S3 URL (may not work for R2 without domain)
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def upload_directory(in_dir: str, prefix: str = "reels/") -> List[str]:
    """Upload all files in a directory (non-recursive) and return their public URLs in the same order.
    Key = prefix + filename
    """
    public_base = os.getenv("S3_PUBLIC_BASE_URL")
    bucket = os.getenv("S3_BUCKET")
    if not os.path.isdir(in_dir):
        raise FileNotFoundError(in_dir)
    urls: List[str] = []
    for p in sorted(pathlib.Path(in_dir).glob("*")):
        if not p.is_file():
            continue
        key = f"{prefix.rstrip('/')}/{p.name}"
        url = upload_file(str(p), key, bucket=bucket, public_base_url=public_base)
        urls.append(url)
    return urls
