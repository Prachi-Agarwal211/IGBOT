import os
from typing import Optional
import ffmpeg

"""
Utilities for preparing Reel-ready videos (9:16, ~1080x1920) using ffmpeg-python.
- Resizes and pads with blurred background if the input is not 9:16
- Normalizes audio loudness (EBU R128)
- Trims to max_duration seconds
- Sets target fps and bitrate

Note: Instagram Graph API requires a publicly accessible URL for video_url. 
This module outputs local files; you must host them or provide a reachable URL for posting.
"""


def make_9_16(
    input_path: str,
    output_path: str,
    *,
    max_duration: int = 58,
    target_width: int = 1080,
    target_height: int = 1920,
    target_fps: int = 30,
    video_bitrate: str = "5M",
    audio_bitrate: str = "128k",
    loudness_i: float = -14.0,
    loudness_tp: float = -1.5,
    loudness_lra: float = 11.0,
) -> None:
    """Convert any video to a 9:16 vertical canvas with letterbox blur if needed.

    Args:
        input_path: source video path
        output_path: destination mp4 path
        max_duration: maximum duration in seconds (trim if longer)
        target_width, target_height: output dimensions
        target_fps: frames per second
        video_bitrate, audio_bitrate: encoding bitrates
        loudness_*: EBU R128 target values for audio normalization
    """
    # Probes
    probe = ffmpeg.probe(input_path)
    v_streams = [s for s in probe["streams"] if s["codec_type"] == "video"]
    if not v_streams:
        raise RuntimeError("No video stream found")
    vw = int(v_streams[0].get("width", 0))
    vh = int(v_streams[0].get("height", 0))

    # Inputs
    inp = ffmpeg.input(input_path, ss=0, t=max_duration)

    # Scale foreground to fit within 9:16 while preserving aspect
    # Use scale with -1 for dynamic dimension and pad to target canvas
    scale_fg = ffmpeg.filter(inp.video, "scale", w=f"if(gt(a,{target_width}/{target_height}),{target_width},-2)",
                             h=f"if(gt(a,{target_width}/{target_height}),-2,{target_height})")
    # Create blurred background from the same source
    bg_scale = ffmpeg.filter(inp.video, "scale", w=target_width, h=target_height)
    bg_blur = ffmpeg.filter(bg_scale, "boxblur", luma_radius=20, luma_power=1)

    # Center the foreground onto the blurred background
    x = f"(W-w)/2"
    y = f"(H-h)/2"
    overlaid = ffmpeg.overlay(bg_blur, scale_fg, x=x, y=y)

    # Set fps
    v_out = ffmpeg.filter(overlaid, "fps", fps=target_fps)

    # Audio: loudness normalization (EBU R128)
    a_norm = ffmpeg.filter(inp.audio, "loudnorm", i=loudness_i, tp=loudness_tp, lra=loudness_lra)

    # Output settings
    out = ffmpeg.output(
        v_out,
        a_norm,
        output_path,
        vcodec="libx264",
        acodec="aac",
        video_bitrate=video_bitrate,
        audio_bitrate=audio_bitrate,
        pix_fmt="yuv420p",
        movflags="+faststart",
        r=target_fps,
        vf=None,
        shortest=None,
    )
    out = ffmpeg.overwrite_output(out)
    out.run(quiet=True)


def batch_process_directory(
    in_dir: str,
    out_dir: str,
    *,
    max_duration: int = 58,
    target_width: int = 1080,
    target_height: int = 1920,
    target_fps: int = 30,
    video_bitrate: str = "5M",
    audio_bitrate: str = "128k",
) -> list[tuple[str, str]]:
    """Process all video files in a directory to 9:16 reels.

    Returns list of (src, dest) tuples for successfully processed items.
    """
    os.makedirs(out_dir, exist_ok=True)
    exts = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}
    out_rows: list[tuple[str, str]] = []
    for name in os.listdir(in_dir):
        src = os.path.join(in_dir, name)
        if not os.path.isfile(src):
            continue
        _, ext = os.path.splitext(name.lower())
        if ext not in exts:
            continue
        dest_name = os.path.splitext(name)[0] + "_9x16.mp4"
        dest = os.path.join(out_dir, dest_name)
        try:
            make_9_16(
                src,
                dest,
                max_duration=max_duration,
                target_width=target_width,
                target_height=target_height,
                target_fps=target_fps,
                video_bitrate=video_bitrate,
                audio_bitrate=audio_bitrate,
            )
            out_rows.append((src, dest))
        except Exception as e:
            # Continue other files
            print(f"Reels process failed for {name}: {e}")
    return out_rows
