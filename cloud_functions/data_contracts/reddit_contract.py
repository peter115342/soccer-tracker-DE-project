from typing import Optional, List
from pydantic import BaseModel, field_validator


class RedditComment(BaseModel):
    id: str
    body: str
    score: int
    author: Optional[str] = None
    created_utc: int

    @field_validator("created_utc")
    @classmethod
    def timestamp_positive(cls, v):
        if v <= 0:
            raise ValueError(f"created_utc must be positive, got {v}")
        return v


class RedditThread(BaseModel):
    """Contract for a single Reddit thread from r/soccer."""

    thread_id: str
    title: str
    body: Optional[str] = None
    created_utc: int
    score: int
    upvote_ratio: Optional[float] = None
    num_comments: int
    flair: Optional[str] = None
    author: Optional[str] = None
    url: Optional[str] = None
    top_comments: Optional[List[RedditComment]] = None

    @field_validator("created_utc")
    @classmethod
    def timestamp_positive(cls, v):
        if v <= 0:
            raise ValueError(f"created_utc must be positive, got {v}")
        return v

    @field_validator("num_comments")
    @classmethod
    def comments_non_negative(cls, v):
        if v < 0:
            raise ValueError(f"num_comments cannot be negative: {v}")
        return v

    @field_validator("upvote_ratio")
    @classmethod
    def ratio_in_range(cls, v):
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError(f"upvote_ratio must be between 0.0 and 1.0, got {v}")
        return v


class RedditDataContract(BaseModel):
    """Contract for a day's worth of Reddit thread data saved to GCS."""

    date: str
    threads: List[RedditThread]
    thread_count: int

    @field_validator("thread_count")
    @classmethod
    def count_non_negative(cls, v):
        if v < 0:
            raise ValueError(f"thread_count cannot be negative: {v}")
        return v
