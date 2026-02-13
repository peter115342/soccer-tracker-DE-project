from typing import Optional, List
from pydantic import BaseModel, field_validator


VALID_MATCH_STATUSES = {
    "SCHEDULED",
    "TIMED",
    "IN_PLAY",
    "PAUSED",
    "FINISHED",
    "POSTPONED",
    "CANCELLED",
    "SUSPENDED",
    "AWARDED",
}

VALID_COMPETITION_CODES = {"PL", "BL1", "PD", "SA", "FL1"}

VALID_WINNERS = {"HOME_TEAM", "AWAY_TEAM", "DRAW", None}

VALID_DURATIONS = {"REGULAR", "EXTRA_TIME", "PENALTY_SHOOTOUT"}


class Area(BaseModel):
    id: int
    name: str
    code: str
    flag: Optional[str] = None


class Competition(BaseModel):
    id: int
    name: str
    code: str
    type: Optional[str] = None
    emblem: Optional[str] = None


class Season(BaseModel):
    id: int
    startDate: str
    endDate: str
    currentMatchday: Optional[int] = None
    winner: Optional[object] = None


class Team(BaseModel):
    id: int
    name: str
    shortName: Optional[str] = None
    tla: Optional[str] = None
    crest: Optional[str] = None


class ScoreDetail(BaseModel):
    home: Optional[int] = None
    away: Optional[int] = None

    @field_validator("home", "away")
    @classmethod
    def score_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("Score cannot be negative")
        return v


class Score(BaseModel):
    winner: Optional[str] = None
    duration: Optional[str] = None
    fullTime: Optional[ScoreDetail] = None
    halfTime: Optional[ScoreDetail] = None

    @field_validator("winner")
    @classmethod
    def valid_winner(cls, v):
        if v is not None and v not in VALID_WINNERS:
            raise ValueError(f"Invalid winner value: {v}")
        return v

    @field_validator("duration")
    @classmethod
    def valid_duration(cls, v):
        if v is not None and v not in VALID_DURATIONS:
            raise ValueError(f"Invalid duration value: {v}")
        return v


class Referee(BaseModel):
    id: int
    name: str
    type: Optional[str] = None
    nationality: Optional[str] = None


class MatchContract(BaseModel):
    """Contract for raw match data from football-data.org API."""

    id: int
    utcDate: str
    status: str
    matchday: Optional[int] = None
    stage: Optional[str] = None
    group: Optional[str] = None
    lastUpdated: Optional[str] = None
    area: Optional[Area] = None
    competition: Optional[Competition] = None
    season: Optional[Season] = None
    homeTeam: Team
    awayTeam: Team
    score: Optional[Score] = None
    odds: Optional[object] = None
    referees: Optional[List[Referee]] = None

    @field_validator("status")
    @classmethod
    def valid_status(cls, v):
        if v not in VALID_MATCH_STATUSES:
            raise ValueError(
                f"Invalid match status: '{v}'. Must be one of {VALID_MATCH_STATUSES}"
            )
        return v

    @field_validator("matchday")
    @classmethod
    def matchday_positive(cls, v):
        if v is not None and v < 1:
            raise ValueError(f"Matchday must be >= 1, got {v}")
        return v
