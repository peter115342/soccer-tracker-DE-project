from typing import Optional, List
from pydantic import BaseModel, field_validator


VALID_LEAGUE_CODES = {"PL", "FL1", "BL1", "SA", "PD"}


class CurrentSeason(BaseModel):
    startDate: str
    endDate: str
    currentMatchday: Optional[int] = None
    winner: Optional[object] = None


class LeagueTeam(BaseModel):
    """Contract for a single team from the football-data.org teams endpoint."""

    id: int
    name: str
    tla: Optional[str] = None
    crest: Optional[str] = None
    venue: Optional[str] = None
    address: Optional[str] = None

    @field_validator("id")
    @classmethod
    def id_positive(cls, v):
        if v < 1:
            raise ValueError(f"Team id must be >= 1, got {v}")
        return v


class LeagueContract(BaseModel):
    """Contract for raw league data from football-data.org API."""

    id: int
    name: str
    code: str
    emblem: Optional[str] = None
    currentSeason: CurrentSeason
    teams: Optional[List[LeagueTeam]] = None

    @field_validator("id")
    @classmethod
    def id_positive(cls, v):
        if v < 1:
            raise ValueError(f"League id must be >= 1, got {v}")
        return v

    @field_validator("code")
    @classmethod
    def valid_league_code(cls, v):
        if v not in VALID_LEAGUE_CODES:
            raise ValueError(
                f"Unknown league code: '{v}'. Expected one of {VALID_LEAGUE_CODES}"
            )
        return v
