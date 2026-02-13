from typing import Optional, List
from pydantic import BaseModel, field_validator, model_validator


class StandingsTeam(BaseModel):
    id: int
    name: str
    shortName: Optional[str] = None
    tla: Optional[str] = None
    crest: Optional[str] = None


class StandingsTableEntry(BaseModel):
    """Contract for a single team's standing in a league table."""

    position: int
    team: StandingsTeam
    playedGames: int
    form: Optional[str] = None
    won: int
    draw: int
    lost: int
    points: int
    goalsFor: int
    goalsAgainst: int
    goalDifference: int

    @field_validator("position")
    @classmethod
    def position_positive(cls, v):
        if v < 1:
            raise ValueError(f"Position must be >= 1, got {v}")
        return v

    @field_validator(
        "playedGames", "won", "draw", "lost", "points", "goalsFor", "goalsAgainst"
    )
    @classmethod
    def non_negative_stats(cls, v):
        if v < 0:
            raise ValueError(f"Stat value cannot be negative: {v}")
        return v

    @model_validator(mode="after")
    def games_add_up(self):
        if self.won + self.draw + self.lost != self.playedGames:
            raise ValueError(
                f"W({self.won}) + D({self.draw}) + L({self.lost}) = "
                f"{self.won + self.draw + self.lost} != playedGames({self.playedGames})"
            )
        return self

    @model_validator(mode="after")
    def goal_difference_correct(self):
        expected = self.goalsFor - self.goalsAgainst
        if self.goalDifference != expected:
            raise ValueError(
                f"goalDifference({self.goalDifference}) != "
                f"goalsFor({self.goalsFor}) - goalsAgainst({self.goalsAgainst}) = {expected}"
            )
        return self


class StandingsGroup(BaseModel):
    stage: Optional[str] = None
    type: Optional[str] = None
    group: Optional[str] = None
    table: List[StandingsTableEntry]


class StandingsArea(BaseModel):
    id: int
    name: str
    code: Optional[str] = None
    flag: Optional[str] = None


class StandingsCompetition(BaseModel):
    id: int
    name: str
    code: str
    type: Optional[str] = None
    emblem: Optional[str] = None


class StandingsSeason(BaseModel):
    id: int
    startDate: str
    endDate: str
    currentMatchday: Optional[int] = None
    winner: Optional[object] = None


class StandingsContract(BaseModel):
    """Contract for raw standings data from football-data.org API."""

    filters: Optional[object] = None
    area: Optional[StandingsArea] = None
    competition: StandingsCompetition
    season: StandingsSeason
    standings: List[StandingsGroup]
    fetchDate: Optional[str] = None
    competitionId: Optional[str] = None
