import reflex as rx
from sqlmodel import Field, SQLModel, Relationship
from datetime import datetime
from typing import List, Optional

class User(rx.Model, table=True):
    """User profile."""
    email: str = Field(unique=True, index=True, nullable=False)
    meetings: List["Meeting"] = Relationship(back_populates="user")

class Meeting(rx.Model, table=True):
    """Meeting details."""
    user_id: int = Field(foreign_key="user.id")
    title: str
    teams_link: str
    status: str = "pending"  # pending, joining, active, completed
    bot_pid: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    user: Optional[User] = Relationship(back_populates="meetings")
    transcripts: List["Transcript"] = Relationship(back_populates="meeting")

class Transcript(rx.Model, table=True):
    """Meeting transcripts."""
    meeting_id: int = Field(foreign_key="meeting.id")
    speaker: str
    text: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    meeting: Optional[Meeting] = Relationship(back_populates="transcripts")
