from typing import Dict, Any, Optional, List, Tuple
import uuid
from datetime import datetime, timedelta
import time
from bizops.services.postgres import PostgresService


class Session:
    def __init__(self, session_id: str, context: Optional[Dict[str, Any]] = None):
        self.session_id = session_id
        self.context = context or {}
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        self.is_active = True
        self._db = PostgresService()
        
        # Create session in database
        self._db.create_session(
            session_id=self.session_id,
            context=self.context,
            created_at=self.created_at.isoformat(),
            last_accessed=self.last_accessed.isoformat()
        )

    def update_last_accessed(self):
        self.last_accessed = datetime.now()
        self._db.update_session(
            session_id=self.session_id,
            context=self.context,
            last_accessed=self.last_accessed.isoformat()
        )

    def update_context(self, new_context: Dict[str, Any]):
        self.context.update(new_context)
        self._db.update_session(
            session_id=self.session_id,
            context=self.context,
            last_accessed=self.last_accessed.isoformat()
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "context": self.context,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "is_active": self.is_active
        }

    @classmethod
    def from_db(cls, session_data: Dict[str, Any]) -> Optional['Session']:
        """Create a Session instance from database data"""
        if not session_data:
            return None
            
        session = cls(
            session_id=session_data["session_id"],
            context=session_data["context"]
        )
        session.created_at = datetime.fromisoformat(session_data["created_at"])
        session.last_accessed = datetime.fromisoformat(session_data["last_accessed"])
        session.is_active = session_data["is_active"]
        return session


class SessionService:
    def __init__(self, session_timeout: int = 30):  # timeout in minutes
        self._db = PostgresService()
        self.session_timeout = timedelta(minutes=session_timeout)

    def create_session(self, context: Optional[Dict[str, Any]] = None) -> Session:
        session_id = str(uuid.uuid4())
        return Session(session_id=session_id, context=context)

    def get_session(self, session_id: str) -> Optional[Session]:
        """
        Get a session by ID. Returns None if session doesn't exist or is expired.
        """
        session_data = self._db.get_session(session_id)
        if not session_data:
            return None
            
        session = Session.from_db(session_data)
        if session and session.is_active:
            session.update_last_accessed()
            return session
        return None

    def end_session(self, session_id: str) -> bool:
        """
        End a session. Returns True if session was ended, False if it doesn't exist.
        """
        return self._db.end_session(session_id)

    def cleanup_expired_sessions(self) -> None:
        """
        Clean up expired sessions based on timeout.
        """
        expiry_time = (datetime.now() - self.session_timeout).isoformat()
        self._db.cleanup_expired_sessions(expiry_time)

    def get_chat_history(self, session_id: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Retrieve chat history for a given session.
        Returns (history, error_message). If error_message is not None, history will be None.
        """
        session = self.get_session(session_id)
        if not session:
            return None, "Session not found or expired"
        return session.context.get("chat_history", []), None

    def add_to_chat_history(self, session_id: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Add a message to the session's chat history.
        Returns (success, error_message). If error_message is not None, success will be False.
        """
        session = self.get_session(session_id)
        if not session:
            return False, "Session not found or expired"
        
        if "chat_history" not in session.context:
            session.context["chat_history"] = []
        
        session.context["chat_history"].append({
            **message,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        })
        
        # Update session in database
        session.update_context(session.context)
        return True, None
