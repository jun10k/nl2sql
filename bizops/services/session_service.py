from typing import Dict, Any, Optional, List
import uuid
from datetime import datetime, timedelta
from http import HTTPException
import time


class Session:
    def __init__(self, session_id: str, context: Optional[Dict[str, Any]] = None):
        self.session_id = session_id
        self.context = context or {}
        self.created_at = datetime.now()
        self.last_accessed = datetime.now()
        self.is_active = True

    def update_last_accessed(self):
        self.last_accessed = datetime.now()

    def update_context(self, new_context: Dict[str, Any]):
        self.context.update(new_context)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "context": self.context,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "is_active": self.is_active
        }


class SessionService:
    def __init__(self, session_timeout: int = 30):  # timeout in minutes
        self._sessions: Dict[str, Session] = {}
        self.session_timeout = timedelta(minutes=session_timeout)

    def create_session(self, context: Optional[Dict[str, Any]] = None) -> Session:
        session_id = str(uuid.uuid4())
        session = Session(session_id=session_id, context=context)
        self._sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        session = self._sessions.get(session_id)
        if session and session.is_active:
            session.update_last_accessed()
            return session
        return None

    def update_session_context(self, session_id: str, context: Dict[str, Any]) -> Optional[Session]:
        session = self.get_session(session_id)
        if session:
            session.update_context(context)
            return session
        return None

    def end_session(self, session_id: str) -> bool:
        if session_id in self._sessions:
            self._sessions[session_id].is_active = False
            return True
        return False

    def cleanup_expired_sessions(self):
        current_time = datetime.now()
        expired_sessions = [
            session_id for session_id, session in self._sessions.items()
            if current_time - session.last_accessed > self.session_timeout
        ]
        for session_id in expired_sessions:
            self.end_session(session_id)

    def get_active_sessions(self) -> Dict[str, Session]:
        return {
            session_id: session 
            for session_id, session in self._sessions.items() 
            if session.is_active
        }

    def get_chat_history(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve chat history for a given session
        """
        session = self.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        return session.context.get("chat_history", [])

    def add_to_chat_history(self, session_id: str, message: Dict[str, Any]) -> bool:
        """
        Add a message to the session's chat history
        """
        session = self.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        if "chat_history" not in session.context:
            session.context["chat_history"] = []
        
        session.context["chat_history"].append({
            **message,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
        })
        return True
