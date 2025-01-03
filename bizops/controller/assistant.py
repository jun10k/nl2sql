from typing import Dict, Any, Optional, List, Tuple
from fastapi import WebSocket
from uuid_extensions import uuid7, uuid7str
import time
from bizops.services.agents.intention_agent import IntentionAgent
from bizops.services.agents.planner_agent import PlannerAgent
from bizops.services.agents.context_agent import ContextAgent
from bizops.services.session_service import SessionService

class AssistantController:
    def __init__(self):
        self.intention_agent = IntentionAgent()
        self.planner_agent = PlannerAgent()
        self.context_agent = ContextAgent()
        self.session_service = SessionService()

    def chat_completions(self, query: str, context: Optional[Dict[str, Any]] = None, 
                        session_id: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Handle synchronous chat completions requests
        Returns (response, error). If error is not None, response will be None.
        """
        try:
            # Get or create session
            if not session_id:
                session = self.session_service.create_session(context=context)
                session_id = session.session_id
                context = session.context
            else:
                session = self.session_service.get_session(session_id)
                if not session:
                    return None, "Session not found or expired"
                context = session.context

            # Analyze user intention
            intention = self.intention_agent.analyze_completion_intention(
                query=query,
                session_id=session_id,
                context=context
            )
            
            # Refine context based on intention
            refined_context = self.context_agent.refine_context(session_id, intention)
            
            # Create execution plan only if intention is executable
            plan = None
            if intention["analysis"]["is_executable"]:
                plan = self.planner_agent.create_plan(intention)
            
            # TODO: Execute plan and generate response
            response_text = f"Generated response for: {query}"
            
            response = {
                "text": response_text,
                "data": {
                    "tokens_used": 50,
                    "model": "default-completion-model",
                    "intention": intention["analysis"],
                    "context": refined_context["data"],
                    "plan": plan and {
                        "id": plan["plan_id"],
                        "tasks": [{"type": t["type"], "status": t["status"]} for t in plan["tasks"]]
                    }
                },
                "context": context,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "request_id": str(uuid7()),
                    "intention_id": intention["metadata"]["intention_id"],
                    "session_id": session_id
                }
            }

            # Update session context with response data
            if response["context"]:
                session.update_context(response["context"])

            # Add to chat history
            success, error = self._add_to_chat_history(session_id, query, response)
            if not success:
                return None, error

            return response, None

        except Exception as e:
            return None, f"Error in chat completions: {str(e)}"

    async def handle_websocket_chat(self, websocket: WebSocket, message: str, context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Handle WebSocket chat messages
        """
        try:
            # Create or get session from websocket state
            if not hasattr(websocket.state, "session"):
                websocket.state.session = self.session_service.create_session(context=context)
            session = websocket.state.session

            # Update session context if provided
            if context:
                session.update_context(context)

            response, error = self.chat_completions(
                query=message,
                context=session.context,
                session_id=session.session_id
            )

            return response, error

        except Exception as e:
            return None, f"Error in websocket chat: {str(e)}"

    def end_session(self, session_id: str) -> Tuple[bool, Optional[str]]:
        """
        End a chat session
        Returns (success, error). If error is not None, success will be False.
        """
        return self.session_service.end_session(session_id)

    def get_chat_history(self, session_id: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Retrieve chat history for a given session
        Returns (history, error). If error is not None, history will be None.
        """
        return self.session_service.get_chat_history(session_id)

    def _add_to_chat_history(self, session_id: str, message: str, 
                           response: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Add a chat interaction to the session history
        Returns (success, error). If error is not None, success will be False.
        """
        chat_message = {
            "user": message,
            "assistant": response["text"],
            "metadata": {
                "intention_id": response["metadata"]["intention_id"],
                "request_id": response["metadata"]["request_id"]
            }
        }
        return self.session_service.add_to_chat_history(session_id, chat_message)

    async def chat(self, websocket: WebSocket, message: str, 
                  session_id: Optional[str] = None,
                  context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Handle WebSocket chat messages
        Returns (response, error). If error is not None, response will be None.
        """
        try:
            # Get or create session
            if not session_id:
                session = self.session_service.create_session(context=context)
                session_id = session.session_id
                context = session.context
            else:
                session = self.session_service.get_session(session_id)
                if not session:
                    return None, "Session not found or expired"
                context = session.context

            # Analyze chat intention
            intention = self.intention_agent.analyze_chat_intention(
                message=message,
                session_id=session_id,
                context=context
            )

            # Process intention similar to chat_completions
            refined_context = self.context_agent.refine_context(session_id, intention)
            
            plan = None
            if intention["analysis"]["is_executable"]:
                plan = self.planner_agent.create_plan(intention)

            response = {
                "text": f"Generated response for: {message}",  # TODO: Implement actual response generation
                "data": {
                    "tokens_used": 50,
                    "model": "default-chat-model",
                    "intention": intention["analysis"],
                    "context": refined_context["data"],
                    "plan": plan and {
                        "id": plan["plan_id"],
                        "tasks": [{"type": t["type"], "status": t["status"]} for t in plan["tasks"]]
                    }
                },
                "context": context,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "request_id": str(uuid7()),
                    "intention_id": intention["metadata"]["intention_id"],
                    "session_id": session_id
                }
            }

            # Update session context with response data
            if response["context"]:
                session.update_context(response["context"])

            # Add to chat history
            success, error = self._add_to_chat_history(session_id, message, response)
            if not success:
                return None, error

            return response, None

        except Exception as e:
            return None, f"Error in chat: {str(e)}"

    def whisper(self, instruction: str, data: Optional[Dict[str, Any]] = None, 
                context: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Handle whisper requests for system instructions
        Returns (response, error). If error is not None, response will be None.
        """
        try:
            # Process whisper instruction
            response = {
                "text": f"Processed whisper: {instruction}",
                "data": data,
                "context": context,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "request_id": str(uuid7())
                }
            }
            return response, None
        except Exception as e:
            return None, f"Error in whisper: {str(e)}"