from typing import Dict, Any, Optional
from fastapi import WebSocket, HTTPException
from uuid_extensions import uuid7, uuid7str
import time
from bizops.services.agents.intention_agent import IntentionAgent
from bizops.services.agents.planner_agent import PlannerAgent
from bizops.services.agents.context_agent import ContextAgent
from bizops.services.session_service import SessionService
from typing import List

class AssistantController:
    def __init__(self):
        self.intention_agent = IntentionAgent()
        self.planner_agent = PlannerAgent()
        self.context_agent = ContextAgent()
        self.session_service = SessionService()

    def chat_completions(self, query: str, context: Optional[Dict[str, Any]] = None, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Handle synchronous chat completions requests
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
                    raise HTTPException(status_code=404, detail="Session not found or expired")
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
            self._add_to_chat_history(session_id, query, response)

            return response

        except HTTPException as he:
            raise he
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def handle_websocket_chat(self, websocket: WebSocket, message: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

            response = self.chat_completions(
                query=message,
                context=session.context,
                session_id=session.session_id
            )

            return response

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def end_session(self, session_id: str) -> bool:
        """
        End a chat session
        """
        return self.session_service.end_session(session_id)

    def whisper(self, instruction: str, data: Optional[Dict[str, Any]] = None, 
                     context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Handle whisper requests for system instructions
        """
        try:
            # TODO: Implement actual whisper logic here
            response_text = f"Processed instruction: {instruction}"
            
            return {
                "text": response_text,
                "data": {
                    "instruction_type": "system_command",
                    "processed": True,
                    "instruction_data": data
                },
                "context": context,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "request_id": str(uuid7())
                }
            }
        except Exception as e:
            raise Exception(f"Error in whisper: {str(e)}")

    async def chat(self, websocket: WebSocket, message: str, session_id: Optional[str] = None,
                  context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Handle WebSocket chat messages
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
                    raise HTTPException(status_code=404, detail="Session not found or expired")
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

            return response

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_chat_history(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve chat history for a given session
        """
        return self.session_service.get_chat_history(session_id)

    def _add_to_chat_history(self, session_id: str, message: str, response: Dict[str, Any]) -> None:
        """
        Add a chat interaction to the session history
        """
        chat_message = {
            "user": message,
            "assistant": response["text"],
            "metadata": {
                "intention_id": response["metadata"]["intention_id"],
                "request_id": response["metadata"]["request_id"]
            }
        }
        self.session_service.add_to_chat_history(session_id, chat_message)