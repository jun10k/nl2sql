from typing import Dict, Any, Optional
from fastapi import WebSocket
from uuid_extensions import uuid7, uuid7str
import time
from bizops.services.agents.intention_agent import IntentionAgent
from bizops.services.agents.planner_agent import PlannerAgent
from bizops.services.agents.context_agent import ContextAgent

class AssistantController:
    def __init__(self):
        self.chat_sessions: Dict[str, dict] = {}
        self.intention_agent = IntentionAgent()
        self.planner_agent = PlannerAgent()
        self.context_agent = ContextAgent()

    def chat_completions(self, prompt: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Handle synchronous chat completions requests
        """
        try:
            # Create a temporary session for the completion request
            session_id = str(uuid7())
            
            # Initialize context
            self.context_agent.create_session_context(session_id, context)
            
            # Analyze user intention
            intention = self.intention_agent.analyze_completion_intention(prompt, context)
            
            # Refine context based on intention
            refined_context = self.context_agent.refine_context(session_id, intention)
            
            # Create execution plan only if intention is executable
            plan = None
            if intention["analysis"]["is_executable"]:
                plan = self.planner_agent.create_plan(intention)
            
            # TODO: Execute plan and generate response
            response_text = f"Generated response for: {prompt}"
            
            return {
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
                    "plan_id": plan and plan["plan_id"],
                    "context_version": refined_context["version"]
                }
            }
        except Exception as e:
            raise Exception(f"Error in chat completions: {str(e)}")

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

    async def chat(self, websocket: WebSocket, message: str, 
                  session_id: Optional[str] = None,
                  context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Handle WebSocket chat sessions
        """
        try:
            # Create new session if none exists
            is_new_session = False
            if not session_id:
                session_id = str(uuid7())
                is_new_session = True
                self.chat_sessions[session_id] = {
                    "history": [],
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%S+08:00")
                }
                # Initialize context for new session
                await self.context_agent.create_session_context(session_id, context)
            elif session_id not in self.chat_sessions:
                raise Exception("Invalid session ID")

            # Analyze user intention with chat history and intention history
            current_context = await self.context_agent.get_context(session_id)
            intention = await self.intention_agent.analyze_chat_intention(
                message=message,
                session_id=session_id,
                context=current_context["data"],
                chat_history=self.chat_sessions[session_id]["history"]
            )
            
            # Update intention history in context
            await self.context_agent.update_intention_history(session_id, intention)
            
            # Refine context based on intention
            refined_context = await self.context_agent.refine_context(session_id, intention["primary_intention"])
            
            # Create execution plan for executable intentions
            plans = {}
            if intention["primary_intention"]["analysis"]["is_executable"]:
                plans["primary"] = await self.planner_agent.create_plan(intention["primary_intention"])
            
            for additional in intention.get("additional_intentions", []):
                if additional["analysis"]["is_executable"]:
                    plans[additional["id"]] = await self.planner_agent.create_plan(additional)
            
            # TODO: Execute plans and generate response
            response_text = f"Chat response to: {message}"
            
            # Update session history with intention and plan
            chat_message = {
                "user": message,
                "assistant": response_text,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                "intention_id": intention["metadata"]["intention_id"],
                "plan_ids": [p["plan_id"] for p in plans.values()],
                "context_version": refined_context["version"]
            }
            self.chat_sessions[session_id]["history"].append(chat_message)

            return {
                "session_id": session_id,
                "is_new_session": is_new_session,
                "response": {
                    "text": response_text,
                    "data": {
                        "tokens_used": 30,
                        "model": "default-chat-model",
                        "intention": {
                            "primary": intention["primary_intention"]["analysis"],
                            "additional": [i["analysis"] for i in intention.get("additional_intentions", [])]
                        },
                        "context": refined_context["data"],
                        "plans": {
                            k: {
                                "id": v["plan_id"],
                                "tasks": [{"type": t["type"], "status": t["status"]} for t in v["tasks"]]
                            } for k, v in plans.items()
                        }
                    },
                    "context": context,
                    "metadata": {
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                        "session_created_at": self.chat_sessions[session_id].get("created_at"),
                        "message_id": str(uuid7()),
                        "intention_id": intention["metadata"]["intention_id"],
                        "plan_ids": [p["plan_id"] for p in plans.values()],
                        "context_version": refined_context["version"],
                        "total_intentions": intention["metadata"]["total_intentions"]
                    }
                }
            }
        except Exception as e:
            raise Exception(f"Error in chat session: {str(e)}")

    def get_chat_history(self, session_id: str) -> list:
        """
        Retrieve chat history for a given session
        """
        if session_id not in self.chat_sessions:
            raise Exception("Invalid session ID")
        return self.chat_sessions[session_id]["history"]