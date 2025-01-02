from fastapi import APIRouter, WebSocket, HTTPException, Depends
from pydantic import BaseModel
import uuid
from typing import Optional, Dict, Any, Union
from bizops.controller.assistant import AssistantController

router = APIRouter(
    prefix="/nl2sql",
    tags=["nl2sql"]
)

# Initialize controller
assistant_controller = AssistantController()

class CompletionsRequest(BaseModel):
    query: str
    context: Optional[Dict[str, Any]] = None

class WhisperRequest(BaseModel):
    instruction: str
    data: Optional[Dict[str, Any]] = None
    context: Optional[Dict[str, Any]] = None

class ResponseWrapper(BaseModel):
    text: str
    data: Optional[Dict[str, Any]] = None
    context: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None

@router.post("/chat/completions", response_model=ResponseWrapper)
def chat_completions(request: CompletionsRequest):
    """
    Synchronous completion API endpoint with enhanced response structure
    """
    try:
        return assistant_controller.chat_completions(
            prompt=request.query,
            context=request.context
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/whisper", response_model=ResponseWrapper)
def whisper(request: WhisperRequest):
    """
    Synchronous whisper API for passing instructions or data without a chat window
    """
    try:
        return assistant_controller.whisper(
            instruction=request.instruction,
            data=request.data,
            context=request.context
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/chat")
async def chat(websocket: WebSocket):
    """
    WebSocket endpoint for streaming chat responses with enhanced response structure
    """
    await websocket.accept()
    
    try:
        while True:
            # Receive the chat request
            data = await websocket.receive_json()
            request = ChatRequest(**data)

            # Process chat message using controller
            response = await assistant_controller.chat(
                websocket=websocket,
                message=request.message,
                session_id=request.session_id,
                context=request.context
            )

            # Send response
            await websocket.send_json(response)

    except Exception as e:
        await websocket.send_json({
            "error": str(e)
        })
    finally:
        await websocket.close()