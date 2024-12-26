from typing import Dict, Any, Optional, List
from uuid7 import uuid7
import time
from enum import Enum, auto

class TaskType(Enum):
    SQL_GENERATION = auto()
    DATA_QUERY = auto()
    SCHEMA_LOOKUP = auto()
    CLARIFICATION = auto()
    REFINEMENT = auto()
    EXPLANATION = auto()
    VALIDATION = auto()

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class PlannerAgent:
    def __init__(self):
        self.plans: Dict[str, Dict[str, Any]] = {}
        self.tasks: Dict[str, Dict[str, Any]] = {}

    async def create_plan(self, intention: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an execution plan from an analyzed intention
        Returns a plan with ordered tasks
        """
        try:
            plan_id = str(uuid7())
            
            # Extract relevant information from intention
            intent_type = intention["type"]
            primary_intent = intention["analysis"]["primary_intent"]
            sub_intents = intention["analysis"].get("sub_intents", [])
            entities = intention["analysis"].get("entities", {})
            context = intention.get("context", {})
            
            # Initialize tasks list
            tasks: List[Dict[str, Any]] = []
            
            if intent_type == "completion":
                tasks = await self._plan_completion_tasks(primary_intent, sub_intents, entities, context)
            elif intent_type == "chat":
                tasks = await self._plan_chat_tasks(
                    primary_intent, 
                    sub_intents, 
                    entities, 
                    context,
                    intention["analysis"].get("conversation_state", {})
                )
            
            plan = {
                "plan_id": plan_id,
                "intention_id": intention["metadata"]["intention_id"],
                "type": intent_type,
                "tasks": tasks,
                "status": TaskStatus.PENDING.value,
                "metadata": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
                    "total_tasks": len(tasks)
                }
            }
            
            # Store plan and tasks
            self.plans[plan_id] = plan
            for task in tasks:
                self.tasks[task["task_id"]] = task
            
            return plan
            
        except Exception as e:
            raise Exception(f"Error creating plan: {str(e)}")

    async def _plan_completion_tasks(
        self, 
        primary_intent: str, 
        sub_intents: List[str], 
        entities: Dict[str, Any],
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Plan tasks for completion requests
        """
        tasks = []
        
        if primary_intent == "sql_generation":
            # Add schema lookup task if needed
            if entities.get("tables") or entities.get("fields"):
                tasks.append({
                    "task_id": str(uuid7()),
                    "type": TaskType.SCHEMA_LOOKUP.name,
                    "priority": 1,
                    "dependencies": [],
                    "parameters": {
                        "tables": entities.get("tables", []),
                        "fields": entities.get("fields", [])
                    },
                    "status": TaskStatus.PENDING.value
                })
            
            # Add main SQL generation task
            tasks.append({
                "task_id": str(uuid7()),
                "type": TaskType.SQL_GENERATION.name,
                "priority": 2,
                "dependencies": [t["task_id"] for t in tasks],  # Depend on schema lookup if present
                "parameters": {
                    "entities": entities,
                    "context": context
                },
                "status": TaskStatus.PENDING.value
            })
            
            # Add validation task
            tasks.append({
                "task_id": str(uuid7()),
                "type": TaskType.VALIDATION.name,
                "priority": 3,
                "dependencies": [tasks[-1]["task_id"]],  # Depend on SQL generation
                "parameters": {},
                "status": TaskStatus.PENDING.value
            })
        
        return tasks

    async def _plan_chat_tasks(
        self, 
        primary_intent: str, 
        sub_intents: List[str], 
        entities: Dict[str, Any],
        context: Dict[str, Any],
        conversation_state: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Plan tasks for chat messages
        """
        tasks = []
        
        if primary_intent == "clarification":
            tasks.append({
                "task_id": str(uuid7()),
                "type": TaskType.CLARIFICATION.name,
                "priority": 1,
                "dependencies": [],
                "parameters": {
                    "entities": entities,
                    "context": context,
                    "conversation_state": conversation_state
                },
                "status": TaskStatus.PENDING.value
            })
        
        elif primary_intent == "refinement":
            # Add task to refine previous query
            tasks.append({
                "task_id": str(uuid7()),
                "type": TaskType.REFINEMENT.name,
                "priority": 1,
                "dependencies": [],
                "parameters": {
                    "entities": entities,
                    "context": context,
                    "conversation_state": conversation_state,
                    "previous_intents": conversation_state.get("previous_intents", [])
                },
                "status": TaskStatus.PENDING.value
            })
            
            # Add validation task for the refinement
            tasks.append({
                "task_id": str(uuid7()),
                "type": TaskType.VALIDATION.name,
                "priority": 2,
                "dependencies": [tasks[-1]["task_id"]],
                "parameters": {},
                "status": TaskStatus.PENDING.value
            })
        
        return tasks

    def get_plan(self, plan_id: str) -> Dict[str, Any]:
        """
        Retrieve a specific plan by ID
        """
        if plan_id not in self.plans:
            raise Exception("Invalid plan ID")
        return self.plans[plan_id]

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """
        Retrieve a specific task by ID
        """
        if task_id not in self.tasks:
            raise Exception("Invalid task ID")
        return self.tasks[task_id]

    async def update_task_status(self, task_id: str, status: TaskStatus, result: Optional[Dict[str, Any]] = None):
        """
        Update task status and optionally store its result
        """
        if task_id not in self.tasks:
            raise Exception("Invalid task ID")
            
        task = self.tasks[task_id]
        task["status"] = status.value
        if result:
            task["result"] = result
        
        # Update plan status if all tasks are completed
        plan = self.plans[task["plan_id"]]
        all_tasks_completed = all(
            self.tasks[t["task_id"]]["status"] == TaskStatus.COMPLETED.value 
            for t in plan["tasks"]
        )
        if all_tasks_completed:
            plan["status"] = TaskStatus.COMPLETED.value
