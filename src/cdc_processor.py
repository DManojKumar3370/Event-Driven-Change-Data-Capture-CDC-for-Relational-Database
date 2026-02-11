import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

class CDCProcessor:
    """CDC processor for detecting and transforming database changes."""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.last_sync_state = {}
    
    def transform_event(self, 
                       operation_type: str, 
                       primary_keys: Dict[str, Any], 
                       old_data: Optional[Dict[str, Any]] = None,
                       new_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Transform database changes into standardized CDC event format.
        
        Event Schema:
        {
            "event_id": "uuid-v4-string",
            "timestamp": "2023-10-27T10:30:00Z",
            "table_name": "products",
            "operation_type": "INSERT|UPDATE|DELETE",
            "primary_keys": {"id": 123},
            "payload": {
                "old_data": {...},  # Present for UPDATE/DELETE, null for INSERT
                "new_data": {...}   # Present for INSERT/UPDATE, null for DELETE
            }
        }
        """
        
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "table_name": self.table_name,
            "operation_type": operation_type,
            "primary_keys": primary_keys,
            "payload": {
                "old_data": old_data,
                "new_data": new_data
            }
        }
        
        logger.debug(f"Transformed {operation_type} event: {event['event_id'][:8]}...")
        return event
    
    def detect_changes(self, 
                      current_state: List[Dict[str, Any]], 
                      pk_column: str = 'id') -> List[Dict[str, Any]]:
        """
        Detect INSERT, UPDATE, and DELETE operations by comparing 
        current state with previous state.
        """
        
        events = []
        current_pks = {str(row[pk_column]): row for row in current_state}
        previous_pks = {str(pk): data for pk, data in self.last_sync_state.items()}
        
        # Detect INSERTs - new records in current state
        for pk, row in current_pks.items():
            if pk not in previous_pks:
                event = self.transform_event(
                    operation_type="INSERT",
                    primary_keys={pk_column: row[pk_column]},
                    new_data=row
                )
                events.append(event)
                logger.info(f"  → Detected INSERT: {pk_column}={pk}")
        
        # Detect UPDATEs - record exists but data changed
        for pk, row in current_pks.items():
            if pk in previous_pks:
                if previous_pks[pk] != row:
                    event = self.transform_event(
                        operation_type="UPDATE",
                        primary_keys={pk_column: row[pk_column]},
                        old_data=previous_pks[pk],
                        new_data=row
                    )
                    events.append(event)
                    logger.info(f"  → Detected UPDATE: {pk_column}={pk}")
        
        # Detect DELETEs - record existed but now missing
        for pk, row in previous_pks.items():
            if pk not in current_pks:
                event = self.transform_event(
                    operation_type="DELETE",
                    primary_keys={pk_column: int(pk)},
                    old_data=row
                )
                events.append(event)
                logger.info(f"  → Detected DELETE: {pk_column}={pk}")
        
        # Update state for next poll
        self.last_sync_state = {str(pk): row for pk, row in current_pks.items()}
        
        return events
