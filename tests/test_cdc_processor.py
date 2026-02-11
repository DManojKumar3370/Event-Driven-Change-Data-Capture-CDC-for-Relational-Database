import pytest
import json
from datetime import datetime
from src.cdc_processor import CDCProcessor

class TestCDCProcessor:
    """Unit tests for CDC Processor - Event Transformation and Change Detection."""
    
    def setup_method(self):
        """Setup test fixtures before each test."""
        self.processor = CDCProcessor("products")
    
    # ==================== EVENT TRANSFORMATION TESTS ====================
    
    def test_transform_insert_event(self):
        """Test INSERT event transformation."""
        new_data = {
            "id": 1,
            "name": "Laptop",
            "description": "High-performance laptop",
            "price": 1200.00,
            "stock": 50
        }
        event = self.processor.transform_event(
            operation_type="INSERT",
            primary_keys={"id": 1},
            new_data=new_data
        )
        
        # Assertions
        assert event["operation_type"] == "INSERT"
        assert event["table_name"] == "products"
        assert event["primary_keys"] == {"id": 1}
        assert event["payload"]["new_data"] == new_data
        assert event["payload"]["old_data"] is None
        assert "event_id" in event
        assert "timestamp" in event
        assert event["timestamp"].endswith("Z")
    
    def test_transform_update_event(self):
        """Test UPDATE event transformation."""
        old_data = {
            "id": 1,
            "name": "Laptop",
            "description": "High-performance laptop",
            "price": 1200.00,
            "stock": 50
        }
        new_data = {
            "id": 1,
            "name": "Laptop",
            "description": "High-performance laptop",
            "price": 1100.00,
            "stock": 45
        }
        
        event = self.processor.transform_event(
            operation_type="UPDATE",
            primary_keys={"id": 1},
            old_data=old_data,
            new_data=new_data
        )
        
        # Assertions
        assert event["operation_type"] == "UPDATE"
        assert event["payload"]["old_data"] == old_data
        assert event["payload"]["new_data"] == new_data
        assert event["primary_keys"]["id"] == 1
    
    def test_transform_delete_event(self):
        """Test DELETE event transformation."""
        old_data = {
            "id": 1,
            "name": "Laptop",
            "description": "High-performance laptop",
            "price": 1200.00,
            "stock": 50
        }
        
        event = self.processor.transform_event(
            operation_type="DELETE",
            primary_keys={"id": 1},
            old_data=old_data
        )
        
        # Assertions
        assert event["operation_type"] == "DELETE"
        assert event["payload"]["old_data"] == old_data
        assert event["payload"]["new_data"] is None
        assert event["primary_keys"]["id"] == 1
    
    # ==================== CHANGE DETECTION TESTS ====================
    
    def test_detect_insert(self):
        """Test detection of INSERT operations."""
        # Initialize with one record
        initial_state = [
            {"id": 1, "name": "Laptop", "price": 1200.00, "stock": 50}
        ]
        self.processor.last_sync_state = {"1": initial_state[0]}
        
        # Current state with new record
        current_state = [
            {"id": 1, "name": "Laptop", "price": 1200.00, "stock": 50},
            {"id": 2, "name": "Mouse", "price": 25.00, "stock": 200}
        ]
        
        # Detect changes
        events = self.processor.detect_changes(current_state, pk_column='id')
        
        # Assertions
        assert len(events) == 1
        assert events[0]["operation_type"] == "INSERT"
        assert events[0]["primary_keys"]["id"] == 2
        assert events[0]["payload"]["new_data"]["name"] == "Mouse"
    
    def test_detect_update(self):
        """Test detection of UPDATE operations."""
        # Initialize state
        initial_state = [
            {"id": 1, "name": "Laptop", "price": 1200.00, "stock": 50}
        ]
        self.processor.last_sync_state = {"1": initial_state[0]}
        
        # Current state with modified record
        current_state = [
            {"id": 1, "name": "Laptop", "price": 1100.00, "stock": 45}
        ]
        
        # Detect changes
        events = self.processor.detect_changes(current_state, pk_column='id')
        
        # Assertions
        assert len(events) == 1
        assert events[0]["operation_type"] == "UPDATE"
        assert events[0]["primary_keys"]["id"] == 1
        assert events[0]["payload"]["old_data"]["price"] == 1200.00
        assert events[0]["payload"]["new_data"]["price"] == 1100.00
    
    def test_detect_delete(self):
        """Test detection of DELETE operations."""
        # Initialize state with two records
        initial_state = [
            {"id": 1, "name": "Laptop", "price": 1200.00, "stock": 50},
            {"id": 2, "name": "Mouse", "price": 25.00, "stock": 200}
        ]
        self.processor.last_sync_state = {"1": initial_state[0], "2": initial_state[1]}
        
        # Current state with one record (Laptop deleted)
        current_state = [
            {"id": 2, "name": "Mouse", "price": 25.00, "stock": 200}
        ]
        
        # Detect changes
        events = self.processor.detect_changes(current_state, pk_column='id')
        
        # Assertions
        assert len(events) == 1
        assert events[0]["operation_type"] == "DELETE"
        assert events[0]["primary_keys"]["id"] == 1
        assert events[0]["payload"]["old_data"]["name"] == "Laptop"
    
    def test_detect_mixed_operations(self):
        """Test detection of multiple operation types."""
        # Initialize state
        initial_state = [
            {"id": 1, "name": "Laptop", "price": 1200.00, "stock": 50},
            {"id": 2, "name": "Mouse", "price": 25.00, "stock": 200},
            {"id": 3, "name": "Keyboard", "price": 75.00, "stock": 100}
        ]
        self.processor.last_sync_state = {
            "1": initial_state[0],
            "2": initial_state[1],
            "3": initial_state[2]
        }
        
        # Current state with mixed changes
        # Laptop (id=1): UPDATED (price changed)
        # Mouse (id=2): DELETED
        # Keyboard (id=3): UNCHANGED
        # Monitor (id=4): INSERTED
        current_state = [
            {"id": 1, "name": "Laptop", "price": 1100.00, "stock": 50},
            {"id": 3, "name": "Keyboard", "price": 75.00, "stock": 100},
            {"id": 4, "name": "Monitor", "price": 350.00, "stock": 30}
        ]
        
        # Detect changes
        events = self.processor.detect_changes(current_state, pk_column='id')
        
        # Assertions - should detect INSERT, UPDATE, and DELETE
        assert len(events) == 3
        
        operation_types = {event["operation_type"] for event in events}
        assert "INSERT" in operation_types
        assert "UPDATE" in operation_types
        assert "DELETE" in operation_types
        
        # Verify specific operations
        insert_event = next(e for e in events if e["operation_type"] == "INSERT")
        assert insert_event["primary_keys"]["id"] == 4
        
        update_event = next(e for e in events if e["operation_type"] == "UPDATE")
        assert update_event["primary_keys"]["id"] == 1
        
        delete_event = next(e for e in events if e["operation_type"] == "DELETE")
        assert delete_event["primary_keys"]["id"] == 2
    
    def test_empty_current_state_deletes_all(self):
        """Test that empty current state detects all records as deleted."""
        #
