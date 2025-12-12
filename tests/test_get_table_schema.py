"""Unit tests for HarlequinWherobotsConnection.__get_table_schema method."""

import unittest
from unittest.mock import Mock, patch
from harlequin_wherobots.adapter import HarlequinWherobotsConnection


class TestGetTableSchema(unittest.TestCase):
    """Test the __get_table_schema method with various schema types."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a connection object with minimal setup
        self.conn = HarlequinWherobotsConnection.__new__(HarlequinWherobotsConnection)
        self.conn.host = "api.cloud.wherobots.com"
        self.conn.headers = {"X-API-Key": "test-key"}

    @patch('requests.get')
    def test_simple_schema(self, mock_get):
        """Test parsing a simple schema with primitive types."""
        # Mock response with simple schema
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": True},
                    {"id": 2, "name": "name", "type": "string", "required": False},
                    {"id": 3, "name": "age", "type": "int", "required": False},
                    {"id": 4, "name": "score", "type": "double", "required": False},
                    {"id": 5, "name": "active", "type": "boolean", "required": False},
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify the results
        self.assertEqual(len(children), 5)
        
        # Check first field
        self.assertEqual(children[0].label, "id")
        self.assertEqual(children[0].type_label, "long")
        self.assertEqual(children[0].qualified_identifier, "test_catalog.test_db.test_table.id")
        self.assertEqual(children[0].query_name, "id")
        
        # Check string field
        self.assertEqual(children[1].label, "name")
        self.assertEqual(children[1].type_label, "string")
        
        # Check other primitive types
        self.assertEqual(children[2].type_label, "int")
        self.assertEqual(children[3].type_label, "double")
        self.assertEqual(children[4].type_label, "boolean")

    @patch('requests.get')
    def test_complex_schema_struct(self, mock_get):
        """Test parsing a complex schema with struct type."""
        # Mock response with struct type
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": True},
                    {
                        "id": 2,
                        "name": "address",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {"id": 3, "name": "street", "type": "string", "required": False},
                                {"id": 4, "name": "city", "type": "string", "required": False},
                                {"id": 5, "name": "zipcode", "type": "string", "required": False}
                            ]
                        },
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify the results
        self.assertEqual(len(children), 2)
        
        # Check simple field
        self.assertEqual(children[0].label, "id")
        self.assertEqual(children[0].type_label, "string")
        
        # Check struct field - should extract just the type name
        self.assertEqual(children[1].label, "address")
        self.assertEqual(children[1].type_label, "struct")

    @patch('requests.get')
    def test_complex_schema_list(self, mock_get):
        """Test parsing a complex schema with list type."""
        # Mock response with list type
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": True},
                    {
                        "id": 2,
                        "name": "tags",
                        "type": {
                            "type": "list",
                            "element-id": 3,
                            "element": "string",
                            "element-required": False
                        },
                        "required": False
                    },
                    {
                        "id": 4,
                        "name": "scores",
                        "type": {
                            "type": "list",
                            "element-id": 5,
                            "element": "double",
                            "element-required": False
                        },
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify the results
        self.assertEqual(len(children), 3)
        
        # Check list fields - should extract just "list" as type name
        self.assertEqual(children[1].label, "tags")
        self.assertEqual(children[1].type_label, "list")
        self.assertEqual(children[2].label, "scores")
        self.assertEqual(children[2].type_label, "list")

    @patch('requests.get')
    def test_complex_schema_map(self, mock_get):
        """Test parsing a complex schema with map type."""
        # Mock response with map type
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": True},
                    {
                        "id": 2,
                        "name": "metadata",
                        "type": {
                            "type": "map",
                            "key-id": 3,
                            "key": "string",
                            "value-id": 4,
                            "value": "string",
                            "value-required": False
                        },
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify the results
        self.assertEqual(len(children), 2)
        
        # Check map field - should extract just "map" as type name
        self.assertEqual(children[1].label, "metadata")
        self.assertEqual(children[1].type_label, "map")

    @patch('requests.get')
    def test_mixed_schema(self, mock_get):
        """Test parsing a mixed schema with simple and complex types."""
        # Mock response with mixed types (based on real Overture Maps data)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "places_place",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": False},
                    {"id": 2, "name": "geometry", "type": "geometry", "required": False},
                    {
                        "id": 14,
                        "name": "bbox",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {"id": 15, "name": "xmin", "type": "float", "required": False},
                                {"id": 16, "name": "xmax", "type": "float", "required": False},
                                {"id": 17, "name": "ymin", "type": "float", "required": False},
                                {"id": 18, "name": "ymax", "type": "float", "required": False}
                            ]
                        },
                        "required": False
                    },
                    {
                        "id": 25,
                        "name": "categories",
                        "type": {
                            "type": "list",
                            "element-id": 26,
                            "element": "string",
                            "element-required": False
                        },
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="2rk2zjbg7pl6f8lb7xkzv",
            catalog="wherobots_open_data",
            db="overture_maps_foundation",
            table="places_place",
            into=children
        )

        # Verify the results
        self.assertEqual(len(children), 4)
        
        # Check simple types
        self.assertEqual(children[0].label, "id")
        self.assertEqual(children[0].type_label, "string")
        self.assertEqual(children[1].label, "geometry")
        self.assertEqual(children[1].type_label, "geometry")
        
        # Check complex types
        self.assertEqual(children[2].label, "bbox")
        self.assertEqual(children[2].type_label, "struct")
        self.assertEqual(children[3].label, "categories")
        self.assertEqual(children[3].type_label, "list")

    @patch('requests.get')
    def test_non_200_response(self, mock_get):
        """Test handling of non-200 HTTP responses."""
        # Mock 401 error response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify no children were added
        self.assertEqual(len(children), 0)

    @patch('requests.get')
    def test_empty_schema(self, mock_get):
        """Test handling of empty schema."""
        # Mock response with empty schema
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": []
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify no children were added
        self.assertEqual(len(children), 0)

    @patch('requests.get')
    def test_malformed_schema(self, mock_get):
        """Test handling of malformed schema response."""
        # Mock response with missing schema field
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table"
            # Missing "schema" field
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify no children were added
        self.assertEqual(len(children), 0)

    @patch('requests.get')
    @patch('logging.warning')
    def test_complex_type_missing_type_key(self, mock_logging, mock_get):
        """Test handling of complex type dict missing the 'type' key."""
        # Mock response with complex type missing "type" key
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": True},
                    {
                        "id": 2,
                        "name": "bad_field",
                        "type": {
                            # Missing "type" key!
                            "element-id": 3,
                            "element": "string"
                        },
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify both fields were added
        self.assertEqual(len(children), 2)
        
        # Check the bad field has warning label with full qualified name
        self.assertEqual(children[1].label, "bad_field")
        self.assertIn("unknown", children[1].type_label)
        self.assertIn("missing type", children[1].type_label)
        self.assertIn("test_catalog.test_db.test_table.bad_field", children[1].type_label)
        
        # Verify warning was logged
        self.assertTrue(mock_logging.called)
        self.assertIn("Missing 'type'", mock_logging.call_args[0][0])

    @patch('requests.get')
    @patch('logging.warning')
    def test_invalid_field_type_format(self, mock_logging, mock_get):
        """Test handling of invalid field type (not dict or string)."""
        # Mock response with field type as integer (invalid)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "name": "test_table",
            "schema": {
                "type": "struct",
                "fields": [
                    {"id": 1, "name": "id", "type": "string", "required": True},
                    {
                        "id": 2,
                        "name": "bad_field",
                        "type": 12345,  # Invalid: should be string or dict
                        "required": False
                    },
                    {
                        "id": 3,
                        "name": "another_bad",
                        "type": ["list", "of", "strings"],  # Invalid: should be string or dict
                        "required": False
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        # Call the method
        children = []
        self.conn._HarlequinWherobotsConnection__get_table_schema(
            catalog_id="test-catalog-id",
            catalog="test_catalog",
            db="test_db",
            table="test_table",
            into=children
        )

        # Verify all fields were added
        self.assertEqual(len(children), 3)
        
        # Check the good field
        self.assertEqual(children[0].label, "id")
        self.assertEqual(children[0].type_label, "string")
        
        # Check the bad fields have warning labels with full qualified names
        self.assertEqual(children[1].label, "bad_field")
        self.assertIn("unknown", children[1].type_label)
        self.assertIn("invalid type", children[1].type_label)
        self.assertIn("test_catalog.test_db.test_table.bad_field", children[1].type_label)
        
        self.assertEqual(children[2].label, "another_bad")
        self.assertIn("unknown", children[2].type_label)
        self.assertIn("invalid type", children[2].type_label)
        self.assertIn("test_catalog.test_db.test_table.another_bad", children[2].type_label)
        
        # Verify warnings were logged (should be 2 warnings)
        self.assertEqual(mock_logging.call_count, 2)
        self.assertIn("Unexpected field_type format", mock_logging.call_args_list[0][0][0])


if __name__ == '__main__':
    unittest.main()
