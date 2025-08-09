import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from src.mechanism_x.data_ingestion import DataIngestionService
from src.mechanism_x.gdrive_reader import GDriveReader
import tempfile
import os

class TestMechanismX(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_transactions = pd.DataFrame({
            'step': [0, 1, 2],
            'customer': ['C1093826151', 'C1093826152', 'C1093826153'],
            'age': ['4', '5', '6'],
            'gender': ['M', 'F', 'M'],
            'zipcodeOri': ['28007', '28008', '28009'],
            'merchant': ['M348934600', 'M348934601', 'M348934600'],
            'zipMerchant': ['28007', '28008', '28009'],
            'category': ['es_transportation', 'es_food', 'es_transportation'],
            'amount': [4.55, 3.25, 5.75],
            'fraud': [0, 0, 0]
        })
        
        self.sample_importance = pd.DataFrame({
            'Source': ['C1093826151', 'C1093826152'],
            'Target': ['M348934600', 'M348934601'],
            'Weight': [4.55, 3.25],
            'typeTrans': ['es_transportation', 'es_food'],
            'fraud': [0, 0]
        })
    
    @patch('src.mechanism_x.gdrive_reader.Credentials')
    @patch('src.mechanism_x.gdrive_reader.build')
    def test_gdrive_reader_initialization(self, mock_build, mock_credentials):
        """Test GDriveReader initialization"""
        mock_service = Mock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = Mock()
        
        # Mock the file listing and download
        mock_service.files().list().execute.return_value = {
            'files': [
                {'id': 'file1', 'name': 'transactions.csv'},
                {'id': 'file2', 'name': 'CustomerImportance.csv'}
            ]
        }
        
        with patch.object(GDriveReader, '_download_csv_with_retry') as mock_download:
            mock_download.side_effect = [self.sample_transactions, self.sample_importance]
            
            reader = GDriveReader()
            
            self.assertIsNotNone(reader.transactions_df)
            self.assertIsNotNone(reader.customer_importance_df)
            self.assertEqual(len(reader.transactions_df), 3)
    
    @patch('src.mechanism_x.gdrive_reader.Credentials')
    @patch('src.mechanism_x.gdrive_reader.build')
    def test_get_next_chunk(self, mock_build, mock_credentials):
        """Test chunk retrieval functionality"""
        mock_service = Mock()
        mock_build.return_value = mock_service
        mock_credentials.from_service_account_file.return_value = Mock()
        
        mock_service.files().list().execute.return_value = {
            'files': [
                {'id': 'file1', 'name': 'transactions.csv'},
                {'id': 'file2', 'name': 'CustomerImportance.csv'}
            ]
        }
        
        with patch.object(GDriveReader, '_download_csv_with_retry') as mock_download:
            mock_download.side_effect = [self.sample_transactions, self.sample_importance]
            
            reader = GDriveReader()
            chunk = reader.get_next_chunk(chunk_size=2)
            
            self.assertEqual(len(chunk), 2)
            self.assertEqual(reader.current_position, 2)
            
            # Test getting remaining data
            chunk2 = reader.get_next_chunk(chunk_size=2)
            self.assertEqual(len(chunk2), 1)  # Only 1 record left
            self.assertEqual(reader.current_position, 3)
            
            # Test completion - should return empty DataFrame
            chunk3 = reader.get_next_chunk(chunk_size=2)
            self.assertEqual(len(chunk3), 0)  # Empty DataFrame when complete
            self.assertTrue(reader.is_complete())
    
    @patch('src.mechanism_x.data_ingestion.GDriveReader')
    @patch('src.mechanism_x.data_ingestion.S3Utils')
    def test_data_ingestion_service(self, mock_s3_utils, mock_gdrive_reader):
        """Test DataIngestionService functionality"""
        # Setup mocks
        mock_reader_instance = Mock()
        mock_gdrive_reader.return_value = mock_reader_instance
        
        # Mock sequence: first call returns data, second call returns empty (completion)
        mock_reader_instance.get_next_chunk.side_effect = [
            self.sample_transactions.head(2),  # First chunk
            pd.DataFrame()  # Empty DataFrame to signal completion
        ]
        mock_reader_instance.is_complete.return_value = True
        
        mock_s3_instance = Mock()
        mock_s3_utils.return_value = mock_s3_instance
        
        # Patch the initialization to avoid actual GDrive connection
        with patch.object(DataIngestionService, '_initialize_gdrive_reader'):
            service = DataIngestionService()
            service.gdrive_reader = mock_reader_instance  # Set the mock directly
            service.running = True  # Set running state to True so the loop executes
            
            # Test single ingestion cycle
            service._run_ingestion()
            
            # Verify that chunk was retrieved and uploaded
            mock_reader_instance.get_next_chunk.assert_called()
            mock_s3_instance.upload_dataframe.assert_called()

class TestGDriveIntegration(unittest.TestCase):
    """Integration tests for Google Drive functionality"""
    
    def test_csv_parsing(self):
        """Test CSV parsing functionality"""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('step,customer,age,gender,zipcodeOri,merchant,zipMerchant,category,amount,fraud\n')
            f.write('0,C1093826151,4,M,28007,M348934600,28007,es_transportation,4.55,0\n')
            f.write('1,C1093826152,5,F,28008,M348934601,28008,es_food,3.25,0\n')
            temp_path = f.name
        
        try:
            df = pd.read_csv(temp_path)
            self.assertEqual(len(df), 2)
            self.assertEqual(df.iloc[0]['customer'], 'C1093826151')
        finally:
            os.unlink(temp_path)

if __name__ == '__main__':
    unittest.main()