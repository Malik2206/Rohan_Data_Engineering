import pandas as pd
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self, source_path: str):
        self.source_path = source_path
        self.data = None
        
    def extract(self) -> pd.DataFrame:
        """Extract data from source"""
        try:
            self.data = pd.read_csv(self.source_path)
            logger.info(f"Extracted {len(self.data)} rows")
            return self.data
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def transform(self) -> pd.DataFrame:
        """Transform and clean data"""
        if self.data is None:
            raise ValueError("No data to transform")
        
        # Remove duplicates
        self.data = self.data.drop_duplicates()
        
        # Handle missing values
        self.data = self.data.fillna(method='ffill')
        
        # Convert datetime columns
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        
        # Add processing metadata
        self.data['processed_at'] = datetime.now()
        
        logger.info(f"Transformed data: {len(self.data)} rows")
        return self.data
    
    def load(self, output_path: str) -> None:
        """Load data to destination"""
        if self.data is None:
            raise ValueError("No data to load")
        
        self.data.to_parquet(output_path, index=False)
        logger.info(f"Loaded data to {output_path}")
    
    def run(self, output_path: str) -> None:
        """Execute full ETL pipeline"""
        self.extract()
        self.transform()
        self.load(output_path)
        logger.info("Pipeline completed successfully")

# Usage
if __name__ == "__main__":
    pipeline = DataPipeline("input_data.csv")
    pipeline.run("output_data.parquet")