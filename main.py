import signal
import sys
import time
import os
import platform
from datetime import datetime
from config.config import Config
from src.mechanism_x import DataIngestionService
from src.mechanism_y import StreamProcessor
from src.utils.logger import setup_logger
from src.utils.postgres_utils import PostgresUtils
import structlog

logger = setup_logger()

class TransactionProcessorApp:
    def __init__(self):
        self.ingestion_service = None
        self.stream_processor = None
        self.postgres_utils = PostgresUtils()
        self.running = False
        self.shutdown_in_progress = False
    
    def start(self):
        try:
            logger.info("Starting Transaction Processor Application")
            
            Config.validate()
            logger.info("Configuration validated")
            
            self.postgres_utils.create_tables()
            logger.info("Database tables initialized")
            
            self.ingestion_service = DataIngestionService()
            self.stream_processor = StreamProcessor()
            
            self.ingestion_service.start()
            self.stream_processor.start()
            
            self.running = True
            logger.info("Transaction Processor Application started successfully")
            
            self._wait_for_completion()
            
        except Exception as e:
            logger.error("Failed to start application", error=str(e))
            self._graceful_stop()
            sys.exit(1)
    
    def stop(self):
        self._graceful_stop()
    
    def _graceful_stop(self):
        if self.shutdown_in_progress:
            return
            
        self.shutdown_in_progress = True
        self.running = False
        
        logger.info("Shutting down gracefully")
        
        if self.ingestion_service:
            try:
                self.ingestion_service.stop()
                logger.info("Data ingestion service stopped")
            except Exception as e:
                logger.warning("Error stopping ingestion service", error=str(e))
        
        if self.stream_processor:
            try:
                self.stream_processor.running = False
                time.sleep(3)
                self.stream_processor.stop()
                logger.info("Stream processor stopped")
            except Exception as e:
                logger.warning("Error stopping stream processor", error=str(e))
        
        time.sleep(1)
        
        if platform.system() == "Windows":
            self._cleanup_windows_processes()
        
        logger.info("Transaction Processor Application stopped")
    
    def _cleanup_windows_processes(self):
        try:
            import subprocess
            import psutil
            
            try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        if proc.info['name'] and 'java.exe' in proc.info['name'].lower():
                            cmdline = proc.info.get('cmdline', [])
                            if cmdline and any('spark' in str(arg).lower() for arg in cmdline):
                                proc.terminate()
                                proc.wait(timeout=3)
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                        continue
            except ImportError:
                pass
            
            try:
                result = subprocess.run(
                    ["wmic", "process", "where", "name='java.exe'", "get", "processid", "/value"],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if 'ProcessId=' in line:
                            pid = line.split('=')[1].strip()
                            if pid.isdigit():
                                try:
                                    subprocess.run(
                                        ["wmic", "process", "where", f"processid={pid}", "delete"],
                                        capture_output=True, timeout=3
                                    )
                                except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                                    pass
            except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
                pass
                
        except Exception as e:
            logger.debug("Process cleanup completed", error=str(e))
    
    def _wait_for_completion(self):
        def signal_handler(signum, frame):
            logger.info("Shutdown signal received")
            self._graceful_stop()
            time.sleep(2)
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            ingestion_completed = False
            while self.running and not self.shutdown_in_progress:
                if (self.ingestion_service and
                    hasattr(self.ingestion_service, 'running') and
                    not self.ingestion_service.running and
                    not ingestion_completed):
                    
                    ingestion_completed = True
                    logger.info("Data ingestion completed")
                    
                    # Allow stream processor to continue processing existing files
                    # Don't shut it down immediately
                    logger.info("Allowing stream processor to continue processing files...")
                    
                    # Wait longer for stream processor to finish processing
                    time.sleep(30)
                    
                    # Check if stream processor has processed all files
                    if self.stream_processor and hasattr(self.stream_processor, 'processed_files'):
                        logger.info("Stream processor status",
                                  processed_files=len(self.stream_processor.processed_files))
                    
                    self._graceful_stop()
                    break
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self._graceful_stop()
        except Exception as e:
            logger.error("Error in main loop", error=str(e))
            self._graceful_stop()

def main():
    print("="*60)
    print("PySpark Transaction Processor")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    app = TransactionProcessorApp()
    app.start()

if __name__ == "__main__":
    main()