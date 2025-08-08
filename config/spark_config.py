import os
import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from config.config import Config

def find_java_home():
    """Find JAVA_HOME on Windows"""
    if os.name == 'nt':  # Windows
        print("Searching for Java installation...")
        
        # Check if JAVA_HOME is already set and valid
        java_home = os.environ.get('JAVA_HOME')
        if java_home:
            java_exe = os.path.join(java_home, 'bin', 'java.exe')
            if os.path.exists(java_exe):
                print(f"Found valid JAVA_HOME: {java_home}")
                return java_home
            else:
                print(f"JAVA_HOME set but java.exe not found at: {java_exe}")
        
        # Try to find Java using PowerShell Get-Command (most reliable on Windows)
        try:
            print("Trying PowerShell Get-Command java...")
            result = subprocess.run([
                'powershell', '-Command',
                'Get-Command java | Select-Object -ExpandProperty Source'
            ], capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                java_path = result.stdout.strip()
                print(f"Found java.exe at: {java_path}")
                # Get parent directory of bin folder
                java_home = os.path.dirname(os.path.dirname(java_path))
                if os.path.exists(java_home):
                    print(f"Derived JAVA_HOME: {java_home}")
                    return java_home
        except Exception as e:
            print(f"Error running PowerShell Get-Command: {e}")
        
        # Try common installation paths
        print("Searching common Java installation paths...")
        possible_paths = [
            r"C:\Program Files\Eclipse Adoptium\jdk-21.0.7+6-hotspot",
            r"C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot",
            r"C:\Program Files\Eclipse Adoptium\jdk-11.0.21.9-hotspot",
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.9.9-hotspot",
            r"C:\Program Files\Java\jdk-21.0.7",
            r"C:\Program Files\Java\jdk-11.0.21",
            r"C:\Program Files\Java\jdk-17.0.9",
            r"C:\Program Files\OpenJDK\openjdk-21.0.2",
            r"C:\Program Files\OpenJDK\openjdk-11.0.2",
            r"C:\Program Files\OpenJDK\openjdk-17.0.2",
            r"C:\Program Files (x86)\Eclipse Adoptium\jdk-21.0.7+6-hotspot",
            r"C:\Program Files (x86)\Eclipse Adoptium\jdk-21.0.7.6-hotspot",
            r"C:\Program Files (x86)\Eclipse Adoptium\jdk-11.0.21.9-hotspot",
            r"C:\Program Files (x86)\Java\jdk-21.0.7",
            r"C:\Program Files (x86)\Java\jdk-11.0.21",
        ]
        
        for path in possible_paths:
            java_exe = os.path.join(path, 'bin', 'java.exe')
            if os.path.exists(java_exe):
                print(f"Found Java at: {path}")
                return path
            else:
                print(f"Java not found at: {path}")
        
        # Try to find any Java installation in Program Files
        try:
            print("Searching Program Files directories...")
            program_files_dirs = [
                r"C:\Program Files",
                r"C:\Program Files (x86)"
            ]
            
            for pf_dir in program_files_dirs:
                if os.path.exists(pf_dir):
                    for item in os.listdir(pf_dir):
                        if 'java' in item.lower() or 'jdk' in item.lower() or 'adoptium' in item.lower():
                            potential_java_home = os.path.join(pf_dir, item)
                            java_exe = os.path.join(potential_java_home, 'bin', 'java.exe')
                            if os.path.exists(java_exe):
                                print(f"Found Java installation: {potential_java_home}")
                                return potential_java_home
        except Exception as e:
            print(f"Error searching Program Files: {e}")
    
    print("No Java installation found")
    return None

def create_spark_session(app_name="TransactionProcessor"):
    """Create and configure Spark session for LOCAL execution"""
    
    try:
        print(f"Creating Spark session: {app_name}")
        
        # Fix Windows Python executable issue
        if os.name == 'nt':  # Windows
            print("Configuring for Windows environment...")
            
            # Set Python executable paths for Windows
            python_exe = sys.executable
            os.environ['PYSPARK_PYTHON'] = python_exe
            os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
            print(f"Python executable: {python_exe}")
            
            # Fix Windows Hadoop issue - create proper dummy Hadoop structure
            import tempfile
            hadoop_home = os.path.join(tempfile.gettempdir(), 'hadoop_dummy')
            hadoop_bin = os.path.join(hadoop_home, 'bin')
            
            # Create dummy Hadoop directories if they don't exist
            os.makedirs(hadoop_bin, exist_ok=True)
            
            # Create dummy winutils.exe if it doesn't exist
            winutils_path = os.path.join(hadoop_bin, 'winutils.exe')
            if not os.path.exists(winutils_path):
                # Create a dummy executable file
                with open(winutils_path, 'wb') as f:
                    f.write(b'dummy')
            
            os.environ['HADOOP_HOME'] = hadoop_home
            os.environ['HADOOP_CONF_DIR'] = hadoop_home
            print(f"HADOOP_HOME set to: {hadoop_home}")
            
            # Always search for the best Java installation (override existing JAVA_HOME if needed)
            current_java_home = os.environ.get('JAVA_HOME')
            if current_java_home:
                java_exe = os.path.join(current_java_home, 'bin', 'java.exe')
                if os.path.exists(java_exe):
                    print(f"Using existing valid JAVA_HOME: {current_java_home}")
                else:
                    print(f"Existing JAVA_HOME invalid: {current_java_home}")
                    print("Searching for correct Java installation...")
                    java_home = find_java_home()
                    if java_home:
                        os.environ['JAVA_HOME'] = java_home
                        print(f"Overrode JAVA_HOME to: {java_home}")
                    else:
                        raise RuntimeError(
                            "Java not found. Please install Java 11 or later from https://adoptium.net/ "
                            "and set JAVA_HOME environment variable."
                        )
            else:
                print("JAVA_HOME not set, searching for Java installation...")
                java_home = find_java_home()
                if java_home:
                    os.environ['JAVA_HOME'] = java_home
                    print(f"Set JAVA_HOME to: {java_home}")
                else:
                    raise RuntimeError(
                        "Java not found. Please install Java 11 or later from https://adoptium.net/ "
                        "and set JAVA_HOME environment variable."
                    )
    
        print("Creating Spark configuration...")
        conf = SparkConf()
        conf.set("spark.master", "local[*]")  # Use all available cores locally
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Windows-specific configuration
        if os.name == 'nt':
            conf.set("spark.pyspark.python", sys.executable)
            conf.set("spark.pyspark.driver.python", sys.executable)
            # Disable Hadoop native library warnings and requirements
            conf.set("spark.hadoop.io.native.lib.available", "false")
            conf.set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            conf.set("spark.hadoop.mapreduce.application.classpath", "")
            conf.set("spark.hadoop.yarn.application.classpath", "")
            conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
            # Disable file system checks that require winutils
            conf.set("spark.hadoop.fs.file.impl.disable.cache", "true")
        
        # AWS S3 Configuration (still needed for S3 access)
        print("Configuring S3 access...")
        conf.set("spark.hadoop.fs.s3a.access.key", Config.AWS_ACCESS_KEY_ID)
        conf.set("spark.hadoop.fs.s3a.secret.key", Config.AWS_SECRET_ACCESS_KEY)
        conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{Config.AWS_REGION}.amazonaws.com")
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # PostgreSQL driver (download automatically)
        print("Configuring database drivers...")
        conf.set("spark.jars.packages",
                 "org.postgresql:postgresql:42.7.0,"
                 "org.apache.hadoop:hadoop-aws:3.3.4,"
                 "com.amazonaws:aws-java-sdk-bundle:1.12.565")
        
        print("Building Spark session...")
        spark = SparkSession.builder \
            .appName(app_name) \
            .config(conf=conf) \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"Spark session created successfully: {app_name}")
        return spark
        
    except Exception as e:
        print(f"Error creating Spark session: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        raise

def get_postgres_properties():
    """Get PostgreSQL connection properties for Spark"""
    return {
        "user": Config.POSTGRES_USER,
        "password": Config.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
        # Prevent table operations that require special permissions
        "truncate": "false",
        # Disable batch inserts that might cause issues
        "batchsize": "1000",
        # Set isolation level to avoid conflicts
        "isolationLevel": "READ_COMMITTED"
    }

def get_postgres_url():
    """Get PostgreSQL JDBC URL"""
    return f"jdbc:postgresql://{Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"