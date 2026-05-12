"""
Setup validation script - Checks dependencies and configuration.
Run this after initial setup to verify everything is working.
"""

import subprocess
import sys
from pathlib import Path


def check_python_version():
    """Verify Python version >= 3.11"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 11):
        print(f"❌ Python 3.11+ required (current: {version.major}.{version.minor})")
        return False
    print(f"✓ Python {version.major}.{version.minor} OK")
    return True


def check_docker():
    """Verify Docker is installed"""
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
        print("✓ Docker installed")
        return True
    except:
        print("❌ Docker not found. Install from https://www.docker.com")
        return False


def check_docker_compose():
    """Verify Docker Compose is installed"""
    try:
        subprocess.run(["docker-compose", "--version"], capture_output=True, check=True)
        print("✓ Docker Compose installed")
        return True
    except:
        print("❌ Docker Compose not found. Install from https://docs.docker.com/compose/install/")
        return False


def check_required_files():
    """Verify required project files exist"""
    required_files = [
        "docker-compose.yml",
        "requirements.txt",
        ".env.example",
        "README.md",
        "producer/kafka_producer.py",
        "processor/spark_stream_processor.py",
        "serving/streamlit_app.py",
        "config/settings.py",
        "config/schemas.py",
        "data_quality/validators.py",
        "terraform/main.tf",
    ]
    
    all_exist = True
    for file in required_files:
        path = Path(file)
        if path.exists():
            print(f"✓ {file}")
        else:
            print(f"❌ {file} NOT FOUND")
            all_exist = False
    
    return all_exist


def check_env_file():
    """Check if .env exists, suggest creating it"""
    if Path(".env").exists():
        print("✓ .env file exists")
        return True
    elif Path(".env.example").exists():
        print("⚠️  .env file not found. Creating from .env.example...")
        try:
            import shutil
            shutil.copy(".env.example", ".env")
            print("✓ .env created from template")
            return True
        except Exception as e:
            print(f"❌ Could not create .env: {e}")
            return False
    return False


def main():
    """Run all checks"""
    print("\n" + "="*60)
    print("🔍 Real-Time Ride Analytics - Setup Validation")
    print("="*60 + "\n")
    
    checks = [
        ("Python Version", check_python_version),
        ("Docker", check_docker),
        ("Docker Compose", check_docker_compose),
        ("Project Files", check_required_files),
        ("Environment Config", check_env_file),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n📋 Checking {name}...")
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"❌ Error during check: {e}")
            results.append((name, False))
    
    print("\n" + "="*60)
    print("📊 Summary")
    print("="*60)
    
    for name, passed in results:
        status = "✓" if passed else "❌"
        print(f"{status} {name}")
    
    all_passed = all(result for _, result in results)
    
    if all_passed:
        print("\n🎉 Setup validation passed! You're ready to go.")
        print("\n📖 Next steps:")
        print("   1. Review .env configuration")
        print("   2. Start services: docker-compose up -d")
        print("   3. View dashboard: http://localhost:8501")
        return 0
    else:
        print("\n⚠️  Setup validation failed. Please fix errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
