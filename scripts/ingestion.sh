#!/bin/bash
set -e

# Configuration
ZIP_FILE="weather_ingestion.zip"
INGESTION_LAYER_REQUIREMENTS_FILE="layers/ingestion/requirements.txt"
LAYER_BUILD_DIR="layers/build/ingestion_layer"
PYTHON_DIR="$LAYER_BUILD_DIR/python"
LAYER_ZIP="layer_ingestion.zip"

cd ..
echo "Working directory: $PWD"
WORK_DIR="$PWD"

echo "🚀 Building Lambda deployment package and layer..."

# Check if requirements.txt exists
if [ ! -f "$INGESTION_LAYER_REQUIREMENTS_FILE" ]; then
    echo "❌ Error: $INGESTION_LAYER_REQUIREMENTS_FILE not found"
    echo "💡 Create the requirements file at: layers/ingestion/requirements.txt"
    exit 1
fi

# Clean up old files
echo "🧹 Cleaning up old files..."
rm -f "$ZIP_FILE"
rm -f "$LAYER_ZIP"
rm -rf "$LAYER_BUILD_DIR"

# Create layer build directory structure
echo "📁 Creating layer build structure..."
mkdir -p "$PYTHON_DIR"

# Install dependencies into layer directory from requirements.txt
echo "📦 Installing dependencies from $INGESTION_LAYER_REQUIREMENTS_FILE..."
echo "📋 Dependencies to install:"
cat "$INGESTION_LAYER_REQUIREMENTS_FILE"
echo ""

pip install -r "$INGESTION_LAYER_REQUIREMENTS_FILE" -t "$PYTHON_DIR" --no-cache-dir

# Create layer ZIP with correct structure (without cd)
echo "🗜️ Creating layer ZIP..."
cd $LAYER_BUILD_DIR
zip -r "$WORK_DIR/$LAYER_ZIP" python
cd $WORK_DIR

echo "✅ Layer created: $LAYER_ZIP"

# Now build the main Lambda deployment package
echo "📦 Building main Lambda package..."

# Create deployment package with the correct structure
echo "Adding files to ZIP package..."
zip -r "$ZIP_FILE" \
    config.py \
    extract/__init__.py \
    extract/ingest_weather_data.py \
    utils/__init__.py \
    utils/weather_api.py \
    utils/s3_client.py

echo ""
echo "🎉 Build completed successfully!"
echo "📦 Lambda package: $ZIP_FILE"
echo "📦 Layer package: $LAYER_ZIP"
echo ""
echo "📋 Dependencies installed from: $INGESTION_LAYER_REQUIREMENTS_FILE"
echo ""
echo "🏗️  Layer structure:"
echo "   Source: $INGESTION_LAYER_REQUIREMENTS_FILE"
echo "   Build: $LAYER_BUILD_DIR"
echo "   Output: $LAYER_ZIP"
echo ""
echo "➡️  Next steps:"
echo "   1. Upload $ZIP_FILE to Lambda function code"
echo "   2. Upload $LAYER_ZIP to Lambda Layers"
echo "   3. Attach layer to Lambda function"