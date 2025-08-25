#!/bin/bash
set -e

# Configuration
GLUE_ZIP="weather_transform.zip"

cd ..
echo "Working directory: $PWD"
WORK_DIR="$PWD"

echo "🚀 Building Glue ETL job package..."

# Clean up old files
rm -f "$GLUE_ZIP"

# Create single ZIP with everything needed
echo "📦 Creating comprehensive Glue job package..."
zip -r "$GLUE_ZIP" \
    utils/ \
    config.py

echo "✅ Package created: $GLUE_ZIP"
echo ""
echo "📋 Next steps:"
echo "   1. Upload $GLUE_ZIP to S3: aws s3 cp $GLUE_ZIP s3://weather-scripts-bhavesh-de/glue/"
echo "   2. Create Glue job pointing to: s3://weather-scripts-bhavesh-de/glue/$GLUE_ZIP"
echo "   3. Set main script as: transform/process_weather_data.py"