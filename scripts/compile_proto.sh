#!/bin/bash

# set paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
PROTO_DIR="$PROJECT_ROOT/src/common/grpc/protos"
OUTPUT_DIR="$PROJECT_ROOT/src/common/grpc/auto_generated"

# directory check
if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir -p "$OUTPUT_DIR"
fi

# clear legacy files
rm -f "$OUTPUT_DIR"/*.py 2>/dev/null
rm -f "$OUTPUT_DIR"/*.pyi 2>/dev/null

# compile .proto files
echo "Compiling .proto files..."
python -m grpc_tools.protoc \
    -I"$PROTO_DIR/messages" \
    -I"$PROTO_DIR/services" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    --pyi_out="$OUTPUT_DIR" \
    "$PROTO_DIR/messages"/*.proto \
    "$PROTO_DIR/services"/*.proto

# fix relative imports
echo "Patching generated files for relative imports..."

# Fix file_operation_service imports
for file in "$OUTPUT_DIR"/file_operation_service_*.py*; do
    if [ -f "$file" ]; then
        sed -i 's/import file_operation_message_pb2/from . import file_operation_message_pb2/g' "$file"
        sed -i 's/import file_operation_service_pb2/from . import file_operation_service_pb2/g' "$file"
    fi
done

# Fix coordinator_service imports if exists
for file in "$OUTPUT_DIR"/coordinator_service_*.py*; do
    if [ -f "$file" ]; then
        sed -i 's/import coordinator_message_pb2/from . import coordinator_message_pb2/g' "$file"
        sed -i 's/import coordinator_service_pb2/from . import coordinator_service_pb2/g' "$file"
    fi
done

# create __init__.py
touch "$OUTPUT_DIR/__init__.py"

echo "Proto files compiled successfully!"
echo "Output directory: $OUTPUT_DIR"