#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD="$DIR/.lambda_build"
OUT="$DIR/lambda.zip"

echo "Nettoyage..."
rm -rf "$BUILD" "$OUT"
mkdir -p "$BUILD"

echo "Installation des dépendances..."
pip install -r "$DIR/requirements.txt" -t "$BUILD" --quiet

echo "Copie du code..."
cp "$DIR/lambda_app.py" "$BUILD/app.py"

echo "Création du zip..."
cd "$BUILD" && zip -r "$OUT" . -x "*.pyc" -x "*/__pycache__/*" > /dev/null

SIZE=$(du -sh "$OUT" | cut -f1)
echo "✓ lambda.zip prêt ($SIZE) → $OUT"
