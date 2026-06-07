#!/bin/bash
pkill -f "python.*app.py" 2>/dev/null && echo "Flask arrêté" || echo "Flask déjà arrêté"
pkill -f "vite"           2>/dev/null && echo "Vite arrêté"  || echo "Vite déjà arrêté"
