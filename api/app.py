from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import pandas as pd
from supabase_client import supabase
from routes import query
from routes.web_routes import web_bp
import os
from datetime import timedelta

def create_api():
    api = Flask(__name__)
    CORS(api)
    
    # Register blueprints
    api.register_blueprint(query.query_bp, url_prefix="/api/query")
    api.register_blueprint(web_bp)

    @api.route("/api")
    def api_home():
        return jsonify({"message": "Api, go out if you'r not from development"})
    
    return api

# Vercel entry point
api = create_api()

# For local development
if __name__ == "__main__":
    api.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
