from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime
import requests
import json
import traceback
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

query_bp = Blueprint("query", __name__)

# Initialize Supabase client
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def parse_date(date_str):
    if not date_str or pd.isna(date_str) or date_str.lower() == 'nan':
        return None
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return dt.isoformat()
    except:
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.isoformat()
        except:
            return None

def parse_numeric(value):
    if pd.isna(value) or value == '':
        return None
    try:
        return float(value)
    except:
        return None

def parse_int(value):
    if pd.isna(value) or value == '':
        return None
    try:
        return int(value)
    except:
        return None

def get_catastral_data(reference: str):
    try:
        print(f"Making request to Catastro API for reference: {reference}")
        url = f"https://ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/json/Consulta_DNPRC?RefCat={reference}"
        response = requests.get(url, headers={'Accept': 'application/json'})
        response.raise_for_status()
        data = response.json()
        print(f"Successfully received data from Catastro API: {data}")
        
        if 'consulta_dnprcResult' in data and 'bico' in data['consulta_dnprcResult']:
            bi = data['consulta_dnprcResult']['bico']['bi']
            return {
                'clase': bi.get('debi', {}).get('luso', 'N/A'),
                'year': parse_int(bi.get('debi', {}).get('ant')) or 'N/A',
                'area': parse_numeric(bi.get('debi', {}).get('sfc')) or 'N/A',
                'planta': bi.get('dt', {}).get('locs', {}).get('lous', {}).get('lourb', {}).get('loint', {}).get('pt', 'N/A') + ', ' + bi.get('dt', {}).get('locs', {}).get('lous', {}).get('lourb', {}).get('loint', {}).get('pu', 'N/A'),
                'location': bi.get('dt', {}).get('locs', {}).get('lous', {}).get('lourb', {}).get('dir', {}).get('nv', 'N/A'),
                'provincia': bi.get('dt', {}).get('np', 'N/A'),
                'ciudad': bi.get('dt', {}).get('nm', 'N/A'),
                'barrio': bi.get('dt', {}).get('locs', {}).get('lous', {}).get('lourb', {}).get('dp', 'N/A')
            }
    except Exception as e:
        print(f"Error fetching catastral data: {str(e)}")
        print(f"Response content: {response.text if 'response' in locals() else 'No response'}")
    return {
        'clase': 'N/A',
        'year': 'N/A',
        'area': 'N/A',
        'planta': 'N/A',
        'location': 'N/A',
        'provincia': 'N/A',
        'ciudad': 'N/A',
        'barrio': 'N/A'
    }

@query_bp.route("/data", methods=["GET"])
def get_data():
    try:
        print("Fetching data from subastas table")
        response = supabase.table('subastas').select("*").execute()
        print(f"Successfully fetched {len(response.data)} records")
        return jsonify(response.data)
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

@query_bp.route("/insert", methods=["POST"])
def query():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        print(f"Processing file: {file.filename}")
        
        # Read Excel file
        df = pd.read_excel(file)
        print(f"Successfully read Excel file with {len(df)} rows")
        
        # Convert all columns to string to avoid type issues
        df = df.astype(str)
        
        # Delete existing table if it exists
        try:
            print("Attempting to delete existing data from subastas table")
            supabase.table('subastas').delete().neq('id', 0).execute()
            print("Successfully deleted existing data")
        except Exception as e:
            print(f"Error deleting existing table: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
        
        # Process each row
        for index, row in df.iterrows():
            try:
                print(f"\nProcessing row {index + 1}/{len(df)}")
                
                # Extract catastral reference from the row
                catastral_ref = str(row.get('bienes_referencia_catastral', ''))
                if not catastral_ref or pd.isna(catastral_ref):
                    print(f"Skipping row {index + 1}: No catastral reference")
                    continue
                
                print(f"Fetching catastral data for reference: {catastral_ref}")
                catastral_data = get_catastral_data(catastral_ref)
                print(f"Successfully fetched catastral data: {catastral_data}")
                
                # Prepare data for insertion
                data = {
                    'referencia': str(row.get('informacion_general_identificador', '')),
                    'fecha_publicacion': parse_date(row.get('informacion_general_fecha_de_inicio')),
                    'fecha_conclusion': parse_date(row.get('informacion_general_fecha_de_conclusion')),
                    'tipo_subasta': str(row.get('informacion_general_tipo_de_subasta', '')),
                    'estado_subasta': str(row.get('informacion_general_estado', '')),
                    'tipo_entidad': str(row.get('autoridad_gestora_descripcion', '')),
                    'entidad': str(row.get('autoridad_gestora_codigo', '')),
                    'tipo_bien': str(row.get('bienes_tipo', '')),
                    'subtipo_bien': str(row.get('bienes_descripcion', '')),
                    'provincia': str(row.get('bienes_provincia', '')),
                    'poblacion': str(row.get('bienes_localidad', '')),
                    'valor_subasta': parse_numeric(row.get('informacion_general_valor_subasta')),
                    'valor_tasacion': parse_numeric(row.get('informacion_general_tasacion')),
                    'puja_minima': parse_numeric(row.get('informacion_general_puja_minima')),
                    'puja_actual': parse_numeric(row.get('pujas_puja_maxima')),
                    'numero_lotes': parse_int(row.get('informacion_general_lotes')),
                    'catastral_reference': str(row.get('bienes_referencia_catastral', '')),
                    'catastral_class': catastral_data['clase'],
                    'catastral_year': catastral_data['year'],
                    'catastral_area': catastral_data['area'],
                    'catastral_floor': catastral_data['planta'],
                    'catastral_location': catastral_data['location'],
                    'catastral_province': catastral_data['provincia'],
                    'catastral_city': catastral_data['ciudad'],
                    'catastral_district': catastral_data['barrio']
                }
                
                print(f"Inserting data for row {index + 1}")
                response = supabase.table('subastas').insert(data).execute()
                print(f"Successfully inserted row {index + 1}")
                
            except Exception as e:
                print(f"Error processing row {index + 1}: {str(e)}")
                print(f"Traceback: {traceback.format_exc()}")
                continue
        
        return jsonify({"message": "File processed successfully"})
        
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500